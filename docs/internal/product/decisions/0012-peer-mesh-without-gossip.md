# 0012. A peer mesh, but not gossip: deterministic ring over platform membership

Status: accepted
Date: 2026-08-30
Requirements: FR-6, FR-12, NFR-4, NFR-5
Research: docs/research/30-design-space/05-az-topology-and-data-flow.md §5b

## Context

Ingester nodes already talk to each other — commit forwarding, push fan-out, and
the per-AZ segment cache. The proposal was to add a **gossip mesh** so pods share
what they hold and redirect to a peer that has it, avoiding object-store GETs.

The instinct is right and is already most of the design. The question is what
gossip would add on top of the deterministic consistent-hash ring.

## Decision

**Build the peer mesh. Do not build gossip.**

| Concern | Mechanism | Not |
|---|---|---|
| Membership | **Kubernetes `EndpointSlice` watch** — authoritative, sub-second, no new protocol | SWIM/gossip |
| Who holds a segment | **consistent-hash ring** over that membership — *computed*, not discovered | a cache directory |
| Who holds a segment it *wrote* | the key embeds `<podShortId>`; the writer serves its own segments regardless of ring position | any lookup |
| Who is the sequencer | the **lease object** is the truth; a peer hint may accelerate, never decide | gossip-elected leadership |
| Which pod has subscribers for a stream | **explicit registration** with the sequencer on subscribe | broadcast to all peers |
| Record order | the commit log | anything peer-derived |

**The governing rule: every peer-derived fact must be verifiable or harmless.**
Acting on a stale one may cost a redirect or one extra GET. If it could cause
divergence or loss, it comes from the store.

### Peer fetch is intra-AZ only

| Serving an 8 MiB segment | Cost |
|---|---|
| Intra-AZ peer fetch | **free** (+~0.5 ms) |
| Object-store GET | $0.0000004 |
| **Cross-AZ peer fetch** | **$0.000168 — 419× the GET** |

⚠️ **Never fetch a segment from a peer in another AZ.** At $0.02/GB, moving 8 MiB
across an AZ boundary costs 419× more than simply asking the object store, which
in-region is free of transfer charges. This inverts the usual "ask a peer before
you ask storage" intuition and is the single most important rule in this ADR.

### The miss ladder
```
local cache  ->  ring owner in the same AZ (proxy, streamed through)  ->  object store
```
Each step once, bounded timeout. If the ring owner is unreachable because
membership is stale, the pod fetches from the object store itself: correct, one
extra GET, no coordination.

## Alternatives considered

- **Gossip (SWIM) for membership.** Its genuine advantage is faster failure
  detection (~1–2 s) than a readiness probe. Rejected because a Kubernetes
  `EndpointSlice` **watch** is also sub-second, *and* authoritative, *and* free,
  *and* already a dependency (FR-12). **In Kubernetes, gossip solves a problem the
  platform already solves.** Kept as the documented fallback for a non-Kubernetes
  deployment, where it would earn its keep.
- **A gossiped cache directory** ("who actually holds what"). Its value is the gap
  between what the ring says a pod *should* hold and what it *does* hold — churn,
  eviction, in-flight prefetch. Quantified: a full 6-pod rolling restart causes
  ~360 extra GETs, **$0.00014**. Rejected: a convergence protocol, an inconsistent
  view, and a new failure mode, to save a fraction of a cent. The ring already
  answers "who has it" with no messages at all.
- **Broadcast commit fan-out to every peer**, avoiding a subscriber directory.
  Rejected on cost: 12 commits/s × ~150 KB × 5 peers = 9.2 MB/s, two-thirds
  cross-AZ ⇒ **~$319/month**, versus ~1.8 MB/s directed. Registration is both
  cheaper and exact.
- **Gossip-elected leadership.** Rejected outright: it is consensus by another
  name, and ADR-0011 settles where leadership comes from.

## Metadata specifically

The original framing of this ADR answered the *data* case. Metadata deserves its
own numbers, because the intuition "metadata is small, so gossip it" is only half
right.

### The crossover: ~19.5 KiB

```
break-even payload = (cost of one GET) / (cross-AZ $/GB)
                   = $4x10^-7 / $0.02 per GB
                   = 19.5 KiB
```

**Below ~19.5 KiB, shipping bytes across an AZ is cheaper than one extra
object-store GET. Above it, fetch from the store.** This is one rule for data and
metadata alike — the 419× figure for an 8 MiB segment is the same physics at the
far end of the same curve.

| Payload | Cross-AZ vs one GET | Verdict |
|---|---|---|
| Lease object (~1 KiB) | 0.1× | **ship** |
| Trickle batch (~8 KiB) | 0.4× | **ship** (this is why small inline events are free) |
| Ordinal registry (~50 KiB) | 2.6× | fetch |
| **Commit delta (~150 KiB)** | **7.7×** | **fetch** |
| Checkpoint (~2.9 MiB) | 148× | fetch |
| Segment (8 MiB) | 419× | fetch |

⚠️ **A commit delta is metadata and is still cheaper to fetch from the store than
to pull from a cross-AZ peer.** "It's only metadata" does not license cross-AZ
chatter; 150 KiB is already seven times past the crossover. Intra-AZ, of course,
it is free and a peer is always preferable.

### What metadata gossip would actually save

The relevant comparison is not *gossip vs nothing* — it is *gossip vs the directed
push already designed*.

| | Cost |
|---|---|
| Every pod tails the commit chain from the store itself | $75/month (6 pods), $224/month (18 pods) |
| Directed push to pods with subscribers (current design) | ~$0 in GETs, ~1.8 MB/s |
| **Gossip, as a fallback on top of directed push (1% push-miss)** | **saves $0.75/month** |

Directed push already captures the whole $75–224/month. Gossip's incremental
saving is **under a dollar a month**, while adding convergence latency
(2–3 rounds) to the one path we optimised hardest — discovery
([discovery-and-tailing §1](../../../research/30-design-space/04-discovery-and-tailing.md)).

Gossip's structural advantage — spreading fan-out load off the sequencer — does
not bind at our scale: 9.2 MB/s out of one pod at 6 pods, 31 MB/s at 18. It would
matter around 100 pods (182 MB/s), a count driven by ingest volume we do not
project.

### What we adopt instead: piggybacked digests

Not a protocol — extra fields on RPCs the pods already exchange (commit
forwarding, peer fetch, subscriber registration):

```
digest = { chainPosition: N, registryVersion: V, ringGeneration: G, leaseEpoch: E }
```

A pod that sees a peer ahead of it **pulls the gap from that peer instead of the
store — if the peer is in its own AZ**, per the crossover above. No timers, no
convergence semantics, no new failure mode, and it captures the practical benefit
of gossip on traffic that is already flowing.

Safety is unchanged: a delta is write-once, immutable and CRC-covered, so a
peer-supplied one is either the real record or detectably corrupt, and the chain
in the store remains the arbiter. Every digest is a **hint**.

⚠️ **Digests carry positive facts only.** A peer can tell you a delta *exists* —
verifiably. **No peer, and no quorum of peers, can tell you one does not exist**,
because none of them is in the write path; a majority vote over non-authoritative
replicas does not make a fact authoritative. A peer reporting the same chain
position as yours means *nothing*, and must never be read as "nothing new".

The consequence is smaller than it sounds: **nobody needs to establish absence.**
Consumers are woken by push when data arrives (R3), and the one safety-critical
place absence matters — sequencer failover — resolves it by **claiming** the next
sequence number rather than reading to check it is empty. See
[discovery-and-tailing §3b](../../../research/30-design-space/04-discovery-and-tailing.md).

## Consequences

- No new distributed protocol, no convergence semantics, no split-brain surface.
  The mesh is direct RPC over a membership list the platform maintains.
- Cache locality degrades gracefully during churn — a stale ring view costs GETs,
  never correctness.
- ⚠️ A non-Kubernetes deployment needs a membership source. Until one exists, the
  supported deployment target is Kubernetes; do not let "we could add gossip
  later" become an unstated assumption that it works today.
- The 419× rule must be enforced in code, not just documented: the peer-fetch path
  takes an AZ-scoped member list, so a cross-AZ peer is not addressable by
  construction rather than by discipline.
- **The ~19.5 KiB crossover becomes a named constant**, used by the peer-fetch
  path and by the inline-vs-coordinates decision in the subscription protocol.
  It is derived from two prices, so it moves with the price table — it belongs in
  the `CostTable`, not hard-coded.
- Piggybacked digests are cheap enough to build now and are the migration path if
  pod counts ever reach the scale where real gossip would pay.
