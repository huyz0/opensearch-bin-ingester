# M5 — Subscription and client

The read side, and the one thing M4 deliberately left broken.

⚠️ **THIS MILESTONE MAKES A MULTI-POD DEPLOYMENT CORRECT FOR THE FIRST TIME.**
M4 introduced a lease so exactly one sequencer writes the chain, and shipped
only the LOCAL implementation — so today every pod that is not the leaseholder
has no way to get its commits to the one that is.
[M4/SPEC.md](../M4/SPEC.md) § *Deployment constraint* states it twice on
purpose and assigns the fix here. Everything else in this milestone is the read
path; this one is a correctness hole, and it is task M5.6 for that reason -- M5.1 to M5.5 are what make it SAFE to close.

## Completion condition

The push channel carries a session and an epoch, **all three fetch modes** work
(`inline`, `proxy`, `direct`), commits are prefetched, **zero object-store
requests are issued by idle consumers at fan-out**, and a non-leaseholder pod
can commit.

⚠️ **"Zero idle requests" is NOT re-proven here — it is proven here for the
first time.** [roadmap.md](../../roadmap.md) and M1.16 both record that the M1
baseline was never established: the property held *by construction*, because no
class on the consumer path held a `BinStore` at all, so no consumer test could
make it fail. ADR-0023 says so in as many words, and adds that M5 "is instead
where the request-count *test* first becomes meaningful".

⚠️ **BUT NOT BY GIVING THE CONSUMER A `BinStore`.** An earlier draft of this
spec proposed exactly that, and it is the regression ADR-0023 exists to
prevent: the decision is enforced as a dependency check — "no class under
`plugin/src/main` or `client/src/main` imports `binjava.binstore`" — and
putting the SPI on the client's classpath to test that it is unused would
break the gate (M1.16e) in order to test the property the gate holds. It would
also contradict ADR-0004's consequence that the plugin needs no cloud SDK, no
object-store credentials and no block cache.

**`direct` does not require it either**: a signed URL is plain HTTP, not a
`BinStore`. So what M5 makes assertable is the ingester-side count — every
object-store request the serving path makes on behalf of idle consumers,
through `CountingBinStore` where the store actually lives — plus the count of
grants issued, which is the only request an idle consumer could cause. Both
can fail; neither needs the client to hold a store.

## Requirements

| ID | Requirement | Served by |
|---|---|---|
| FR-5 | Push tail notifications over a persistent same-AZ channel, with **session-based incremental subscription** | scope 7, criteria 11-12 |
| FR-6 | Serve record bytes in three modes — `inline`, `proxy`, `direct` (signed URL) — **chosen by the ingester** | scope 5-6, criteria 4-7 |
| FR-10 | Consumers keep working without the ingester, via a documented fallback ladder down to LIST recovery | scope 11, criterion 14 |
| FR-11 | Sequencer leadership by CAS lease with epoch fencing — M5 supplies the *remote* implementation of the seam M4 defined | scope 1-4, criteria 1-3 |
| NFR-2 | Idle cost: **zero** object-store requests from consumers | scope 8, criterion 8 |
| NFR-3 | LIST on hot paths: zero, hard ceiling ~1/s sustained | scope 11, criterion 14 |
| NFR-4 | Read request rate scales with segments, AZs and nodes — never with shards, partitions or indices | scope 8-9, criterion 9 |
| NFR-5 | Cross-AZ bytes < 0.1% of ingested bytes | scope 9, criterion 10 |

⚠️ **THREE REQUIREMENTS ARE EXPLICITLY NOT CLAIMED HERE, and one of them M4
expected M5 to meet.**

**NFR-9 (RTO < 5 s) moves to M8, with the EndpointSlice watch.** M4's SPEC says
"NFR-9 is met at M5" and routes it through an early-challenge path defined as
"an EndpointSlice watch triggering a lease challenge before TTL expiry", which
"needs the same membership signal as the peer mesh". M5 ships that membership as
static configuration — and **a static list never removes a member**, so the
event the challenge fires on has no producer. Building the challenge here would
give criterion 13 a fake to inject into and nothing in production to trigger it:
NFR-9 would read as met while real failover stayed TTL-bound at ADR-0007's ~10 s.
So the watch and the challenge travel together to M8, and **M4's claim is
corrected rather than quietly inherited**.

**NFR-7 (p99 < 3× the flush window) and NFR-13 (outage tolerance = retention)** An earlier draft listed both with no scope item, no
criterion and no task behind them — which is how a requirement gets lost between
two specs that each believe the other has it. NFR-7 needs a latency harness and
belongs with **M9**'s measurement; NFR-13 is a property of retention and belongs
with **M7**. ⚠️ **All three reassignments are written into roadmap.md's M7, M8
and M9 rows in this same commit** — a reassignment recorded only inside the
spec of the milestone giving it up is invisible to the author picking it up,
which is the exact failure this paragraph exists to prevent.

## Scope

**In:**

1. **The remote `Sequencer`** — commit forwarding. A non-leaseholder pod sends
   its `CommitRequest` to the pod named by the lease and gets the `CommitDelta`
   back. ⚠️ The seam already has the right shape: `CommitRequest` is
   `(podId, incarnationId, flushSeq, segmentKey, recordCounts)` — all values,
   meaningful when they arrive from another pod — and `Lease` already carries
   `holderEndpoint`. M4 built both for this.
2. **The idempotency window must survive a takeover** — `IdempotencyWindow`
   is process-local, and `Sequencer`'s own javadoc says so: "a replay that
   crosses a takeover commits twice. Inheriting it is M4.10f." ⚠️ **M4.10f is
   not in the build and is a backlog row nowhere**, on this branch or the
   archive. Forwarding is exactly what makes that reachable — a forwarded
   commit whose reply is lost, retried after the lease moves, lands twice — so
   M5 owns it and it lands BEFORE forwarding is wired.
3. **A retry must reuse its triple.** `DefaultIngest.java:355` writes
   `flushSeq++` at the commit call site, so a retry mints a DIFFERENT triple
   and the leaseholder's dedup cannot match it. `Sequencer`'s contract states
   the consequence and adds "THIS GUARANTEE STOPS AT THIS SEAM and no
   production caller yet reaches it". M5.6 makes one reach it.
4. **A `SequencerTransport` seam** and its fake, because the remote
   implementation must be testable at T1 without a socket. ⚠️ **The lease is the
   truth about who holds it** (ADR-0012): a peer hint may accelerate, never
   decide. A forwarding pod re-reads the lease when its target refuses.
5. **`FetchMode` on the subscription**, and the two modes that do not exist:
   - `proxy` — the ingester streams the segment through to the consumer.
     ⚠️ **Stream through, never buffer-then-forward** (ADR-0004): buffering
     makes service memory scale with fan-out, which is NFR-6.
   - `direct` — a short-lived signed URL, so the consumer reads the store
     itself. Used when fan-out is 1 (catch-up replay) or the pod is under
     pressure.
   - **The ingester chooses**, not the consumer. FR-6 says so, and a consumer
     that could demand `direct` at fan-out 300 would reproduce the
     $3,732/month design ADR-0004 rejected.
6. **`BinStore.presign`**, as a capability. `direct` cannot exist without it,
   no backend has it, and `Capabilities` must advertise it so a backend that
   lacks it refuses `direct` at startup rather than at first use — the shape
   `requireConditionalWrites` already establishes.
7. **Session and epoch on the push channel.** A session identifies a
   subscription across reconnects; the epoch tells a consumer the sequencer
   changed under it. Resume is `(session, lastOffset)`.
8. **Prefetch on DURABILITY, not on commit.** The pod that wrote a segment
   still holds it; the other AZs fetch once when it becomes durable rather than
   once per consumer. ⚠️ Prefetching is cache warming with no visibility
   implication, so it starts 20-350 ms before the commit; the PUSH still waits
   for the commit, which is I4. ⚠️ **2 GETs
   per segment for 3 AZs, independent of node count** — that is ADR-0004's
   whole arithmetic and the difference between $25 and $3,732 a month.
9. **A membership seam** for the peer mesh, with a static implementation, and
   the **consistent-hash ring** over it that decides which pod in an AZ fetches.
10. **The fallback ladder** (FR-10). M4 supplied tier 3; M4's SPEC says "the
   ladder itself is M5's". ⚠️ Tier 4 is a LIST and **must never run
   automatically on a hot path** — cost.md R15 caps LIST at ~1/s sustained, and
   M4's SPEC warns that up to 300 plugin nodes scanning breaches it **at
   fan-out, not at M4's own scale**. That warning is inherited here.

⚠️ **TWO OF THESE ARE WIRE-FORMAT CHANGES** and carry the
[`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)
obligations in the commit that makes them: the `BinStore` SPI gaining `presign`
(the skill's trigger names the store SPI) and the subscription protocol gaining
session and epoch (it names the subscription protocol). Each needs its ADR, a
version bump, golden files, every reader and writer and fake updated in one
commit, and **the read side shipped first** — an old plugin receiving a new
event shape stalls consumers cluster-wide, and nothing in a functional test
would catch it.

**Out, explicitly:**

- **The Kubernetes `EndpointSlice` watch.** ADR-0012 makes membership
  authoritative from EndpointSlice, which means the *seam* is the design and
  the watch is an adapter over it. Building it here drags a Kubernetes client
  and a live cluster into a milestone about subscriptions, and every behaviour
  that depends on membership is testable against an injected one. ⚠️ **Stated
  as a consequence rather than hidden: until it lands, membership is
  configuration.** Same shape as M4 deferring the transport, and it goes to
  **M8** with the rest of the chaos matrix.
- **Nothing about placement.** ⚠️ An earlier draft scoped the consistent-hash
  ring OUT while keeping criterion 9's "2 GETs per segment across 3 AZs". Those
  contradict: FR-12 mandates ≥2 ingester nodes per AZ, so without a rule saying
  WHICH pod in an AZ fetches, each fetches for its own consumers — 2 pods × 2
  non-writing AZs = **4 GETs**, and the criterion fails. The ring is the
  mechanism ADR-0012 names for exactly this ("consistent-hash ring over that
  membership — *computed*, not discovered"), and its miss ladder is
  `local cache -> ring owner in the same AZ (proxy, streamed through) -> object
  store`. It is IN.
- **The degraded `ctl/inbox/` path.** M4's SPEC calls it the degraded
  counterpart to forwarding and says it cannot precede it. It is the tier of
  the fallback ladder below forwarding, so it belongs with the chaos matrix at
  **M8**.
- **Cross-AZ peer fetch.** Not deferred — *forbidden*. ADR-0012 measures it at
  419× an object-store GET. Peer fetch is intra-AZ only, and a test asserts it.
- **`IngestionMessageMapper`, `os_routing`, aliases.** M6.
- **Priority lanes.** M10; the lane byte is already reserved in the format.

## Design

### Commit forwarding, and why it is not a new coordination mechanism

```
pod B (not leaseholder)                pod A (leaseholder)
  DefaultIngest                          LocalSequencer
    └── RemoteSequencer                    └── CommitLog ──▶ object store
          │  reads lease ──▶ holderEndpoint
          └── SequencerTransport ─────────▶ commit(CommitRequest)
                                         ◀───────── CommitDelta
```

The forwarding pod holds **no** coordination state. It reads the lease to learn
where to send, sends, and on refusal re-reads. If the lease has moved, the new
holder answers; if the request was already applied, M4.10's idempotency answers
it with the offsets that already apply and appends nothing.

⚠️ **Idempotency is what makes forwarding safe, and TWO OF ITS THREE PARTS ARE
MISSING.** A forwarded commit whose *response* is lost is the ambiguous case:
the pod does not know whether it was applied, so it retries. That retry is
answered rather than duplicated only if all three hold:

1. the key is `(podId, incarnationId, flushSeq)` — **true today** (ADR-0036);
2. the retry reuses the same triple — **false today**: `DefaultIngest.java:355`
   writes `flushSeq++` at the call site, and `Sequencer`'s contract says "THIS
   GUARANTEE STOPS AT THIS SEAM and no production caller yet reaches it";
3. the window survives a takeover — **false today**: `Sequencer.java:59-61`
   says "A SUCCESSOR INHERITS NOTHING ... a replay that crosses a takeover
   commits twice."

M5.1 and M5.2 supply (3) and (2), and they land **before** M5.6 wires
forwarding. Wiring it first is duplicate records at committed offsets, which I2
forbids and no existing test covers.

**Rejected:** having every pod write the chain directly and race on
`putIfAbsent`, which is what the tree did before M4. It is safe for I1 —
write-once *is* the safety mechanism — but it has no leader, so nothing can be
sealed and nothing can be fenced, which is the whole reason M4 introduced the
lease.

**Rejected:** a peer-to-peer election or a hint cache deciding who leads.
ADR-0012: every peer-derived fact must be verifiable or harmless; leadership is
neither, so it comes from the store.

### The three fetch modes, and who chooses

| Mode | Bytes travel | Store requests by the consumer | When |
|---|---|---|---|
| `inline` | with the push | 0 | the default, and the only one M1 had |
| `proxy` | streamed through the ingester on request | 0 | segment too large to inline, or a late subscriber |
| `direct` | consumer reads the store over a signed URL | 1 | fan-out 1 (catch-up replay), or the pod is under pressure |

⚠️ **`inline` is the default and stays it**, because it is the mode under which
a consumer issues no request at all. `direct` is the escape hatch whose cost
ADR-0004 priced; making it the default is the $3,732/month design.

⚠️ **Signed URLs are never logged** (security.md rule 4), are read-only, scoped
to one key, and short-lived (rule 3).

### Prefetch on durability

When a segment becomes DURABLE, the ring owner in each other AZ fetches it once; consumers in that AZ are served
from it. The writing AZ fetches nothing — the writer still holds the bytes. Two
GETs per segment for three AZs, **independent of node count**, which is NFR-4.

**Rejected:** fetch-on-demand per consumer. At ~400 streams per node it makes
the read rate scale with consumers, which is exactly what NFR-4 forbids.

## Cost impact

⚠️ **Cited by `R`-number, because that is how cost.md's rules resolve.** An
earlier draft filled this column with NFR IDs, so a reader grepping cost.md's
namespace for what M5 touches would have found nothing.

| Rule | What M5 does | Budget |
|---|---|---|
| **R3** *idle consumers issue zero requests* | An idle consumer is parked on a queue; `inline` gives it the bytes it was told about | **0**, asserted at fan-out |
| **R5** *one fetch per object per node, shared by every shard on it* | Prefetch is per segment per AZ, through the ring owner — not per consumer, per shard or per stream | 2 GETs/segment at 3 AZs, flat in node count |
| **R10** *above ~10 data nodes, serve reads from the ingester* | Reads are served BY THE INGESTER in every mode; ⚠️ within that, `inline` is the default delivery and `proxy` the fallback when the bytes are too large to inline -- `direct` is the exception the ingester chooses | $25/mo vs ADR-0004's rejected $3,732 |
| **R11** *cache where fan-in is high, not where it is low* | The cache lives in the ingester pods (fan-in ~100 nodes), not in the plugin | — |
| **R12-R14, R17** *the store is fronted by a cost governor that REFUSES, not merely counts* | M5 adds three discretionary read paths -- proxy fetch, per-AZ prefetch, presigned grants -- and they are the first traffic worth governing. Each declares its `expected` so ratio-to-expected has a denominator | alarm 3x, hard stop 10x; ⚠️ **never refuse a data write or a commit** |
| *(ADR-0012, not an R-rule)* ⚠️ **cross-AZ crossover ≈ 19.5 KiB** | The inline-vs-coordinates decision reads it from `CostTable` | ⚠️ **not hard-coded**, and ⚠️ **not R12** -- an earlier draft cited it as one, which would have sent a reader to the cost governor |
| **R15** *LIST ceiling ~1/s sustained* | Ladder tier 4 is a LIST and is gated behind an explicit recovery action, never a hot path | ⚠️ inherited from M4: 300 plugin nodes scanning breaches it **at fan-out** |
| **R2** *never LIST on a hot path* | No new LIST on any path M5 adds | 0 |
| **R18** *never relax a budget to make a check pass* | No budget moves here | — |
| — | Commit forwarding adds **no object-store request at all**: it is a pod-to-pod RPC, and the leaseholder's commit is the same single PUT it already made | +0 |

⚠️ `direct` is the one mode adding a consumer-side GET, which is why the
ingester chooses it and a consumer cannot demand it. ⚠️ **The fan-out threshold
that selects it is measurement M3, deferred to M9** — so it is configuration
here, not a constant. M4 applied the same discipline to the lease TTL.

## Acceptance criteria

1. **A non-leaseholder pod commits successfully** and gets offsets from the same
   total order: with 3 pods where 1 holds the lease, every commit from the other
   2 lands, I1–I5 hold, and **no pod writes the chain except the leaseholder** —
   asserted by `CountingBinStore` over the chain prefix.
2. **A forwarded commit whose reply is lost is answered, not duplicated**: a
   transport that drops the reply *after* the commit applied, retried, yields the
   same offsets and appends nothing.
3. **And that still holds across a takeover.** The retry goes to a NEW
   leaseholder that never saw the original, and is still answered rather than
   appended — which is only true once the window is inherited. ⚠️ **This is the
   criterion an earlier draft wrote as if already satisfiable.** It is not:
   `IdempotencyWindow` is process-local today and `Sequencer`'s javadoc says a
   replay crossing a takeover commits twice.
4. **A retry reuses its triple end to end**: killing the reply on the
   *production* path — `DefaultIngest`, not a fake — and retrying produces one
   delta, not two. Fails today, because `flushSeq++` sits at the call site.
5. **All three fetch modes deliver identical bytes** for the same segment, and
   the mode is chosen by the ingester — a consumer asking for `direct` at
   fan-out > 1 is served something else.
6. **`proxy` streams through**: serving an N MiB segment to K consumers holds
   service memory flat in K, at K = 1 and K = 64, with a bound independent of K.
7. **`direct` issues a short-lived, read-only, single-key signed URL**; a
   backend without `presign` refuses `direct` **at startup**; and the grant
   appears in **no** log, trace or error message (security.md rules 3 and 4),
   asserted by capturing output while one is issued.
8. **Zero object-store requests attributable to idle consumers, at fan-out**:
   ≥1,000 idle consumers over ≥3,000 intervals of an **injected clock** cause
   **zero** requests through the ingester's `CountingBinStore` and **zero**
   grants issued. ⚠️ **With a recorded red** against a serving path that fetches
   once per empty poll — a green alone repeats M1's non-proof. ⚠️ And the clock
   must be **read**, or the test is the tautology M1.16b was withdrawn for.
9. **Prefetch is per segment per AZ**: a segment fanned to 300 consumers across
   3 AZs issues **2** GETs for it, and **the same 2** at 600 consumers and at 2
   pods per AZ — flat in both consumer and pod count. ⚠️ **Counted over the whole
   run, not over a window scoped to the commit.** Prefetch fires on durability,
   20-350 ms EARLIER than the commit, so a commit-scoped counter observes 0 and
   would pass identically against a fetch-per-consumer implementation.
10. **Cross-AZ peer fetch is not addressable**, not merely avoided: the
    peer-fetch path takes an AZ-scoped member list, so a cross-AZ peer cannot be
    named. ADR-0012 requires this "enforced in code, not just documented".
11. **A session resumes exactly**: reconnecting with `(sessionId, epoch)` plus
    `add`/`remove` deltas yields the next record, no duplicate and no skip; and a
    server-side **reset** signal makes the client re-send full state.
12. **The subscription epoch is not the lease epoch.** A sequencer failover does
    not invalidate a session, and a session reset does not imply a failover —
    asserted both ways. ⚠️ They are different counters and conflating them is the
    obvious defect.
13. **The ladder's tier 4 never runs on a hot path**: a LIST happens only behind
    an explicit recovery action, and 300 simulated plugin nodes in the degraded
    tier stay under cost.md R15's ~1 LIST/s ceiling.
14. **`sequencer`, `client` and `plugin` still compile with no Helidon**, and
    **`client` and `plugin` still import no `binjava.binstore`** —
    `GATE_SCOPE=full ./scripts/check-module.sh` green, and ADR-0023's dependency
    check green.

## Test plan

| Tier | Behaviour | Fails first against |
|---|---|---|
| T0 | `FetchMode` selection policy | a policy that lets the consumer choose |
| T0 | signed-URL shape: TTL, read-only, one key | a URL with no expiry |
| T1 | `RemoteSequencer` forwards and returns offsets | a stub that commits locally |
| T1 | forwarded commit, reply dropped, retried | a non-idempotent forward |
| T1 | lease moves mid-run → re-read and reach the new holder | a cached endpoint |
| T1 | `proxy` streams through | a buffer-then-forward implementation |
| T1 | memory flat in consumer count at K=1 and K=64 | the same |
| T1 | **zero requests attributable to idle consumers at fan-out**, counted at the INGESTER through `CountingBinStore` plus grants issued | a serving path that fetches once per empty poll |
| T1 | prefetch: 2 GETs per segment per 3 AZs, flat in consumers | fetch-per-consumer |
| T1 | cross-AZ peer fetch refused | one that merely prefers intra-AZ |
| T1 | session resume, epoch change surfaced | a resume that replays or skips |
| T1 | commit-protocol simulation extended with forwarding pods | forwarding that bypasses the lease |
| T1 | a retry across a TAKEOVER is answered, not appended | a process-local window |
| T2 | end-to-end: a non-leaseholder pod's write is searchable | — |
| T4 | the cluster tests still pass with forwarding wired | — |

⚠️ **The simulation is extended, not replaced.** M4's 1,000-seed sweep gains
pods that forward rather than lead, so I1–I5 are asserted over a fleet where
most pods do not hold the lease — which is the deployment M5 makes correct.

Mutation expectations: 80% on changed classes, and the checkers M4.13k records
as invisible to the gate remain so until `check-mutants.sh` is wired.

## Risks

| Risk | What reveals it |
|---|---|
| Forwarding becomes a second coordination path | Criterion 1's assertion that only the leaseholder writes the chain |
| Forwarding is wired before a retry is answerable | Criteria 3 and 4, which fail against today's tree by construction |
| `proxy` buffers under load | Criterion 6's memory bound, at K=64 |
| `direct` becomes the default by accident | Criterion 5: the ingester chooses, and a consumer cannot demand it |
| The zero-idle criterion repeats M1's non-proof | Criterion 8 requires a **recorded red** against a serving path that fetches per empty poll, and requires the injected clock to be READ -- M1.16b was withdrawn for a clock that was not |
| Membership-as-configuration is mistaken for the finished mesh | Stated in Scope, and in the roadmap row |

## Tasks

One commit each. ⚠️ **The first three are prerequisites, not the headline.**
Forwarding cannot be wired safely until a retry is answerable, and today it is
not — across a takeover or on the production path.

| ID | Task |
|---|---|
| M5.0 | This spec, the roadmap row corrected to name commit forwarding, and NFR-7/NFR-13 reassigned rather than silently claimed |
| M5.1 | Inherit the idempotency window across a takeover (the unowned M4.10f) |
| M5.2 | A retry reuses its triple: move `flushSeq` off the call site in `DefaultIngest` |
| M5.3 | `SequencerTransport` seam and its fake |
| M5.4 | `RemoteSequencer`: read the lease, forward, return the delta |
| M5.5 | Follow the lease when the target refuses or it moves |
| M5.6 | Wire forwarding into `DefaultIngest` — **the correctness hole closes here** |
| M5.7 | Extend the commit-protocol simulation with pods that forward rather than lead |
| M5.8 | The membership seam, static implementation, and the AZ-scoped ring |
| M5.9 | Cross-AZ peer fetch not addressable by construction |
| M5.10 | ADR + `BinStore.presign` and `Capabilities.presignedUrls`, both backends, conformance, golden files (wire-format-change) |
| M5.11 | `FetchMode` and the ingester-side selection policy, threshold configurable |
| M5.12 | `proxy`: stream through, memory flat in consumer count |
| M5.13 | `direct`: the grant, refused at startup without the capability, never logged |
| M5.14 | ADR + session and epoch on the subscription, read side first, golden files (wire-format-change) |
| M5.15 | Session resume, reset signal, and the two epochs kept distinct |
| M5.16 | Prefetch on **durability**, per segment per AZ through the ring owner |
| M5.18 | The fallback ladder, with tier 4 gated off every hot path |
| M5.19 | **Zero requests attributable to idle consumers at fan-out**, with a recorded red |
| M5.20 | M5's `VERIFIED.md` and the milestone review |

⚠️ **Prefetch is on DURABILITY, not on commit**, and the corpus corrects itself
on this: "an ingester node should prefetch a segment when it becomes durable,
not when it is committed. Prefetching is cache warming with no visibility
implication, so it can start 20–350 ms earlier". The PUSH still waits for the
commit — that is I4 — but the fetch need not.

⚠️ **The `ctl/inbox/` degraded path has no owning milestone.** M4 scoped it out
saying it "cannot precede" forwarding, and it appears in no roadmap row, no
backlog row and no ADR. M5 makes forwarding exist, so M5 is the earliest
milestone that *could* own it — and does not: it is the tier below forwarding
in the ladder and belongs with the chaos matrix at **M8**. Assigned here so it
stops being unowned, which is the defect M4's own deployment-constraint section
exists to demonstrate.
