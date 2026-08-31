# AZ topology and data flow

**Status:** proposal · **Confidence:** high for the AWS cost facts, medium for the K8s mechanics
(verify against the target cluster) · **Last updated:** 2026-08-29

**Read this if:** you are writing the Kubernetes manifests, the endpoint discovery logic, or
reasoning about where bytes travel.
**One-line takeaway:** the object store is both the storage layer **and** the cross-AZ network. Every
byte of record data moves only along free paths (compute ↔ object store, same region); the only
cross-AZ traffic is commit metadata.

---

## 1. The cost topology

| Path | Charge |
|---|---|
| EC2/EKS pod ↔ S3, same region | **free** |
| pod ↔ pod, **same AZ** | **free** |
| pod ↔ pod, **cross-AZ, same region** | **$0.01/GB each direction ($0.02 round trip)** |
| S3 replication across AZs (inside the ingester) | **free / included** |

WarpStream's framing is exactly right and worth internalising: the agent *"leverages the free
networking between EC2 and S3, which just so happens to durably replicate your data along the way."*
**We do not build cross-AZ replication. We let S3 do it, for free, as a side effect of a PUT.**

## 2. The data flow

```
   AZ-a                          AZ-b                          AZ-c
 ┌──────────┐                 ┌──────────┐                 ┌──────────┐
 │ producer │                 │ producer │                 │ producer │
 └────┬─────┘                 └────┬─────┘                 └────┬─────┘
      │ HTTP (same-AZ)             │                            │
 ┌────▼─────┐                 ┌────▼─────┐                 ┌────▼─────┐
 │ ingester │                 │ ingester │                 │ ingester │
 │  pod A   │                 │  pod B   │                 │  pod C   │
 └────┬─────┘                 └────┬─────┘                 └────┬─────┘
      │ PUT segment (free)         │                            │
      └──────────────┬─────────────┴──────────────┬─────────────┘
                     ▼                            │
            ╔══════════════════╗                  │ commit metadata only
            ║   OBJECT STORE   ║                  │ (~30 B per stream per segment)
            ║  (regional, 3AZ) ║           ┌──────▼───────┐
            ╚═════┬═══════┬════╝           │  sequencer   │  (a leased role on one
                  │       │                │  (pod B)     │   of the same pods)
   GET range (free)│       │(free)          └──────┬───────┘
            ┌─────▼──┐ ┌──▼─────┐                 │ push events (metadata, same-AZ preferred)
            │ OS node│ │ OS node│ ◄───────────────┘
            │  AZ-a  │ │  AZ-c  │
            └────────┘ └────────┘
```

**Rules the diagram encodes:**
1. Producers reach an ingester **in their own AZ** (topology-aware routing).
2. An ingester writes segments containing whatever it received; it never forwards record bytes.
3. Commit metadata may cross AZs. It is small, and it is the only thing that does.
4. OpenSearch nodes read segment byte ranges **directly from the object store** — free, in-region.
5. The tail subscription prefers a **same-AZ** ingester node, but crossing an AZ for metadata is
   acceptable (kilobytes/s).

## 3. Kubernetes shape

```yaml
# Ingester: stateless Deployment, spread across zones
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector: { matchLabels: { app: bin-ingester } }
env:
  - name: NODE_ZONE       # projected from the node label via the downward API + a small init step,
    value: ...            # or read from the K8s API at startup
  - name: POD_ID
    valueFrom: { fieldRef: { fieldPath: metadata.uid } }
```

- **`Deployment`, not `StatefulSet`.** Pods hold no durable state; identity comes from the lease
  objects, not from an ordinal hostname.
- **Peer discovery** via a headless `Service` (`clusterIP: None`) — DNS returns all pod IPs — or via
  the K8s API with a label selector. Needed for the commit RPC and push fan-out
  ([discovery-and-tailing](04-discovery-and-tailing.md) §4). This is the "nodes communicate to
  inform each other about transient state" capability from the brief.
- **Producer-facing routing:** either one `Service` per zone (`bin-ingester-us-east-1a`) selecting
  pods pinned to that zone, or a single Service with topology-aware routing
  (`spec.trafficDistribution: PreferClose`, or the older
  `service.kubernetes.io/topology-mode: Auto`). **Explicit per-zone `Service`s are less magical and
  fail more visibly** — prefer them until there is a reason not to.
- **PodDisruptionBudget** so a rolling node upgrade cannot take every sequencer at once.
- **Graceful shutdown:** on `SIGTERM` — flush buffered records, commit, release held leases
  (a voluntary `putIfMatch` that expires them immediately), then close. Releasing leases turns a
  TTL-length stall into a sub-second handover; skipping it is the difference between a clean deploy
  and a 30-second visibility gap on every rollout.

## 4. AZ awareness in the plugin

The plugin must resolve a **same-AZ** service endpoint. Sources, in order of preference:

1. **Explicit config:** `index.ingestion_source.param.ingester_endpoints` as a zone→endpoint map,
   plus the node's zone from an OpenSearch node attribute
   (`node.attr.zone`, which OpenSearch already uses for shard allocation awareness). **This is the
   most robust option** — it reuses a value the cluster operator has already had to set correctly.
2. **Cloud metadata:** IMDSv2 `placement/availability-zone` on AWS; equivalents elsewhere. Works
   without configuration, adds a cloud dependency to the plugin, and is awkward under the plugin
   security policy.
3. **Service-assisted:** connect to any endpoint, let the ingester reply with a same-AZ redirect based
   on the source IP. Simple, one extra round trip at startup, no client config.

**Recommendation: (1) with (3) as fallback.** Log loudly when the plugin ends up on a cross-AZ
endpoint — silent cross-AZ chatter is exactly the failure this project exists to prevent, and it
will otherwise only be discovered on an invoice.

> **Note:** WarpStream had to bolt AZ hints onto the Kafka client ID and explicitly warns against
> using Kafka's own rack-awareness because it triggers rebalance storms
> ([warpstream.md](../10-prior-art/01-warpstream.md) §5). We have no such legacy — put zone
> awareness in the protocol from day one.

## 5. Read amplification across AZs — the recommendation depends on cluster size

Each AZ must read each relevant segment at least once (that is the price of not paying cross-AZ
transfer). WarpStream accepts a 3× GET multiplier for a 3-AZ deployment and states it is >10×
cheaper than the bandwidth alternative. Our multiplier is **nodes per AZ** unless nodes share a
cache — and that is the number that decides the design.

| Data nodes | Per-node fetch, 250 ms flush | With 2 s flush | With per-AZ shared cache (2 s) |
|---|---|---|---|
| 9 (3/AZ) | $112/mo | $14/mo | $2/mo |
| 100 | $1,244/mo | $156/mo | $5/mo |
| **300** | **$3,732/mo** | **$467/mo** | **$25/mo @250 ms · $3/mo @2 s** |
| 600 | $7,465/mo | $933/mo | $5/mo |

> ⚠️ **Revised 2026-08-29.** An earlier version of this document concluded "$75/month is not worth a
> distributed cache — ship the node-local cache." That holds for a 9-node cluster. At the real target
> scale (~120,000 shards ⇒ **~300 data nodes**, see
> [cost-model §5b](../00-problem/02-cost-model.md)) the same choice costs **$3,700/month**, and the
> shared cache becomes the highest-value component on the read side.

### Two levers, in order of cost-effectiveness

**Lever 1 — lengthen the flush interval (free).** 250 ms → 2 s cuts segment count 8× and therefore
read requests 8×, at the cost of ~2 s of ingestion latency. This is precisely the trade the brief
asks for, it requires no new code, and it should be the **default at large cluster sizes**.

**Lever 2 — share the fetch within an AZ.** Three places to put the shared cache:

| Where | Mechanism | Pros | Cons |
|---|---|---|---|
| **Ingester nodes** (recommended) | plugin fetches ranges from a same-AZ ingester node. **In the writing AZ the bytes are still in the pod's write buffer ⇒ 0 GETs**; other AZs do 1 GET per segment. Net: **2 GETs per segment for 3 AZs**, independent of node count ([discovery-and-tailing §2b](04-discovery-and-tailing.md)) | reuses a cluster that already exists, already has peer discovery, already is AZ-aware; no new distributed system inside OpenSearch; removes an S3 round trip from tail latency | adds a bandwidth tier — size ingester nodes for ~100 MiB/s egress per AZ; same-AZ traffic is free, so it costs capacity, not transfer |
| OpenSearch data nodes | consistent-hash ring among nodes in an AZ, à la WarpStream's distributed mmap | no extra tier or bandwidth | a distributed cache inside a plugin, in someone else's JVM, competing for the data node's heap and circuit breakers. Substantial complexity |
| Nowhere (node-local only) | each node GETs what it needs | simplest | the $3,732/month column — **and 29.3 GiB/s of aggregate read bandwidth, 100 MiB/s per node, for 0.33 MiB/s of useful data** |

**Recommendation (revised):** **Lever 2 via the ingester nodes is the primary decision** — see
[discovery-and-tailing §2a](04-discovery-and-tailing.md) for the full comparison. Once reads are
proxied, read cost stops depending on the flush interval ($25/month at 250 ms, $3/month at 2 s), so
**Lever 1 is no longer needed to control cost** and the flush interval can be chosen purely for
latency. Direct plugin→S3 reads survive only as the break-glass fallback.
Note this partially re-adopts WarpStream's agent-serves-fetches model, which
[comparison-matrix §2](../10-prior-art/04-comparison-matrix.md) lists as something we could skip.
That claim is true at small cluster sizes and false at 300 nodes — the reason WarpStream built the
distributed cache is the reason we eventually need one.

**A third option, not recommended:** placement-aware segment assembly — grouping streams into
segments by which OpenSearch node hosts their shards, so fewer nodes need each segment. It would cut
fan-out dramatically, but it couples the ingester to OpenSearch shard placement and breaks on every
relocation. Recorded in [50-open-questions](../50-open-questions.md) Q18 rather than recommended.

## 5b. The peer mesh: what pods tell each other

Decision: [ADR-0012](../../internal/product/decisions/0012-peer-mesh-without-gossip.md).
Pods do talk to each other, but **there is no gossip protocol** — membership comes
from Kubernetes and placement is computed, not discovered.

### What is computed vs communicated vs stored

| Fact | Source | If stale |
|---|---|---|
| AZ pod membership | K8s `EndpointSlice` **watch** | a request to a dead pod, retried |
| Which pod owns segment *S* in this AZ | `consistentHash(S)` over that membership | one extra GET |
| Which pod wrote *S* | the key's `<podShortId>` — the writer serves its own segments regardless of ring position | nothing; it is in the key |
| Who is the sequencer | the **lease object**; peer hints may accelerate | a fenced commit, retried |
| Which pod has subscribers for a stream | explicit **registration** with the sequencer | a missed push, recovered from the chain |
| Record order | the **commit log** | never stale — it is the truth |

⚠️ **Every peer-derived fact must be verifiable or harmless.** Acting on a stale
one may cost a redirect or an extra GET; anything that could cause divergence
comes from the store.

### Peer fetch is intra-AZ only — the 419× rule

| Serving an 8 MiB segment | Cost |
|---|---|
| Intra-AZ peer fetch | **free**, +~0.5 ms |
| Object-store GET | $0.0000004 |
| **Cross-AZ peer fetch** | **$0.000168 — 419× the GET** |

⚠️ At $0.02/GB, moving a segment across an AZ boundary costs **419× more than
asking the object store**, which in-region charges no transfer at all. This
inverts the usual "ask a peer before you ask storage" intuition, and it is why the
peer-fetch path must take an **AZ-scoped** member list: a cross-AZ peer should not
be addressable by construction, not merely discouraged by a comment.

### The miss ladder

```
local cache
   -> ring owner in the SAME AZ   (proxy, streamed through -- never buffer-and-forward)
       -> object store            (single-flight; concurrent requests coalesce)
```

Each step is tried once with a bounded timeout. An unreachable ring owner means
the pod fetches from the object store itself — correct, one extra GET, no
coordination needed.

### Metadata is not exempt — the ~19.5 KiB crossover

```
break-even = (one GET $4x10^-7) / (cross-AZ $0.02 per GB) = 19.5 KiB
```

| Payload | Cross-AZ vs one GET | |
|---|---|---|
| Lease (~1 KiB) | 0.1× | ship |
| Trickle batch (~8 KiB) | 0.4× | ship |
| **Commit delta (~150 KiB)** | **7.7×** | **fetch from the store** |
| Checkpoint (~2.9 MiB) | 148× | fetch |
| Segment (8 MiB) | 419× | fetch |

⚠️ **"It's only metadata" does not license cross-AZ chatter.** A commit delta is
already seven times past the crossover. Intra-AZ, a peer is always preferable
because the transfer is free; cross-AZ, compare against 19.5 KiB.

### What metadata gossip would save, given directed push exists

| | Cost |
|---|---|
| Every pod tails the chain from the store | $75/mo (6 pods) · $224/mo (18 pods) |
| Directed push (current design) | ~$0 in GETs |
| Gossip as a fallback on top of push, at 1% miss | **saves $0.75/mo** |

Directed push already captures the whole $75–224. Gossip's incremental saving is
under a dollar a month, and it would add 2–3 rounds of convergence latency to the
one path optimised hardest. Its structural benefit — spreading fan-out off the
sequencer — needs ~100 pods to bind (182 MB/s); at 6–18 pods the sequencer emits
9–31 MB/s.

**Adopted instead: piggybacked digests.** Extra fields on RPCs already flowing —
`{chainPosition, registryVersion, ringGeneration, leaseEpoch}`. A pod behind a
**same-AZ** peer pulls the gap from that peer instead of the store. No timers, no
convergence semantics, no new failure mode. Deltas are write-once and CRC-covered,
so a peer-supplied one is either the real record or detectably corrupt, and the
chain in the store stays the arbiter. Every digest is a hint.

### Why not gossip a cache directory

Gossip would close the gap between what the ring says a pod *should* hold and
what it *does* hold: churn, eviction, in-flight prefetch. That gap was measured —
a full six-pod rolling restart causes about **360 extra GETs, $0.00014**. A
convergence protocol and an inconsistent view are not worth a fraction of a cent,
and the ring answers "who has it" with **no messages at all**.

Gossip's one real advantage is failure detection faster than a readiness probe.
A Kubernetes `EndpointSlice` **watch** is also sub-second, and additionally
authoritative and free. **In Kubernetes, gossip solves a problem the platform
already solves.** It remains the right answer off Kubernetes, and is recorded as
the fallback rather than built speculatively.

### Directed fan-out, not broadcast

Broadcasting each commit to every peer costs 12 commits/s × ~150 KB × 5 peers =
9.2 MB/s, two-thirds of it cross-AZ ⇒ **~$319/month**. Subscriber registration
makes the fan-out directed at ~1.8 MB/s — cheaper *and* exact.

## 6. Failure domains

A summary table lived here; it has been superseded by a full treatment in
**[08-failure-domains-and-resilience.md](08-failure-domains-and-resilience.md)** — minimum topology
(**≥2 ingester nodes per AZ**), the three singleton-ish roles hiding inside a "stateless" service,
the detection-independent safety argument, the failure matrix, graceful-shutdown ordering, RTO/RPO,
and the chaos matrix.

The one-line version: **an AZ failure costs ⅓ of capacity and a lease-failover stall; it never costs
data**, because we ack only after the segment is durable in a *regional* (multi-AZ) bucket and
committed.
