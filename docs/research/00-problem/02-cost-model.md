# The Cost Model — the arithmetic that drives every design decision

> **This is the most important document in the corpus.** Every architectural choice in
> `30-design-space/` is justified by a number in here. If you change a design, re-run these
> sums first.

**Status:** stable · **Confidence:** high for AWS list prices (verified Aug 2026), medium for the worked scenarios (assumptions stated inline) · **Last updated:** 2026-08-29

---

## 1. Unit prices (AWS us-east-1, S3 Standard, Aug 2026)

| Item | Price | Per-unit |
|------|-------|----------|
| PUT / COPY / POST / **LIST** | $0.005 per 1,000 | $5.0 × 10⁻⁶ |
| GET / SELECT | $0.0004 per 1,000 | $4.0 × 10⁻⁷ |
| DELETE | **free** | $0 |
| Storage (S3 Standard) | $0.023 per GB-month | — |
| S3 → EC2, **same region** | **free** | $0 |
| EC2 ↔ EC2, **cross-AZ, same region** | $0.01/GB **each direction** | $0.02/GB round trip |

### Three facts to memorise

1. **A LIST costs exactly as much as a PUT — 12.5× a GET.** LIST is not a cheap "peek";
   it is the most expensive way to discover anything. Design to never LIST on a hot path.
2. **1 sustained request/second for a month:**
   - 1 PUT/s ≈ **$13/month** · 1 LIST/s ≈ **$13/month** · 1 GET/s ≈ **$1/month**
   (86,400 × 30 = 2.59M requests/month.)
3. **Bytes moved between S3 and same-region compute are free.** Only *requests* cost money.
   This has a counter-intuitive consequence — see §6 "always coalesce".

### Other providers
Roughly the same shape, different constants. GCS Class A (write/list) ops and Azure Blob
write operations are the expensive tier; reads are ~10× cheaper; same-region egress to
compute is free on both. Do not hard-code AWS ratios into the code — expose them as a
cost table in the [pluggable store SPI](../30-design-space/07-pluggable-store-abstraction.md).

### S3 Express One Zone — flagged, not adopted
AWS cut Express prices in April 2025 (storage −31%, PUT −55%, GET −85%), and it delivers
single-digit-ms latency at up to 200k PUT/s per directory bucket. **But it is single-AZ**,
so a consumer in another AZ pays cross-AZ transfer for every byte — which directly violates
constraint C2. ✅ **Computed and rejected (2026-08-30).** Post-April-2025 prices: PUT $0.00113/1,000 (4.4× cheaper
than Standard), GET $0.00003/1,000 (13× cheaper), storage **$0.11/GB-mo (4.8× more expensive)**.
For Scenario B at 6 h retention:

| | API | Storage | Total |
|---|---|---|---|
| S3 Standard | $336/mo | $50/mo | **$386/mo** |
| S3 Express One Zone | $72/mo | $238/mo | **$310/mo** |

Once the API bill is already small, the storage penalty eats the request saving — a 20% difference,
not an order of magnitude — and Express is **single-AZ**, violating NFR-8 (RPO 0) and NFR-10. A
per-AZ-bucket hybrid would reintroduce exactly the cross-AZ replication this project exists to
avoid. **Not adopted at any layer.**

---

## 2. Why Kafka is expensive here (the baseline we must beat)

For a 100 MiB/s ingest workload across 3 AZs with RF=3:

| Component | Traffic | Cost |
|-----------|---------|------|
| Producer → leader (⅔ of writes land cross-AZ) | 67 MiB/s | |
| Leader → 2 followers (both cross-AZ) | 200 MiB/s | |
| Consumer fetch (⅔ cross-AZ, no rack-aware fetch) | 67 MiB/s | |
| **Total cross-AZ** | **~334 MiB/s ≈ 28.2 TB/day** | **≈ $564/day ≈ $17,100/month** |

…before any broker EC2 or EBS cost. WarpStream's published example makes the same point
with different numbers: ~140 MiB/s in with 3 consumers costs **$641/day** in inter-zone fees
on a perfectly tuned Kafka cluster, versus **<$40/day** of S3 API cost for the same workload.

**The entire value proposition of this project is converting that $17,100/month network bill
into a ~$300/month API bill.** Everything else is secondary.

---

## 3. The naive object-store design is *also* expensive

If you write one object per partition every 250 ms (i.e. mimic Kafka segments on S3):

```
cost/month = P partitions × 4 PUT/s × $13/month-per-PUT-per-second
           = $52 × P per month
```

- 100 partitions → $5,200/month
- 1,000 partitions → **$52,000/month** — worse than the Kafka bill it was meant to replace.

This is the trap. WarpStream states the same figure independently (~$50/month per partition
at 250 ms rotation). **Bundling many partitions per object is not an optimisation; it is the
whole architecture.**

---

## 4. The cost formula

For a system that bundles, the *only* things that matter:

```
writes/s   = (ingester nodes) × (flushes per second per pod)     ← independent of partition count
commits/s  = (sequencer groups) × (commit flushes per second)   ← independent of index count
reads/s    = (objects/s) × (readers that need that object)      ← reduce via layout + caching
lists/s    = 0                                                  ← must be zero on the hot path
idle cost  = 0                                                  ← must be zero when no traffic
```

Note what is **absent**: partition count, index count, shard count, document count. If any of
those appear in your request-rate formula, the design is wrong.

---

## 5. Worked scenario — "Scenario A"

**Assumptions** (state these when quoting the result):
100 indices × 16 partitions = **1,600 partitions**; **100 MiB/s** aggregate ingest;
**3 AZs**; **3 ingester nodes** (one per AZ); **9 OpenSearch data nodes** (3 per AZ);
flush trigger 250 ms *or* 8 MiB; 24h retention.

### Write path
| Item | Rate | Cost/month |
|------|------|-----------|
| Data objects: 33.3 MiB/s per pod ⇒ size-triggered ≈ 4 objects/s/pod × 3 pods | 12 PUT/s | $156 |
| Commit deltas: 3 sequencer groups × 4 flushes/s | 12 PUT/s | $156 |
| **Write total** | **24 PUT/s** | **$312** |

**0.24 requests per MiB ingested.** Target (<0.3) met.

> **Optimisation W3 — piggybacked commits.** The sequencer for a group is itself an ingester
> pod. If it appends its group's commit delta as an extra section of the data object it was
> going to write anyway, commit PUTs drop to **zero** and the write path halves to $156/month
> (0.12 req/MiB). Cost: commit visibility is tied to the leader's flush cadence (adds ≤ 1 flush
> interval of latency). See [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §6.

### Read path
Every object contains data for nearly every node (1,600 partitions spread over 9 nodes ⇒
~178 partitions/node), so essentially every node needs a piece of every object.

| Strategy | Requests | Cost/month | Verdict |
|----------|----------|-----------|---------|
| One range GET per partition per node | 178 × 12/s × 9 = 19,224 GET/s | **$19,800** | Catastrophic — never do this |
| One whole-object GET per node | 12/s × 9 = 108 GET/s | **$112** | Baseline. 9× read amplification of bandwidth (free, but real CPU/NIC) |
| Sorted layout + coalesced range GET (~2 reqs/node/object) | ~216 GET/s | **$224** | Worse on requests, far better on bytes |
| Whole-object GET + **node-local** block cache shared by all shards | 108 GET/s | **$112** | **Recommended v1** |
| + per-AZ sharing across the 3 nodes in an AZ | 36 GET/s | $37 | v2; adds a distributed cache (WarpStream's "distributed mmap") |

**~1.08 requests per MiB consumed cluster-wide, 0.36 per AZ.** Target met.

The first row is the single most important negative result in this document: **byte-range GETs
are billed per request, not per byte, so a "precise" reader that fetches exactly its own
partitions is 175× more expensive than a "wasteful" one that reads the whole object.**

### Storage
8.64 TB/day ingested. At 24h retention: 8,640 GB × $0.023 = **$199/month** — *more than the
API cost*. At 6h retention: **$50/month**.

> **Finding:** in this system the object store holds *transient* data (the plugin indexes it
> within seconds), so retention is a pure cost dial, unlike Kafka where multi-day retention is
> a feature. Default retention should be hours, not days, sized to worst-case consumer downtime.

### Cross-AZ metadata
~30 bytes of metadata per (object, stream). 1,600 streams/object × 12 objects/s ≈ 576 KB/s,
of which ~⅔ crosses an AZ ⇒ ~34 GB/day ⇒ **$0.68/day ($20/month)**.
That is 0.4% of ingested bytes — above the 0.1% target, so **delta-encode and compress the
commit metadata** (sorted partition IDs delta-encode to ~1–2 bytes each; expect 4–6× shrink,
bringing it to <0.1%).

### Scenario A total
| | |
|---|---|
| Write API | $156–312 |
| Read API | $112 |
| Storage (6h retention) | $50 |
| Cross-AZ metadata | $20 |
| **Marginal total** | **≈ $340–495/month** |
| Kafka equivalent (network only) | ≈ $17,100/month |
| **Ratio** | **~40–50×** |

---

## 5b. Scenario B — the real target scale

**Assumptions:** **10,000 indices** — ~10 mega indices at up to **2,000 shards** each (20,000
partitions) plus ~9,990 ordinary indices at 6–15 shards (~99,900 partitions) ⇒ **U ≈ 120,000
streams**; therefore ~120,000 OpenSearch shards over **~300 data nodes** (3 AZs, ~100/AZ);
100 MiB/s aggregate ingest; 3 ingester nodes.

### What does *not* change: the write path
| Item | Rate | Cost/month |
|------|------|-----------|
| Data segments: 3 pods × 4/s | 12 PUT/s | $156 |
| Commit deltas: **one sequencer** (`S=1`, [ADR-0007](../../internal/product/decisions/0007-one-sequencer-slot-by-default.md)), batched at 250 ms | **4 PUT/s** | **$52** |
| **Write total** | **16 PUT/s** | **$208** |

⚠️ **Revised 2026-08-30.** This row previously assumed three sequencer groups at
12 PUT/s ($156). ADR-0007 settles on a single sequencer, so the chain is written
by exactly one writer at `1 / commitBatchInterval` — **$52/month, and the commit
batch interval is now the post-PUT latency dial**: 250 ms costs $52/mo and adds
0–250 ms; 50 ms costs $259/mo and adds 0–50 ms. See
[discovery-and-tailing §5](../30-design-space/04-discovery-and-tailing.md).

⚠️ Note the data-PUT row assumes 3 pods. The HA minimum is **6**
([failure-domains §2](../30-design-space/08-failure-domains-and-resilience.md)),
which doubles time-triggered PUTs to 24/s unless the **adaptive flush interval**
(§3 there) lengthens the interval to keep segments full. Budget the range.

Identical to Scenario A. **Bundling makes the write path completely indifferent to going from 1,600
streams to 120,000.** That is the whole point of rule R1, and it is worth re-stating: a 75× increase
in stream count costs **zero** additional write requests.

### What changes decisively: the read path
Read cost scales with **node count**, not stream count, because at ~500 distinct indices per segment
essentially every node needs a piece of every segment.

| Flush interval | Segments/s | × 300 nodes | Cost/month |
|---|---|---|---|
| 250 ms | 12 | 3,600 GET/s | **$3,732** |
| 1 s | 3 | 900 GET/s | $933 |
| **2 s** | **1.5** | **450 GET/s** | **$467** |

At 600 nodes, double these. **The read path now dominates total cost by an order of magnitude** —
the opposite of Scenario A, where it was $112 against $312 of writes.

### Two mitigations, and a reversed recommendation

| Mitigation | Segments/s | GET/s | Cost/month | Note |
|---|---|---|---|---|
| Baseline (250 ms, per-node fetch) | 12 | 3,600 | $3,732 | |
| Longer flush (2 s) | 1.5 | 450 | $467 | **free**; costs ~2 s of latency — exactly the trade the brief asks for |
| **+ reads served by the ingester nodes** | 1.5 | **3** | **$3** | 1 GET per segment per *non-writing* AZ; the writing AZ serves from its own write buffer |
| **same, but back at a 250 ms flush** | 12 | **24** | **$25** | proxying **decouples read cost from the flush interval** — so keep the low latency ([discovery-and-tailing §2a](../30-design-space/04-discovery-and-tailing.md)) |
| Per-AZ cache alone (250 ms) | 12 | 36 | $37 | |

> ⚠️ **This reverses the v1 recommendation in
> [az-topology](../30-design-space/05-az-topology-and-data-flow.md) §5.** At 9 nodes a per-AZ shared
> cache saved $75/month and was not worth building. At 300 nodes it saves **$3,700/month** and
> becomes the single highest-value component on the read side. See that document for *where* to put
> the cache — the answer is probably the ingester nodes, which are already a per-AZ cluster with peer
> discovery, rather than a consistent-hash ring inside OpenSearch data nodes.

### The idle-cost number becomes existential
120,000 shards × ~10 Hz = **1.2 M `readNext` calls/s**. If any of them touch the object store:

| Naive `readNext` | Cost/month **while idle** |
|---|---|
| GET a marker/manifest | **$1,244,000** |
| LIST a prefix | **$15,552,000** |

Rule R3 is not a nicety at this scale; it is the difference between a viable product and an
absurdity. See [poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md) §3.

### Other scale consequences
- **Commit metadata:** ~5,000 runs/segment × 30 B × 12 segments/s ≈ 1.8 MB/s; ~⅔ crosses an AZ ⇒
  ~$62/month. Fine, but delta-encode it.
- **Checkpoints:** 120,000 streams × ~24 B ≈ **2.9 MB** per checkpoint. Written once a minute —
  cheap, but it means recovery reads a few MB, not a few KB. Size the recovery path accordingly.
- **Sequencer throughput:** 12 segments/s × ~5,000 runs = **60,000 offset assignments/s**. A single
  serial sequencer handles this comfortably (it is a map lookup plus an add), so **S=1 remains
  viable** — see [50-open-questions](../50-open-questions.md) Q2.
- **Segment composition:** without a per-stream trickle policy, a segment would contain thousands of
  single-record runs — wrecking compression, inflating the directory, and making the key filter
  impossible. See [object-layout](../30-design-space/01-object-layout-and-format.md) §7b.

### Scenario B total (250 ms flush, reads served by the ingester)
| | |
|---|---|
| Write API | $312 |
| Read API | $25 |
| Service serving capacity (compute) | ~$150–300 |
| Commit metadata (cross-AZ) | $62 |
| Storage (6 h retention, 8.64 TB/day) | $50 |
| **Marginal total** | **≈ $600–750/month, at 250 ms latency** |

With direct plugin→S3 reads the same workload costs **≈ $4,150/month** *and* moves 29.3 GiB/s of
aggregate read bandwidth (100 MiB/s per data node for 0.33 MiB/s of useful data). Proxying reads is
the largest single cost and bandwidth win available on the read side.

---

## 6. Design rules that fall out of the arithmetic

| # | Rule | Because |
|---|------|---------|
| R1 | Bundle all indices and all partitions of a pod into one object per flush | §3: per-partition objects cost $52/partition/month |
| R2 | Never LIST on a hot path; LIST only for recovery and GC | §1: LIST = 12.5 GETs |
| R3 | Idle consumers must issue **zero** requests | A 100 ms poll loop per shard × 178 shards/node × 9 nodes = 16,020 req/s of *pure idle cost* ($16k/month to discover nothing). See [poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md) |
| R4 | **Always coalesce** adjacent reads; wasted same-region bytes are free, extra requests are not | §5 read table |
| R5 | One fetch per object **per node**, shared by every shard on that node | Otherwise cost scales with shard count |
| R6 | Commit/metadata write rate must be independent of index count | Otherwise 100 indices × 4/s = $5,200/month |
| R7 | Prefer a single speculative range read over a "read footer then read index" two-step | Halves requests on the recovery path; AutoMQ does exactly this ([automq.md](../10-prior-art/02-automq.md) §4) |
| R8 | Retention is a first-class cost dial; default to hours | §5 storage |
| R9 | Every store operation must be counted and attributable; ship a cost meter in the SPI | Otherwise regressions are invisible until the bill arrives |
| R10 | Read cost scales with **node count**, not stream count. Above ~10 data nodes, **serve reads from the ingester nodes** rather than letting the plugin read the object store | §5b: $112/month at 9 nodes becomes $3,732/month at 300, with 300× read amplification |
| R11 | Cache where **fan-in** is high (the ingester node, shared by ~100 nodes), not where it is low (a data node that will never re-read the segment) | [discovery-and-tailing §2a](../30-design-space/04-discovery-and-tailing.md) |
| R13 | **Bundling harder reduces PUT *and* GET.** Prefetch GETs are `segments/s × (AZ−1)`, so fewer, larger segments cost less on both. The PUT/GET tension exists only where consumers fetch whole objects themselves — ADR-0004 removed that | [worked-example §8](../30-design-space/13-worked-example-multi-tenant.md) |
| R15 | **Commit on arrival, rate-limited — never on a fixed timer.** A fixed 250 ms commit interval writes 4 deltas/s whether or not anything arrived; at 1 MiB/s that is 7× the segment rate and $44/month of nothing | [ADR-0016](../../internal/product/decisions/0016-designated-writer-per-az.md) |
| R16 | **A lane's deadline is the only per-request control that changes the object count.** One `+1` record per window cuts the whole segment on that deadline — 20× the PUTs at 250 ms vs 5 s. Cap the high-lane flush rate | [worked-example §10](../30-design-space/13-worked-example-multi-tenant.md) |
| R14 | **PUT rate must track `bytes ÷ segmentSize`, never `writers ÷ interval`** — and the lever is the **flush interval**, not the writer count. Every pod writes; the interval adapts locally. At 1 MiB/s the interval delivers 95% of the available saving and consolidating writers adds $7.78/mo, which does not pay for a hop and a lease | [ADR-0017](../../internal/product/decisions/0017-every-pod-writes.md) |
| R12 | **Cross-AZ crossover ≈ 19.5 KiB** = (one GET) / ($0.02 per GB). Below it, ship the bytes across an AZ; above it, fetch from the store. Applies to metadata as much as to data — a 150 KiB commit delta is already 7.7× past it. Intra-AZ is free, so a same-AZ peer always wins | [ADR-0012](../../internal/product/decisions/0012-peer-mesh-without-gossip.md) |

---

## 7. The cost meter (make this a deliverable, not a spreadsheet)

Wrap the store SPI with a counting decorator that records `{op, count, bytes}` per
`(index, purpose)` and exposes:

- `requests_per_mib_written`, `requests_per_mib_read`
- `estimated_usd_per_tib_ingested` using a per-provider price table
- idle request rate (must be 0)

Assert these in the benchmark harness — see
[benchmarking-plan](../40-implementation/03-benchmarking-plan.md) §5. A cost regression test
is cheaper than a surprise invoice.

---

**Sources:** [S3 pricing](https://aws.amazon.com/s3/pricing/) ·
[AWS cross-AZ transfer](https://aws.amazon.com/ec2/pricing/on-demand/) ·
[WarpStream: Minimizing S3 API Costs with Distributed mmap](https://www.warpstream.com/blog/minimizing-s3-api-costs-with-distributed-mmap) ·
[S3 Express price reduction](https://aws.amazon.com/blogs/aws/up-to-85-price-reductions-for-amazon-s3-express-one-zone/) ·
[Quickwit cost figures](https://quickwit.io/blog/quickwit-101)
