# Worked example: 5M tenants, 10k indices, end to end

**Status:** worked example over decided designs — no new decisions ·
**Confidence:** high (arithmetic over ADRs 0001–0015) · **Last updated:** 2026-08-30

**Read this if:** you want to see the whole path once, concretely, with the
request counts.
**One-line takeaway:** **tenant count appears nowhere in the request-rate model.**
5M tenants or 1 tenant, the same **16.5 PUT/s and 25 GET/s** — because bundling is
keyed by `(index, partition)`, and the routing value is consumed at admission and
discarded.

---

## 1. The workload

| | |
|---|---|
| Tenants | 5,000,000 · routing = `tenantId + "_" + (n mod k)` |
| Indices | 10,000 (10 mega at 2,000 shards, rest 6–15) |
| Streams `(index, partition)` | ~120,000 |
| Ingest | 100 MiB/s, ~100,000 records/s, ~1 KiB average |
| Ingester pods | 6 (2 per AZ × 3 AZ) |
| OpenSearch data nodes | ~300 |

## 2. One request, step by step

### 2.1 Arrival — nothing is parsed
The producer holds one HTTP/2 connection to a **same-AZ** pod and streams frames:

```
header: BING | ver | flags | indexOrAlias="logs"
frame : [RECORD][i8 lane][u16 routingLen]["t_4821993_2"][u32 payloadLen][payload…]
```

The pod reads exactly `payloadLen` bytes straight into a pooled buffer. **The
document is never parsed** — routing and lane arrive out of band precisely so it
does not have to be.

### 2.2 Resolve — three map lookups and one hash
```
alias "logs"        -> indexUUID                     (registration map)
indexUUID           -> indexOrdinal, S, RNS, RF      (registry, cached)
partition           = floorMod(murmur3_32("t_4821993_2"), RNS) / RF
streamId            = (indexOrdinal, partition)
```
≈ **50–100 ns**, and **independent of tenant count** — a hash over ~12 bytes plus
two map lookups. The routing value is now **discarded**: it is not stored, not in
the segment, not in the key.

⚠️ If the shard count is not yet known (index registered a moment ago), the record
goes to a bounded FIFO **pending pool** and is partitioned when registration
arrives — legal because a buffered record has no offset yet
([ADR-0015](../../internal/product/decisions/0015-routing-registration-and-aliases.md) §3).

### 2.3 Accumulate — one accumulator per stream, not per tenant
Appended to the accumulator for `(indexOrdinal, partition)`. **5M tenants collapse
into ~120,000 accumulators**, because a tenant is not a stream.

**Lane is consumed here too.** It sets the accumulator's flush urgency and is
stored only as a per-run `maxLane` packed into the directory entry's existing
`codecAndFlags` word — **zero extra bytes, and no multiplication of runs by lane.**

### 2.4 Flush — two independent triggers
- **Run joins a segment** when `streamBytes ≥ 64 KiB` **or** `streamAge ≥ maxStreamLatency(lane)`.
- **Segment is cut** every 250 ms or at 8 MiB.

### 2.5 Build — sort, index, filter, name
Runs sorted by `(indexOrdinal, partition)` so each consumer's data is contiguous;
directory built; membership filter computed; key minted.

## 3. The object path

```
<prefix>/<clusterId>/data/2026/08/30/14/1756563012345-p3a-01J9XKQ7VD-h76800-Z8kQm…q.bseg
         └─trust domain └────time partition──┘└──ts──┘ └pod┘└─ULID──┘ └hdr len┘└─filter─┘
```

| Component | Why |
|---|---|
| `<clusterId>` | the **trust domain** — segments are never bundled across clusters ([ADR-0010](../../internal/product/decisions/0010-multi-tenancy-and-security-model.md)) |
| `2026/08/30/14/` | bounds a recovery `LIST` to a time window via `start-after` |
| `1756563012345` | millis; lexicographic ≈ chronological within the hour |
| `p3a` + ULID | uniqueness with no allocator — **this is what lets us avoid AutoMQ's `PrepareS3Object` controller** |
| `h76800` | exact header length ⇒ the recovery path reads preamble + directory in **one** GET, no guessing |
| `Z8kQm…q` | membership filter — see §4 |

## 4. What is stored where — the "bitmap" question

⚠️ **There is no bitmap of shard counts anywhere in an object.** Three different
structures answer three different questions:

| Where | What | Exact? | Read when |
|---|---|---|---|
| **Object key** | adaptive membership filter over **index ordinals** — `Z` exact bitmap, `R` run-length, `B` bloom, `A` all, `N` none | approximate | **recovery only**, from a LIST page, zero extra requests |
| **Object header** | the **stream directory**: one 48-byte fixed-width entry per run — `indexOrdinal, partition, recordCount, byteStart, byteLen, minTs, flags+maxLane` | **exact** | recovery, compaction, GC |
| **Commit log** | `offset → (segmentKey, byteRange)` per stream | **exact** | the authority for ordering |
| **Registry** | `indexUUID → ordinal, S, RNS, RF, aliases` | exact | resolution; pushed by the plugin |

The filter is over **indices, not tenants and not shards** — 10,000 ordinals, not
5,000,000 routing values. Tenant cardinality never enters any of these structures.

On the hot path **none of them is read**: the ingester pushes exact byte ranges.

## 5. How the plugin gets the data

```
sequencer commits -> push event to the pod holding the subscriber -> plugin
   { index, partition, firstOffset, recordCount,
     segment: "…-h76800-Z8kQm…q.bseg", byteStart: 184320, byteLen: 65536,
     via: "inline" | "proxy" | "direct" }
```

- `inline` — bytes ride in the event. Common for trickle indices (<19.5 KiB, so
  cheap even cross-AZ). **Plugin issues nothing.**
- `proxy` — plugin asks its **same-AZ** pod, which serves from RAM (it wrote the
  segment, or prefetched it on durability). **Plugin touches no object store.**
- `direct` — signed URL, only for catch-up with fan-out 1 or a pod shedding load.

The subscription already carries this node's `(index, partition)` set, so nothing
is filtered client-side and no shard ever receives another shard's bytes.

## 6. The numbers

### Writes
```
data segments = max(bytes/s ÷ segmentSize , pods ÷ flushInterval)
              = max(100 MiB/s ÷ 8 MiB      , 6 ÷ 0.25 s)
              = max(12.5 , 24.0)  = 24.0/s     -> 12.5/s with the adaptive interval
commit deltas = 1 ÷ commitBatchInterval = 1 ÷ 0.25 = 4.0/s        (S=1, ADR-0007)
```
**16.5 – 28 PUT/s → $214 – 363/month.**

### Reads
```
prefetch = segments/s × (AZ − 1) = 12.5 × 2 = 25/s
```
The **writing AZ needs no GET** — the pod that wrote the segment still holds it.
**25 GET/s → $26/month.** The plugin issues **zero**.

### HEAD and LIST
- **HEAD: 0** on the hot path. Used only for CAS redrive after a `412`, i.e. lease
  contention — a handful per failover.
- **LIST: 0.** Recovery and GC only (cost rule R2).

### Independence from tenant count

| Tenants | PUT/s | GET/s |
|---|---|---|
| 1 | 16.5 | 25.0 |
| 1,000 | 16.5 | 25.0 |
| 1,000,000 | 16.5 | 25.0 |
| 10,000,000 | 16.5 | 25.0 |

**Tenant count does not appear in either formula.** It is consumed by a hash at
admission and never reaches a key, a directory, a commit record or a request.

### Per record

| | |
|---|---|
| PUT per record | 1.65 × 10⁻⁴ — **one PUT per ~6,100 records** |
| GET per record | 2.50 × 10⁻⁴ — **one GET per ~4,000 records** |
| Cost per 1M records | **$0.0009** |
| **Cost per TiB ingested** | **$0.97** |

### The alternative this avoids

If an object were written per tenant:

| Frequency | PUT/s | Cost |
|---|---|---|
| per flush interval (250 ms) | 20,000,000 | **$259,200,000/mo** |
| per minute | 83,333 | $1,080,000/mo |
| per hour | 1,389 | $18,000/mo |

Even at one object per tenant **per hour** — useless for streaming — the bill is
50× the bundled design's, and the data would be an hour stale. This is cost rule
R1 in its most extreme form.

## 7. Why the tenant count vanishes

Three properties, each from a decision made earlier:

1. **The routing value is consumed at admission** and never enters a key,
   directory or commit record (ADR-0015).
2. **Streams are `(index, partition)`**, bounded by shard count — millions of
   routing values reduce to ~120,000 streams.
3. **Request rates scale with segments, AZs and nodes** — never with records,
   tenants, shards or indices (cost rule R5/R6/R10).

⚠️ The one thing tenant cardinality *does* affect is **skew**: a dominant tenant
makes its partition hot (7× the even share at 40% of an index's traffic). That is
handled by spreading a tenant over `k` routing values and by the **per-stream
share cap** ([ADR-0010](../../internal/product/decisions/0010-multi-tenancy-and-security-model.md)),
not by anything in the bundling arithmetic.

## 8. The low-throughput regime — where the real workload lives

⚠️ §1–7 modelled 100 MiB/s. The stated normal load is **60,000 records/minute
≈ 1,000 rec/s ≈ 1 MiB/s** — a hundred times lower, and the cost structure inverts.

### The failure the high-throughput model hides

With every pod writing its own segment, the PUT rate is `pods ÷ interval`,
**independent of throughput**. At 1 MiB/s that means tiny objects:

| Writers | Interval | Segment | PUT/s | Cost |
|---|---|---|---|---|
| 6 (every pod) | 250 ms | **41.7 KiB** | 24.0 | **$311/mo** |
| 6 | 5 s | 833 KiB | 1.2 | $15.55/mo |
| **3 (one per AZ)** | **2 s** | **667 KiB** | **1.5** | **$19.44/mo** |
| 3 | 5 s | 1.6 MiB | 0.6 | $7.78/mo |

$311/month to move 2.5 TB is **$0.12/GB** — worse than the network bill this
project replaces. Fixed by [ADR-0016](../../internal/product/decisions/0016-designated-writer-per-az.md):
**one designated writer per AZ** (forwarding is intra-AZ, hence free) plus an
**adaptive interval** targeting segment size.

Consolidating further to one cluster-wide writer was computed and **rejected**:
forwarding ⅔ of ingest cross-AZ costs $35.39/month against a $5–26/month PUT
saving.

### ⚠️ Bundling more does **not** cost more GETs

This is the key correction to the intuition that PUT and GET pull against each
other. Because the ingester proxies reads
([ADR-0004](../../internal/product/decisions/0004-the-service-serves-reads.md)),
prefetch GETs are `segments/s × (AZ−1)` — a function of **segment count**, not
segment size:

| Interval (1 writer/AZ) | Segments/s | Prefetch GET/s | Cost |
|---|---|---|---|
| 250 ms | 12.0 | 24.0 | $24.88/mo |
| 5 s | 0.60 | 1.20 | $1.24/mo |
| 30 s | 0.10 | 0.20 | $0.21/mo |

**GET falls with PUT.** Bundling harder improves both, and the only things it
trades away are **latency** and **pod memory**.

The PUT/GET tension is real — but only in an architecture where consumers fetch
whole objects themselves. Removing that was the point of ADR-0004, and this is the
dividend.

### Reading what is inside a bundle

The mechanism the tension *would* require already exists, and costs one request:

```
key: …-h76800-….bseg
  GET Range: bytes=0-76831      -> preamble + exact fixed-width directory   (1 request)
  GET Range: <coalesced range>  -> only the runs this node wants            (1 request)
```

`h<headerLen>` is in the key precisely so the directory read needs no guessing and
no second probe. **Two requests give any reader exact contents plus its own bytes,
regardless of how much the object was bundled.**

### Run shape at 1,000 rec/s

Most streams are idle in any window — 120,000 streams and 1,000 rec/s is one
record per stream every two minutes — so runs are small whatever the interval:

| Interval | Records | Distinct streams | Directory overhead |
|---|---|---|---|
| 5 s | 5,000 | ≤5,000 | **3.5%** |
| 30 s | 30,000 | ≤30,000 | **3.5%** |

3.5% is the price of a 36-byte entry against a ~1 KiB record, and it does not
improve with a longer interval because the runs stay ~1 record. Acceptable.

⚠️ **The key filter degrades to `N`** here: thousands of distinct indices per
segment exceed what fits in a key
([bloom-in-key §4](02-partition-bloom-in-key.md)). That is fine, and it prompts a
correction to how that filter was justified — see §9.

### The re-bootstrap burst subsidises everything

Re-bootstrapping 30% of 5M tenants in lane `-1` is a large, dense, low-priority
flow. It is the **best** thing that can happen to bundling economics:

- Segments fill on **size** rather than on the timer, so the adaptive interval
  shortens toward the floor and PUT cost tracks `bytes ÷ 8 MiB` — the efficient
  regime.
- Live records **share those segments** and are therefore flushed sooner and more
  cheaply than they would be alone. Backfill volume subsidises live latency.
- Lane `-1` gets larger `minRunBytes` and a longer deadline, so its own runs are
  full and compress well, and a per-lane share cap keeps it from starving live
  traffic.

**Do not separate lanes into different segments.** Lane governs run eligibility
and deadlines, not segment membership — separating them would leave the live
segments sparse and expensive, which is exactly the $311/month failure above.

## 9. ⚠️ Correction: what the key filter is actually worth

[bloom-in-key §2](02-partition-bloom-in-key.md) justifies the filter with an
"80× cheaper recovery" argument. The ratio is right and **the absolute number is
negligible**:

| Recovery over a 6 h retention window | Requests | Cost |
|---|---|---|
| 0.6 segments/s (low traffic) → 13,000 segments | 13,000 header GETs | **$0.005** |
| 12 segments/s (high traffic) → 259,000 segments | 259,000 header GETs | **$0.10** |

**The filter's real value is recovery *time* and request volume — turning 259,000
round trips into 259 LIST pages — not dollars.** Keep it, size it cheaply, and do
not distort the object format to preserve it. Its degradation to `N` at low
throughput costs a slower recovery and essentially no money.

## 10. How many objects, and what actually drives the count

```
segments/s = max( totalBytes ÷ segmentSize , writers ÷ interval )
deltas/s   = min( commitArrivalRate , 1 ÷ minCommitInterval )
objects/s  = segments/s + deltas/s
```

**Every pod writes** ([ADR-0017](../../internal/product/decisions/0017-every-pod-writes.md)) —
so `writers = pods`, there is no forwarding, and no pod is a chokepoint. The
**adaptive interval** is the dial and it needs no coordination: each pod measures
`fillRatio` on its own flushes and adjusts its own interval.

⚠️ An earlier version consolidated writing to one pod per AZ. Decomposition showed
the interval delivered **95%** of that saving and consolidation only $7.78/month —
not worth a hop, a lease and a ramp-time chokepoint. Fleet size is a Kubernetes
concern: an operator scales pods on **ingest bytes/s per pod**, floored at 2 per AZ.

| Load | Segments/s | Deltas/s | Objects/s | Cost | Objects/day | Live at 6 h |
|---|---|---|---|---|---|---|
| normal, 1 MiB/s, 5 s | 0.60 | 0.60 | **1.20** | **$15.55/mo** | 103,680 | 25,920 |
| backfill, 50 MiB/s | 6.25 | 4.00 | 10.25 | $132.84/mo | 885,600 | 221,400 |
| high, 100 MiB/s | 12.50 | 4.00 | 16.50 | $213.84/mo | 1,425,600 | 356,400 |

### What does **not** change the object count

| | |
|---|---|
| 10,000 indices vs 1 | 0.60/s — unchanged |
| 2,000 shards vs 6 | 0.60/s — unchanged |
| 5,000,000 tenants vs 1 | 0.60/s — unchanged |
| Any routing scheme | 0.60/s — unchanged |
| **Nodes per AZ (2, 4, 10…)** | **0.60/s — unchanged**, because only writers write |

All of them are *runs inside* a segment, or are consumed at admission. This is
cost rules R1/R5/R6 holding.

### ⚠️ What **does**: the lane deadline sets the flush cadence

Lanes share segments ([§8](#the-re-bootstrap-burst-subsidises-everything)) — but a
run's deadline can cut the segment, so **one `+1` record per window forces the
whole segment to cut on that lane's deadline**, however little data is in it:

| Lane deadline | Objects/s @ 1 MiB/s | Cost | vs 5 s baseline |
|---|---|---|---|
| 250 ms | 12.00 | $155.52/mo | **20×** |
| 1 s | 3.00 | $38.88/mo | 5× |
| 2 s | 1.50 | $19.44/mo | 2.5× |
| **5 s** | **0.60** | **$7.78/mo** | 1× |

**A lane's deadline is a per-lane cost dial, and it is the only per-request
control that moves the object count.** Expose it that way: "250 ms priority costs
20× the PUTs of 5 s priority at this volume." Cap the high-lane flush rate so a
single stray `+1` record cannot pin the whole cluster to a 250 ms cadence.

The other multiplier is **trust domains**: *n* OpenSearch clusters means *n*
independent segment streams ([ADR-0010](../../internal/product/decisions/0010-multi-tenancy-and-security-model.md)),
so *n* × the table above.
