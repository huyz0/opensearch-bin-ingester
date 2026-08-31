# Membership filters in the object key

**Status:** proposal, **revised 2026-08-29 for the real target scale** (10,000 indices /
~120,000 shards). The earlier version assumed a 1,600-stream universe and reached a different
conclusion. · **Confidence:** high (closed-form maths, computed below) · **Last updated:** 2026-08-29

**Read this if:** you are implementing key generation, the recovery path, or GC.
**One-line takeaway:** at 10,000 indices the filter's feasibility is decided entirely by **what
scopes its universe**. An exact bitmap is excellent for one mega-index's 2,000 shards (334 chars)
and impossible for the whole cluster's 120,000 streams (19,984 chars). The workable design is a
**two-tier filter: an index-level Bloom in the key, and the exact stream directory in the header.**

---

## 0. Target scale (the input that drives everything below)

| | |
|---|---|
| Indices | **10,000** |
| ~10 mega indices | up to **2,000 shards** each ⇒ 20,000 partitions |
| ~9,990 ordinary indices | 6–15 shards, ~10 avg ⇒ ~99,900 partitions |
| **Stream universe U** | **≈ 120,000** |
| OpenSearch data nodes | ~200–600 (at 200–600 shards/node) |

Assume a base64url encoding (6 bits/char) and a ~900-character budget for the filter component of
the key (1,024-byte cap minus ~90 bytes of fixed key structure) ⇒ **5,400 bits**.

## 1. What the filter is for (unchanged, and now more important)

**For:** answering "might this segment contain data for X?" from a `LIST` result alone, with zero
extra requests — on the **disaster-recovery, GC and compaction-planning paths only**.

**Not for:** the hot path. A subscribed consumer receives the exact `(key, byteRange)` from the
service ([discovery-and-tailing](04-discovery-and-tailing.md)) and never looks at the key.

Because a false positive costs **one header GET** (which then answers exactly, thanks to
`h<headerLen>` in the key — [object-layout](01-object-layout-and-format.md) §5), **an FPR of 5–20%
is entirely acceptable.** Prefer a short key over a low FPR. This tolerance is what makes the
design below viable at all.

## 2. Why it is still worth doing — the 80× argument

Recovery over 1,000 segments:

| Approach | Requests | Cost |
|---|---|---|
| LIST 1,000 keys, filter in memory, GET only matches | 1 LIST (+ matches) | $5.0 × 10⁻⁶ |
| LIST 1,000 keys, then GET each header to check | 1 LIST + 1,000 GET | $4.05 × 10⁻⁴ |

**~80× cheaper** and 1,000 fewer round-trips. Unchanged by scale.

## 3. The maths

Bloom with *m* bits, *n* items, optimal *k = (m/n)·ln2*: `m/n = −1.4427·log2(p)`.

| Target FPR | bits/item | k | max n in 5,400 bits |
|---|---|---|---|
| 1%  | 9.59 | 7 | **563** |
| 5%  | 6.24 | 4 | **866** |
| 10% | 4.79 | 3 | **1,126** |
| 20% | 3.35 | 2 | **1,612** |

Exact bitmap over a dense universe *U*: **U bits**, zero false positives ⇒ max **U = 5,400**.

## 4. The scale verdict — three different universes, three different answers

Everything depends on what the key prefix has already scoped the universe to:

| Universe | Size | Exact bitmap | Best Bloom | Verdict |
|---|---|---|---|---|
| **One mega index's shards** | 2,000 | **334 chars, 0% FPR** ✅ | 3,196 chars @1% ✗ | **bitmap wins outright** — 10× smaller *and* exact |
| **All 10,000 indices** (index-level) | 10,000 | 1,667 chars ✗ | 208–799 chars ✅ (n=200–1,000 distinct indices/segment) | **Bloom** |
| **All ~120,000 streams** (stream-level) | 119,900 | **19,984 chars ✗ (22× over)** | 3,994 chars @10% for n=5,000 ✗ | **neither fits — abandon stream-level filtering in the key** |

Bloom size by distinct items per segment, for reference:

| n per segment | p=1% | p=5% | p=10% |
|---|---|---|---|
| 200 | 320 ✅ | 208 ✅ | 160 ✅ |
| 500 | 799 ✅ | 520 ✅ | 400 ✅ |
| 1,000 | 1,598 ✗ | 1,040 ✗ | 799 ✅ |
| 2,000 | 3,196 ✗ | 2,079 ✗ | 1,598 ✗ |
| 5,000 | 7,988 ✗ | 5,197 ✗ | 3,994 ✗ |

### Direct answer to "2,000 shards — is an exact bitmap still good?"
**Yes — but only once the universe is scoped to that one index.** 2,000 bits = 334 base64url
characters, exact, and with the near-100% occupancy a busy 2,000-shard index will have, the RLE
encoding collapses it to a handful of characters or the single-character `A` ("all"). A Bloom filter
over the same 2,000 shards would need 3,196 characters at 1% FPR and still produce false positives.

**Scoped to the whole cluster it is hopeless:** 119,900 bits ≈ 19,984 characters, ~22× the entire
key length limit.

> **The general rule this establishes:** *the prefix sets the universe, and the universe decides the
> encoding.* Prefix design and filter design are one decision, not two.

## 5. The recommended design: two-tier

**Tier 1 — in the key: an *index-level* Bloom.** Answers "might this segment contain data for
index *I*?" Sized for the number of distinct **indices** in a segment (hundreds), not streams
(thousands).

**Tier 2 — in the header: the exact stream directory.** One `GET bytes=0..32+headerLen-1`
(the length is in the key, so no guessing) gives the exact `(index, partition) → byteRange` map.

```
recovery: LIST page (1 req / 1000 keys)
            -> key Bloom says index I might be present     [free]
            -> GET header                                   [1 req, exact answer]
            -> GET coalesced data range                     [1 req]
```

A tier-1 false positive costs one header GET. At 10% FPR that is a ~10% overhead on a path that
runs approximately never. **This is the right place to spend the error budget.**

### The feasibility condition, and how the flush policy secures it
Tier 1 needs **distinct indices per segment ≲ 1,000**. With 9,990 low-traffic indices all
trickling, a naive 250 ms flush could touch far more than that.

**The per-stream trickle policy fixes this and pays for itself twice**
(see [object-layout](01-object-layout-and-format.md) §7b): a stream's buffer is flushed when it
reaches a byte threshold **or** its age exceeds `maxStreamLatency` (default ~5 s), rather than on
every segment flush. With 5 s accumulation and 250 ms segments, roughly **1/20** of trickle streams
appear in any given segment ⇒ ~500 distinct indices/segment ⇒ **520 chars at 5% FPR** ✅.

The same policy independently removes the pathological "8,192 runs of one record each" segment that
would otherwise wreck compression and inflate the directory. **Two problems, one mechanism** —
which is a good sign the mechanism is right.

## 6. Prefix scoping: tempting, and mostly not worth it

A prefix turns LIST from a scan into a seek, and scopes the filter universe. But **each prefix needs
its own object stream, so PUT cost multiplies by the number of prefixes**:

```
PUT/s = (prefixes) x (pods) x (flush rate)      => ~$156/month per prefix at 3 pods / 250 ms
```

- 64 hash buckets for the small-index tail: **768 PUT/s ≈ $10,000/month.** Immediately disqualified.
- A dedicated prefix per mega index (10): **$1,560/month** at 250 ms, or **$390/month** at a 1 s
  flush for those indices.

Is that worth it? The benefits are (a) LIST-as-seek on a path that never runs, (b) an exact bitmap
filter instead of a Bloom, and (c) marginally better read locality. At ~300 data nodes each node
needs most segments anyway, so (c) is small
([az-topology](05-az-topology-and-data-flow.md) §5).

**Recommendation: a single shared data prefix.** Give a mega index its own prefix only if
measurement shows read amplification or recovery time actually hurts — and if you do, that is
exactly the case where the **exact 334-char bitmap** becomes available, which is the one genuine
upside.

## 7. Proposed encoding: an adaptive tagged filter (unchanged, now scoped to indices)

```
<filter> ::= <tag><payload>

  A                       every registered index in scope is present   (1 char)
  N                       no filter; reader must read the header
  Z<base64url bitmap>     exact bitmap over a scoped ordinal space (mega-index prefix case)
  R<base64url RLE>        run-length encoded bitmap (dense with gaps)
  B<k><base64url bits>    bloom over index ordinals (the default at this scale)
```

The writer computes the candidates and emits the shortest that fits, degrading to `N` if none do.
`N` must be handled correctly by readers — at 10,000 indices it will occasionally be the honest
answer, and a reader that treats `N` as "no data here" would silently lose records.

**Bloom implementation:** one 128-bit hash (xxh3-128 / murmur3-128) split into `h1,h2`, then
Kirsch–Mitzenmacher `h_i = h1 + i·h2 (mod m)`. Benchmark against a blocked Bloom —
[benchmarking-plan](../40-implementation/03-benchmarking-plan.md) §3 (B6).

**Index ordinals:** tier 1 filters on `indexOrdinal`, so the ordinal registry
([object-layout](01-object-layout-and-format.md) §6) is now **required**, not optional — 10,000
16-byte UUIDs would be a 160 KB directory per segment otherwise. A 2-byte ordinal covers 65,536
indices.

## 8. Testing requirements

- Property test: **no false negatives, ever** — a false negative silently drops data.
- Measured FPR within tolerance of the analytic prediction, at n = 100/500/1,000 indices.
- Key length never exceeds 1,024 bytes for **any** input, including a segment touching all 10,000
  indices (which must degrade to `N`, not overflow).
- Readers treat an unknown tag and `N` as "must read the header", never as "no match".
- Every emitted key round-trips through S3, GCS, Azure and local FS.
- Golden-file tests per tag so key formats stay stable across versions.
