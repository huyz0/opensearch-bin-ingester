# Object layout and binary format

**Status:** proposal · **Confidence:** medium-high (format borrows a proven design; the
offset-agnostic property is the load-bearing idea and should be reviewed carefully) ·
**Last updated:** 2026-08-29

**Read this if:** you are implementing the writer, the reader, or the compactor.
**One-line takeaway:** one object per pod per flush, carrying many `(index, partition)` streams,
sorted so each consumer's data is contiguous, **containing no absolute offsets** — offsets are
assigned later by the sequencer, which is what makes parallel leaderless writes possible.

---

## 1. Vocabulary

| Term | Meaning |
|---|---|
| **Record** | one document payload, opaque bytes (see [SPI](../20-opensearch/01-pull-based-ingestion-spi.md) §2) |
| **Stream** | the ordered record sequence for one `(indexUUID, partitionId)` |
| **Run** | the contiguous chunk of one stream's records inside one object |
| **Segment** | one object written by one pod at one flush; contains many runs |
| **Offset** | the logical `long` position of a record within its stream; assigned by the sequencer |

## 2. The load-bearing decision: segments contain no absolute offsets

A segment records, per run, only a **record count** and the byte layout. The mapping

```
(stream, [startOffset, startOffset+count))  ->  (segmentKey, byteRange)
```

lives in the **commit log**, not in the segment. Consequences:

- **Any pod can write any partition's bytes at any time with zero coordination.** Ordering is
  decided afterwards, at commit — the WarpStream/KIP-1150 property
  ([warpstream.md](../10-prior-art/01-warpstream.md) §2).
- A failed or abandoned PUT leaves no hole in the log; it is simply never committed and later
  garbage-collected. Pre-assigning offset ranges before the PUT would create holes that stall
  every consumer of that partition — **do not do that**.
- Segments are immutable and self-contained, so compaction can rewrite them freely as long as the
  commit log is updated to point at the new locations.

## 3. Segment layout

Header at the **front** (the brief calls for this, and it makes the recovery path a forward scan);
a footer duplicate of the directory offsets guards against truncation.

```
+=====================================================================+
| PREAMBLE (fixed 32 B)                                               |
|   u32 magic 'BSEG'    u16 formatVersion   u16 flags                 |
|   u32 headerLen       u32 headerCrc32c                              |
|   u64 createdAtMillis u32 runCount                                  |
+=====================================================================+
| HEADER / STREAM DIRECTORY  (headerLen bytes, fixed-width entries)   |
|   repeated runCount x RunEntry (48 B, sorted by (indexId,partition))|
|     u128 indexId (UUID)     -- 16 B                                 |
|     u32  partitionId        --  4 B                                 |
|     u32  recordCount        --  4 B                                 |
|     u64  byteStart          --  8 B   (absolute in this segment)    |
|     u32  byteLen            --  4 B                                 |
|     u64  minTimestampMillis --  8 B                                 |
|     u32  codecAndFlags      --  4 B   (codec, crc present, ...)     |
+=====================================================================+
| DATA                                                                |
|   run 0: block(s)   block = [u32 uncompressedLen][u32 crc32c][bytes]|
|            bytes = repeated [uvarint recordLen][record payload]     |
|   run 1: ...                                                        |
+=====================================================================+
| FOOTER (24 B): u64 headerStart | u32 headerLen | u64 MAGIC | u32 crc|
+=====================================================================+
```

⚠️ **REVISED 2026-09-02 (M3; ADR-0025).** The `RunEntry` above is
`formatVersion` 0. `formatVersion` 1 appends one more field, `i8 lane` (1 B,
always `0` until M10 wires a real value — see [ADR-0014](../../internal/product/decisions/0014-priority-lanes.md)
for what the field will eventually mean) — 49 B per entry, not 48. A reader
accepts BOTH versions; a v0 segment an earlier build already wrote has no
lane byte at all and is never rewritten. This diagram predates the field and
is left describing v0's shape as originally designed; the current wire
format lives in `format.SegmentFormat`'s own javadoc, which is the
authoritative source once code exists (this doc is upstream of it, not a
substitute for it).

Design notes:

- **Fixed-width directory entries** ⇒ binary search over the header with no parsing, exactly like
  AutoMQ's `DataBlockIndex` (36 B entries, `IndexBlockOrderedBytes`). Keep this property.
- 48 B/run × 1,600 runs = **76.8 KiB** of header on an 8 MiB segment (0.9% overhead). Acceptable.
  If it becomes a problem, replace the 16-byte UUID with a 4-byte index ordinal from the registry
  (§6) and the entry shrinks to 36 B.
- **Sorted by `(indexId, partitionId)`** so that one consumer's runs are contiguous ⇒ its fetch is
  one coalesced range instead of hundreds of range GETs. This directly implements cost rule R4/R5.
- `minTimestampMillis` per run makes `pointerFromTimestampMillis()` answerable from metadata alone.
- Records are **length-prefixed with uvarint** and never parsed by the ingester.

## 4. Compression and checksums

- Compress **per block**, not per record: per-record compression destroys the ratio on small JSON
  documents. Blocks of ~256 KiB–1 MiB (AutoMQ uses `objectBlockSize = 1 MiB`).
- **Blocks must not span runs.** A consumer must be able to decompress only its own runs. A small
  run gets a small block; that is the price of independent decodability.
- Codec is per-run in `codecAndFlags`: `none | lz4 | zstd(level)`. Benchmark, do not guess — see
  [benchmarking-plan](../40-implementation/03-benchmarking-plan.md) §3. Expect zstd-1..3 to be the
  sweet spot for JSON, with lz4 the choice if the ingester becomes CPU-bound.
- **CRC32C** (`java.util.zip.CRC32C`, hardware-accelerated) per block, plus a header CRC. Object
  stores already guarantee integrity in transit and at rest; these checksums exist to catch *our*
  bugs — framing errors, partial writes, bad compaction — which they will.

## 5. The object key: self-describing, so reads cost one request

We mint the key, so we can put the things a reader needs *before* it reads anything:

```
<prefix>/data/<yyyy>/<MM>/<dd>/<HH>/<ts>-<podShortId>-<ulid>-h<headerLen>-<filter>.bseg
```

| Component | Purpose |
|---|---|
| `<yyyy>/<MM>/<dd>/<HH>/` | bounds a recovery LIST to a time window via `start-after` |
| `<ts>` (millis, zero-padded) | lexicographic ≈ chronological within the hour |
| `<podShortId>` + `<ulid>` | uniqueness without coordination; ULID is monotonic per pod |
| `h<headerLen>` | **the reader knows the exact header length before its first request** |
| `<filter>` | partition/index membership filter — see [partition-bloom-in-key](02-partition-bloom-in-key.md) |

> **`h<headerLen>` is a strict improvement on AutoMQ's speculative tail read**
> ([automq.md](../10-prior-art/02-automq.md) §4). AutoMQ must *guess* the index size
> (`8192 + size/1MiB*36`) and occasionally pay a second GET. We simply write the number in the key:
> one `GET Range: bytes=0-<32+headerLen-1>` retrieves preamble **and** directory, always, with no
> guessing and no retry. Costs nothing, removes a whole failure mode.

Key length budget: S3/GCS/Azure all cap object names at **1024 bytes**. The fixed part above is
~90 bytes, leaving ~900 for the filter — see [02](02-partition-bloom-in-key.md) §5 for what fits.

## 6. Index identity: UUID vs ordinal

Two representations, both needed:

- **`indexUUID`** — authoritative, from `IndexMetadata`. Survives index recreation with the same name.
- **`indexOrdinal`** — a small dense `int` assigned by a registry object
  (`<prefix>/ctl/registry/indices.json`, updated with CAS). Used where size matters: key filters,
  compact directory entries, commit-log delta encoding.

The registry is tiny, changes only when an index is created, and is cacheable indefinitely with a
single `If-None-Match` conditional GET to revalidate. **Open question:** whether the ordinal
registry is worth the extra CAS'd object versus hashing the UUID into a fixed bucket space
(see [02](02-partition-bloom-in-key.md) §4 — hashing avoids the registry at the cost of collisions).

## 7. Sizing

| Parameter | Default | Rationale |
|---|---|---|
| Flush interval | **250 ms** | WarpStream and AutoMQ independently converged here |
| Flush size | **8 MiB** | ditto (`ObjectWALConfig.maxBytesInBatch`) |
| Max unflushed bytes (backpressure) | **1 GiB** | ditto (`maxUnflushedBytes`) |
| Block size | 256 KiB–1 MiB | compression ratio vs independent decode |
| Multipart threshold / part size | 16 MiB | `Config.objectPartSize`; S3 min part is 5 MiB |
| Max runs per segment | 20,000 | AutoMQ's `maxStreamNumPerStreamSetObject` |
| `minRunBytes` | 64 KiB | see §7b — do not emit single-record runs |
| `maxStreamLatency` | 5 s | see §7b — the latency SLO for low-traffic indices |

For a *low-traffic* deployment the size trigger never fires and the interval dominates: 4 PUT/s per
pod ≈ $52/month/pod regardless of throughput. **A slow-traffic tenant should raise the interval**
(1–5 s), which is the primary latency-for-cost dial exposed to operators.

## 7b. Per-stream flush policy — required at 10,000 indices

A naive design flushes *every* buffered stream on every segment flush. At the target scale
(10,000 indices, ~120,000 streams, most of them trickling a few records per second) that produces a
segment full of **single-record runs**, which is bad in four independent ways:

1. **Directory bloat** — 8,192 runs × 48 B = 393 KiB of header on an 8 MiB segment (4.8%).
2. **Compression collapse** — a block per run, each holding one document, so there is nothing to
   compress against.
3. **Commit-log bloat** — one index entry per run, per segment, forever.
4. **The key filter becomes impossible** — thousands of distinct indices per segment cannot be
   described in 900 characters ([partition-bloom-in-key](02-partition-bloom-in-key.md) §5).

**Policy:** a stream's buffer joins the current segment when

```
streamBytes >= minRunBytes            (default 64 KiB)
   OR streamAge >= maxStreamLatency   (default 5 s, per-index override)
   OR the pod is shutting down
```

Hot streams flush on every segment (they hit the byte threshold); trickle streams flush roughly
every `maxStreamLatency`. With 5 s accumulation and 250 ms segments, about **1/20** of the trickle
population appears in any given segment.

Consequences, all good:
- ~500 distinct indices per segment instead of thousands ⇒ the index-level Bloom **fits**.
- Runs are ≥64 KiB ⇒ compression works and the directory is ~1% of the object.
- **`maxStreamLatency` becomes the real latency SLO for low-traffic indices**, and it is the honest
  dial to expose: a trickle index sees ~5 s to searchability, a busy index sees ~250 ms. State this
  in operator documentation — it is a surprising property if undocumented.
- The segment flush interval and the stream flush latency are now **two separate knobs**. Do not
  conflate them.

**Bound the buffer:** total buffered bytes across all held-back streams must be capped and counted
against the slab pool ([streaming-io](../40-implementation/02-streaming-io-and-memory.md) §4), or
120,000 idle streams each holding a partial slab will exhaust memory. Hold trickle data in
right-sized buffers, not full slabs.


## 8. Read strategies, ranked by cost

| Situation | Strategy | Requests |
|---|---|---|
| Hot path (subscribed consumer) | service pushed `(key, byteRange)`; fetch the coalesced range directly | **1** |
| Consumer needs many runs from one segment | coalesce into one range covering min..max (bytes are free in-region) | **1** |
| Consumer has only the key (recovery) | `GET bytes=0..32+headerLen-1` using `h<headerLen>` from the key, then one coalesced data range | **2** |
| Full recovery, no service | LIST time window → filter keys by `<filter>` → the 2-request path per surviving key | 1 LIST/1000 keys + 2/segment |

**Never** issue one range GET per run. See the 175× penalty in
[cost-model](../00-problem/02-cost-model.md) §5.

## 9. Open questions

- ✅ **Directory compression: no.** It is 0.9% of a segment and 100% of the recovery-path read, but
  delta-encoding forfeits fixed-width binary search, which is what makes a lookup free. Recovery is a
  cold path; a lookup is not. Revisit only if recovery time is measured to hurt. (Q12)
- Per-run vs per-block codec selection (some indices compress far better than others).
- Should small tenants get their own segments to avoid a noisy neighbour dominating read
  amplification? Related to the sorting decision in §3.
