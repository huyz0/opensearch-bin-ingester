# Strict streaming, framing and memory discipline

**Status:** proposal · **Confidence:** medium-high · **Last updated:** 2026-08-29

**Read this if:** you are implementing the ingest handler, the wire protocol, or the buffer pool.
**One-line takeaway:** the ingester should treat document bodies as **opaque bytes it never parses**,
copied exactly once from the socket into a pooled buffer that becomes part of a segment. Routing
metadata arrives out of band in the framing, not by inspecting JSON.

---

## 1. The single-copy goal

The ideal path for a record:

```
socket -> pooled buffer (one copy) -> [compress] -> segment body -> object store
```

Anything that adds a copy or an allocation multiplies GC pressure by the ingest rate. At 100 MiB/s a
single extra copy is 100 MiB/s of memory bandwidth and, if it allocates, 100 MiB/s of garbage.

What must **not** happen:
- `req.content().as(String.class)` / `as(byte[].class)` — materialises the whole body.
- `new String(bytes, UTF_8)` anywhere on the hot path.
- Jackson databind into `Map<String,Object>` — allocates dozens of objects per document.
- Re-encoding: whatever bytes arrive should be the bytes stored.

## 2. Wire protocol: length-prefixed frames

**Recommended primary format** — a binary framed stream, consumable incrementally with no lookahead
and no parsing of the payload:

```
stream := header frame*

header := magic "BING" | u16 version | u16 flags
          | u8 indexIdLen | indexId bytes (UUID or name)
          | u32 defaultPartition

frame  := u8 type
          type=0x01 RECORD:    u32 partitionId | u32 payloadLen | payload bytes
          type=0x02 RECORD_DP: u32 payloadLen | payload bytes        (default partition)
          type=0x03 IDX_SWITCH:u8 idLen | indexId bytes              (multi-index in one request)
          type=0x7f END:       u32 frameCount | u32 crc32c
```

Why this shape:
- **`payloadLen` before the payload** ⇒ the handler reads exactly that many bytes straight into a
  pooled buffer. No scanning, no delimiter escaping, no ambiguity about embedded newlines.
- **Partition is explicit and out of band** ⇒ the ingester never opens the document (§3).
- **Multi-index in one request** ⇒ an agent shipping 50 low-volume indices does so on one
  connection, which is exactly the bundling the cost model wants.
- **Trailing CRC + count** ⇒ truncated uploads are detected without buffering.

**Also support NDJSON** (`Content-Type: application/x-ndjson`) for compatibility, with the partition
supplied by headers (`X-Bin-Index`, `X-Bin-Partition`) or a `?partition_field=` pointer. Read it as a
byte stream split on `\n` — **never** `BufferedReader.readLine()`, which decodes to `String`. Treat
NDJSON as the slow path and say so in the docs.

## 3. Never parse the payload

Parsing is the most expensive thing the ingester could do, and it is optional. Three ways a partition
can be determined, in order of preference:

| Approach | Cost | Recommendation |
|---|---|---|
| Producer sends `partitionId` in the frame | zero | **default** |
| Producer sends a routing key; service hashes it | one hash over a short string | good fallback |
| Service extracts a field from the document | full streaming JSON parse of every document | **avoid**; offer it, warn about it, benchmark it so the cost is visible |

If the third is unavoidable, use a **streaming** `JsonParser` that stops at the target field and
*tees* the raw bytes into the buffer as it goes — never build a tree, never re-serialise. Expect it
to dominate the CPU profile; that is the point of measuring it.

> Note the alignment with OpenSearch: the mappers
> ([SPI](../20-opensearch/01-pull-based-ingestion-spi.md) §2) parse the document *on the consumer
> side* anyway. Parsing it in the ingester too would be doing the same work twice, at the more
> expensive end of the pipeline.

## 4. Buffer management

- **Pooled, bounded, explicit.** A slab allocator over direct `ByteBuffer`s (or `MemorySegment`s
  from the FFM API) with acquire/release and leak detection in tests. Not `ThreadLocal`
  ([java-runtime](01-java-runtime-helidon-vthreads.md) §3).
- **Per-stream accumulators** hold a chain of slabs, not one growing array — no copy-on-grow.
- **Backpressure by blocking**: when the pool is exhausted, the ingest handler's virtual thread
  blocks on `pool.acquire()`, which stops reading the socket, which propagates TCP backpressure to
  the producer. This is the great simplification virtual threads buy — no manual flow control.
  Mirror AutoMQ's `maxUnflushedBytes = 1 GiB` ceiling.
- **Direct vs heap:** direct avoids a copy on the way to the socket/SDK; heap is cheaper to
  compress. Benchmark both (§ [benchmarking](03-benchmarking-plan.md) §2). Set
  `-XX:MaxDirectMemorySize` explicitly either way — an unset limit turns a leak into a mysterious
  container OOM kill.
- **Bound everything:** buffer pool size, per-stream accumulator cap, in-flight PUT count
  (`maxInflightUploadCount = 50` in AutoMQ), subscriber queue depth. Every unbounded queue is an
  OOM waiting for a traffic spike.

## 5. Segment assembly without a big copy

The segment is written as a **scatter list**: preamble, directory, then each run's slab chain, in
sorted order. Two ways to hand it to the store:

1. **Gathering write** — pass the buffer list to the SDK/HTTP client; content length is known
   (sum of the parts), no concatenation.
2. **Pipe** — a `PipedInputStream`-style bridge where a producer thread walks the scatter list.
   Simpler with SDKs that insist on an `InputStream`, but adds a copy and a thread handoff.

Prefer (1). Note the directory can only be written after all runs are sized, so **build the
directory last but place it first** — reserve the space, or assemble it in a separate small buffer
and emit it ahead of the data buffers in the scatter list.

Multipart upload is needed only above `partSize` (16 MiB, min 5 MiB on S3). At the 8 MiB flush
target, **single PUTs are the norm** — which is fortunate, because conditional writes on multipart
are evaluated at `CompleteMultipartUpload` and complicate the CAS protocol
([metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §2).

## 6. The consumer side (plugin)

- `getRange` returns an `InputStream`; decode frames incrementally into `Message<byte[]>` objects.
  Here a `byte[]` per record is unavoidable — the OpenSearch SPI demands it — but it should be the
  **only** allocation per record.
- **Decompress per block, lazily**, only blocks that overlap requested runs.
- The **block cache** stores compressed blocks (more entries per byte) or decompressed ones (less
  CPU). Benchmark; leaning compressed, since the same block is rarely decoded twice by one node.
- **Bound the cache** by bytes, not entries, with an LRU or S3-FIFO policy, and account it against
  the node's circuit breaker so the plugin cannot push a data node into GC death.

## 7. Memory budget to design against

| Component | Budget (per ingester node) |
|---|---|
| Slab pool (ingest buffers) | 512 MiB direct, hard cap |
| Per-stream accumulators | included in the slab pool |
| In-flight PUT buffers | ≤ 50 × 8 MiB = 400 MiB, counted in the pool |
| Commit/metadata structures | ~50 MiB |
| **Total heap + direct** | **~1.5 GiB**, flat regardless of request sizes |

Prove flatness with a soak test: fixed small heap, 10× larger request bodies, throughput unchanged
and no OOM. **A memory profile that is independent of input size is the acceptance criterion for
constraint C8** — that test is the only thing that keeps "strict streaming" from silently
regressing.
