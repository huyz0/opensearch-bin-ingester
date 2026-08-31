# Benchmarking plan

**Status:** proposal · **Confidence:** high (methodology), medium (which micro-benchmarks will
actually matter — that is what the profiling tier is for) · **Last updated:** 2026-08-29

**Read this if:** you are about to optimise something, or you want to know what "measured" means
on this project.
**One-line takeaway:** three tiers — JMH micro, macro throughput/latency harness, and a
**cost benchmark** that counts object-store requests. The third is unusual and is the one most
tied to the project's goal: a cost regression must fail CI, not an invoice.

---

## 0. Method (non-negotiable)

- **Profile before optimising.** Build the macro harness first, find the top 3 hot spots with JFR /
  async-profiler, and *then* write micro-benchmarks for those. Micro-benchmarking on a hunch is how
  you spend a week making a 0.3% path 40% faster.
- **JMH rules:** `@Fork(≥2)`, warmup ≥5 iterations, consume results with `Blackhole` (never rely on
  a return value to defeat DCE), `@State(Scope.Benchmark)` for shared immutable inputs,
  `-prof gc` **always** — allocation rate (`gc.alloc.rate.norm`, B/op) is a first-class result here,
  not a footnote. A change that is 5% faster and allocates 2× more is a regression.
- **Realistic inputs.** Real log/JSON documents of realistic size distribution (long tail matters:
  200 B API logs and 50 KB stack traces behave very differently), realistic partition counts,
  realistic key cardinality.
- **Report distributions, not means.** Object-store latency is fat-tailed; p99/p99.9 is the number
  that determines the flush interval.
- **Pin the environment**: fixed JDK build, `-XX:+AlwaysPreTouch`, CPU governor fixed, no
  co-tenancy. Record all of it with the results.

## 1. Tier 1 — JMH micro-benchmarks

Candidates, in the order they are likely to matter:

| # | Benchmark | Compares | Metric |
|---|---|---|---|
| B1 | Frame decode | length-prefixed vs NDJSON scan vs streaming-JSON field extraction | ns/record, B/op |
| B2 | Buffer strategy | heap `byte[]` vs direct `ByteBuffer` vs `MemorySegment`; pooled vs allocated | ns/copy, B/op |
| B3 | Compression | zstd 1/3/6, lz4, none, on real documents, at 64 KiB / 256 KiB / 1 MiB blocks | MiB/s, ratio, MiB/s per core |
| B4 | Checksum | `java.util.zip.CRC32C` vs xxh3 vs none | GiB/s |
| B5 | Hashing | xxh3-128 vs murmur3-128 vs `Objects.hash` for stream IDs and routing keys | ns/op |
| B6 | Membership filter | bloom (double-hash) vs blocked bloom vs exact bitmap vs RLE bitmap — build **and** query | ns/op, bits/item, measured FPR |
| B7 | Directory encode/decode | fixed-width entries + binary search vs delta-encoded scan | ns/lookup at 100/1k/20k runs |
| B8 | Segment assembly | scatter-gather vs single-buffer copy vs pipe | GiB/s, B/op |
| B9 | Base64url encoding | JDK `Base64` vs hand-rolled, for key filters | ns/op |
| B10 | Per-stream accumulator | striped locks vs per-stream ownership vs `ConcurrentHashMap` merge, at 16/256/4096 streams | ns/record under contention |
| B11 | Consumer decode | frame decode + `byte[]` materialisation for the plugin | ns/record, B/op |
| B12 | Varint | uvarint vs fixed u32 length prefixes | ns/op, bytes saved |

**Do not write all twelve up front.** B1, B3, B6 and B10 are near-certain to matter; the rest wait
for the profiler.

## 2. Tier 2 — macro harness

A load generator plus the ingester plus a store backend, measuring the whole pipeline.

| Dimension | Values |
|---|---|
| Store | `MemoryBinStore`, `LocalFsBinStore`, MinIO (Testcontainers), real S3 |
| Ingest rate | 1, 10, 50, 100, 500 MiB/s |
| Streams | 16, 256, 1,600, 20,000 |
| Doc size | 200 B, 1 KiB, 10 KiB, mixed realistic |
| Flush interval | 100 ms, 250 ms, 1 s, 5 s |

Outputs: throughput per core, end-to-end latency histogram (producer ack → OpenSearch searchable),
heap/direct usage over time, GC pause distribution, **and the tier-3 cost counters**.

The key deliverable from tier 2 is the **cost/latency curve** — cost per TiB versus p99 latency as
the flush interval varies. That single chart is what an operator uses to pick their dial setting,
and it is the most useful artifact this project can publish.

## 3. Tier 3 — the cost benchmark (the distinctive one)

Using `CountingBinStore` ([store SPI](../30-design-space/07-pluggable-store-abstraction.md) §5),
assert hard budgets in CI:

```java
@Test void writePathStaysUnderRequestBudget() {
    var stats = run(Workload.scenarioA(), Duration.ofMinutes(5));
    assertThat(stats.requestsPerMiBWritten()).isLessThan(0.30);
    assertThat(stats.listRequests()).isZero();                 // R2: no LIST on a hot path
}

@Test void idleClusterCostsNothing() {
    var stats = runIdle(Duration.ofMinutes(5), /*shards=*/1600);
    assertThat(stats.totalRequests()).isZero();                // R3
}

@Test void readPathScalesWithObjectsNotShards() {
    var few  = runConsumer(/*shards=*/16);
    var many = runConsumer(/*shards=*/1600);
    assertThat(many.getRequests()).isLessThan(few.getRequests() * 1.2);   // R5
}
```

These encode the design rules from [cost-model](../00-problem/02-cost-model.md) §6 as executable
assertions. **The idle test is the most valuable test in the project** — it is the one failure that
would otherwise be invisible in every functional test and catastrophic in production.

Also emit `estimated_usd_per_tib_ingested` per run and track it over time. A trend line beats a
threshold for catching slow drift.

## 4. Specific decisions to settle by measurement

| Question | Benchmark | Where it comes from |
|---|---|---|
| Sync AWS SDK + virtual threads vs async/CRT | tier 2 at 100/500 MiB/s | [store SPI](../30-design-space/07-pluggable-store-abstraction.md) §6 |
| Hand-rolled S3 REST vs SDK for the plugin's ranged GET | B-series + tier 2 | [plugin-packaging](../20-opensearch/03-plugin-packaging.md) §2 |
| Optimal block size and codec | B3 + tier 2 (ratio affects storage cost too) | [object-layout](../30-design-space/01-object-layout-and-format.md) §4 |
| Bloom vs bitmap crossover in practice | B6 with real occupancy traces | [bloom-in-key](../30-design-space/02-partition-bloom-in-key.md) §4 |
| Cache compressed vs decompressed blocks | tier 2, consumer-side | [streaming-io](02-streaming-io-and-memory.md) §6 |
| Directory: fixed-width vs delta-encoded | B7 | [object-layout](../30-design-space/01-object-layout-and-format.md) §9 |
| Flush interval default | tier 2 cost/latency curve | [object-layout](../30-design-space/01-object-layout-and-format.md) §7 |

## 5. Correctness benchmarks (the ones that are not about speed)

- **Deterministic simulation of the commit protocol** — injectable clock, faulty store, partitioned
  leaders, duplicated in-flight PUTs. Assert invariants I1–I4 from
  [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §7. Run thousands of randomised
  seeds in CI; store failing seeds as regression tests.
- **Store conformance suite** across all backends
  ([store SPI](../30-design-space/07-pluggable-store-abstraction.md) §3).
- **Memory flatness soak** — fixed small heap, 10× request sizes, unchanged throughput
  ([streaming-io](02-streaming-io-and-memory.md) §7).
- **End-to-end via OpenSearch** — `internalClusterTest` with the local-FS store: index documents
  through the ingester, assert they are searchable, kill and restart a node, assert no loss and no
  duplicate beyond at-least-once expectations.

## 6. Anti-patterns to avoid

- Benchmarking against `MemoryBinStore` only, then being surprised by S3 tail latency.
- Optimising the record path while the segment assembly copies everything twice (profile first).
- Reporting throughput without allocation rate.
- A "1 million docs/sec" number on 100-byte documents with one partition, which measures nothing
  anyone will run.
- Treating the cost counters as diagnostics rather than assertions.
