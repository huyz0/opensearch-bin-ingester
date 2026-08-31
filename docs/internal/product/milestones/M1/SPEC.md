# M1 — Walking skeleton

**Status:** draft, awaiting review · **Decided by:** [ADR-0018](../../decisions/0018-walking-skeleton-first.md)

**Completion condition:** a document written by a producer as `_bulk` is
**searchable in a single-node OpenSearch test**, having travelled
producer → ingester → local-FS segment → consumer → plugin — and an idle cluster
issues **zero** object-store requests.

---

## Requirements

FR-1 (streamed bulk intake) · FR-2 (bundling) · FR-3 (stable offsets) ·
FR-4 (ack after durable) · FR-5 (push tail) · FR-7 (OpenSearch plugin) ·
FR-8 (store SPI) · NFR-2 (**zero idle cost**) · NFR-11 (offset stability)

## Scope

**In.** The thinnest end-to-end path, and only what proves the four assumptions
ADR-0018 names as unvalidated. **Both delivery surfaces**
([ADR-0019](../../decisions/0019-two-delivery-surfaces.md)): the in-process
`Ingest` API is built and tested first — the four risky assumptions are all on the
consumer side and none need HTTP — then the HTTP adapter wraps it.

**Out — deliberately, and each already designed elsewhere:** authentication
(M1 uses `StaticCredentialSource`; the *seam* and the trust domain are in from
day one so nothing is retrofitted through the auth path) · WAL and quorum
(ADR-0013) · lanes (ADR-0014) · key membership filter (ADR-0003; segments emit tag
`N`) · leases, epochs, seal (ADR-0002) · compaction and GC (doc 06) · retention ·
`proxy`/`direct` fetch modes (ADR-0004; M1 is `inline` only) · peer mesh (ADR-0012)
· AZ awareness · aliases and `os_routing` (ADR-0015; M1 takes an explicit
partition) · adaptive flush interval (ADR-0017; M1 uses a fixed 250 ms/8 MiB).

⚠️ **Throwaway must be labelled.** Anything in M1 that a later milestone replaces
carries `// SKELETON: replaced by M<n>` and is listed in the milestone review. The
review checks it was actually replaced, not quietly kept.

## Design

Seven of the eight modules in [architecture.md](../../architecture.md) — all
but `sequencer`, whose leases and epochs are M4; M1's minimal commit log lives
in `ingest`:

```
producer(test) ──_bulk──▶ ingester ──segment──▶ LocalFsBinStore
                             │                        ▲
                             │ commit delta ──────────┘
                             └── subscribe(inline) ──▶ consumer ──▶ plugin ──▶ Lucene
```

**What is a genuine subset rather than a stub** — these are extended later, not
rewritten:

- **Commit log.** One sequentially numbered delta per flush, written with
  `putIfAbsent`. M4 adds leases, epochs and the seal; the record format and the
  write-once discipline are already right.
- **Offsets.** A single `long` per stream, assigned at commit
  ([ADR-0001](../../decisions/0001-segments-carry-no-absolute-offsets.md)).
  Segments carry **no absolute offsets** from day one.
- **Segment format.** Preamble + fixed-width 48-byte directory + uncompressed runs
  + footer. [M2](../../roadmap.md) adds compression, the filter and the ordinal
  registry — the roadmap puts the segment format in M2; M3 is the ingest path.

**Rejected for M1:** an in-memory-only sequencer. It would not survive restart, and
NFR-11 plus assumption 2 (the pointer resumes correctly) is precisely what M1 must
prove. A minimal durable commit log is barely more work and is not throwaway.

## Cost impact

M1 establishes the meter rather than optimising against it. Rules touched:

| Rule | M1 obligation |
|---|---|
| **R3** — idle consumers issue zero requests | **The headline acceptance criterion.** Asserted, not hoped |
| R2 — no LIST on hot paths | `listRequests() == 0` asserted |
| R9 — every store op counted | `CountingBinStore` exists and is wired |
| R1, **R1b** | Bundling works, but the interval is **fixed at 250 ms**, which R1b names as the rejected operating point ($311/mo against $15.55/mo). M1 accepts that cost knowingly; the adaptive loop is [M3](../../roadmap.md) |

⚠️ Dollar figures are **modelled** in M1 — local FS has no prices. Request counts
are real and are what the gates assert.

## Acceptance criteria

Each is checkable by something other than an opinion.

-1. A lane `-1` replay carrying **older source versions** does **not** overwrite
   newer live writes for the same `_id`, with both running concurrently.
0. A `_bulk` request mixing **index and delete** actions with external versions
   round-trips: the delete removes the document, and a **replayed stale version is
   rejected rather than resurrecting it**.
1. A `_bulk` request of 100 documents to index `logs` partition 3 returns `202`
   only after the segment and its commit delta are both durable in the store.
2. A single-node OpenSearch test with `ingestion_source.type: BINSTORE` makes all
   100 documents **searchable**, with `_offset` present and monotonic.
3. **1,600 idle consumers produce `store.totalRequests() == 0` across 5 minutes of
   *injected* clock time** — the `Clock` seam is advanced, not slept on, so the T1
   test fits L0's 90 s budget. ⚠️ The window must span **at least 3,000 poll
   intervals** at the configured `pollTimeout`, or a zero proves only that nothing
   was scheduled yet.
   ⚠️ **A zero needs a positive liveness signal, not the absence of one.** Assert
   that all 1,600 subscriptions are registered at the start, and push one control
   record at the end of the window and require all 1,600 to receive it. "The
   transport delivered nothing" is *also* what 1,600 consumers that never started
   produce, so on its own it proves the opposite of what it claims —
   at **T1**, against a fake transport, in ~200 MiB. A **T4** variant with 20 real
   shards proves it holds in OpenSearch. ⚠️ Scale at the cheap tier, realism at the
   expensive one ([build.md](../../../standards/build.md)); 1,600 real shards would
   need 6+ GiB and could not run on every commit.
4. Killing and restarting the OpenSearch node resumes from the persisted
   `batch_start`, loses nothing, and duplicates only within one commit batch.
5. `readNext` returns within 10 ms of a push arriving, and blocks for the full
   `pollTimeout` when nothing arrives — proving the blocking contract.
6. One `TailSubscriber` and one cache exist **per node**, not per shard, with 100
   shards on the node.
7. Segment encode → decode → equals round-trips, and a committed golden file still
   parses.
8. A `_bulk` body of 200 MB is ingested with a **256 MB heap** and no OOM.
   ⚠️ The conventions plugin sets `maxHeapSize = "512m"` on every suite, so this
   criterion is unrunnable until its test task overrides it — **M1.18 must add a
   task with `-Xmx256m`**, or the criterion silently passes at twice the heap it
   names.

## Test plan

**Tier discipline:** everything is T0 unless it names a reason. T4
(`./gradlew clusterTest` — the task name in
[build.md](../../../standards/build.md) and in the conventions plugin) appears
**7 times**, and each occurrence names why a fake cannot answer the question:
T5c, T5d and T5b2 need OpenSearch's own versioning and Lucene state; T8b, T11,
T11b and T11c need real shards and a real Lucene commit.
⚠️ Scale still lives at T1 — criterion 3's 1,600 consumers run against a fake
transport, and only its 20-shard variant is T4.

| # | Test that must fail first | Tier | The mutation it must catch |
|---|---|---|---|
| T1 | `segmentRoundTripsAllRuns` | T0 | swap `byteStart`/`byteLen` in a directory entry. ⚠️ Survives if the swap is symmetric across writer and reader — that case is T1b's golden file, which is why both rows exist |
| T2 | `directoryLookupFindsRunByStreamId` | T0 | off-by-one in the binary search bound — **the case must include the first and last entry and a stream absent from both ends**, or the mutation survives |
| T3 | `putIfAbsentRejectsExistingKey` | T1 | make `putIfAbsent` an unconditional `put` |
| T4 | `ackOnlyAfterSegmentAndCommitDurable` | T1 | ack before the commit delta is written |
| T5 | `offsetsAreMonotonicPerStreamAcrossFlushes` | T1 | reset the per-stream counter on flush |
| T5b | `deleteRoundTripsWithVersionAndNoPayload` | T0 | drop `_op_type` so a delete becomes an index |
| T5b2 | `deleteRemovesTheDocumentFromTheIndex` | **T4** | ack the delete without applying it — criterion 0's middle clause, which no round-trip test can reach because it is about Lucene state, not framing |
| T5c | `staleVersionIsRejectedOnReplay` | **T4** | assemble the envelope without `_version` |
| T5d | `backfillWithOlderVersionDoesNotOverwriteLive` | **T4** | stamp a replay-time version instead of the source's. ⚠️ Concurrency alone is not enough: the test must force the **losing interleaving** (replay applied after the live write), or a last-writer-wins bug escapes whenever the replay happens to land first |
| T6 | `bulkBodyIsNeverFullyBuffered` | T1 | replace the streaming read with `readAllBytes` |
| T6b | `httpAdapterPassesTheRequestThroughUnaltered` | T1 | the handler recomputes the partition, rewrites `_id`, or acks before delegating. ⚠️ Pure *duplication* of a decision that yields the same answer is not observable from outside and is not claimed here — rule 4 is held by `check-module.sh`, not by this test |
| T6c | *(not a test)* `check-module.sh` asserts no module below `http` resolves an HTTP dependency | gate | add an HTTP import below the adapter. ⚠️ Listed here for completeness only: a classpath constraint has no red run in the sense of testing.md rule 2, so it is a **gate**, not a row in this table |
| T7 | `readNextBlocksUntilPushArrives` | T1 | make `readNext` return empty immediately |
| T8 | `idleShardsIssueNoStoreRequests` | T1 | poll the store on an empty queue **once per `pollTimeout`** — the test advances the injected clock 3,000 intervals, so a single leaked poll per interval shows up as 3,000 requests, not as a rounding error |
| T9 | `tailSubscriberIsOnePerNode` | T1 | construct the subscriber in the factory per shard |
| T9b | `segmentCacheIsOnePerNode` | T1 | give each shard its own cache, so 100 shards on a node fetch one segment 100 times — criterion 6's cache half, and the half that carries R5 |
| T10 | `pointerResumesFromCommitData` | T2 | resume from `earliest` instead of `batch_start` |
| T11 | `documentsAreSearchableAfterIngest` | **T4** | drop a run from the segment directory, so fewer than 100 documents arrive. ⚠️ This does **not** catch wrong-shard placement: a `_search` queries every shard of the index and still returns all 100 |
| T11c | `searchableDocumentsCarryMonotonicOffsetAcrossFlushes` | **T4** | stamp `_offset` from the segment's position instead of the sequencer's assignment. ⚠️ The documents must span **at least two flushes** — within a single flush the two coincide and the mutation survives — criterion 2's second half, and the field ADR-0001 makes load-bearing for dedup |
| T11b | `documentLandsInTheShardForItsPartition` | **T4** | index into the wrong shard — asserted with `preference=_shards:3`, which is the only form that fails when placement is wrong |
| T12 | `memoryFlatUnderTenXBodySize` | T2 | unbounded accumulator growth. ⚠️ A **ratio** is satisfied by a copy with a constant factor, so the row must also assert an **absolute** ceiling: peak heap under a 256 MB cap while ingesting 200 MB (criterion 8) |
| T7b | `readNextBlocksForTheFullPollTimeoutWhenNothingArrives` | T1 | return empty after one poll interval instead of blocking — criterion 5's second half, and the half the zero-idle-cost property actually rests on |
| T8b | `idleShardsIssueNoStoreRequestsInACluster` | **T4** | poll the store on an empty queue, at 20 real shards — criterion 3's T4 half |
| T10b | `restartDuplicatesOnlyWithinOneCommitBatch` | T2 | resume from the batch *end* pointer, which loses records, or from `earliest`, which duplicates without bound — criterion 4's duplicate bound |
| T1b | `committedGoldenSegmentStillParses` | T0 | change a field width in the preamble or directory — criterion 7's golden file, which the round-trip test cannot catch because it encodes and decodes with the same code |

**Coverage:** ≥95% line / ≥90% branch per module; **mutation ≥80%** on changed
code. Exclusions: none expected — a skeleton with an exclusion is a smell.

**Suites this milestone starts:** the store conformance suite (non-CAS half), the
cost assertions, the memory-flatness soak. The commit-protocol simulation starts
at M4.

## Risks

| Risk | What would reveal it | If it happens |
|---|---|---|
| ⚠️ **`readNext` cannot block as the source suggests** | T7 fails | The zero-idle-cost property collapses and NFR-2 is unreachable via the SPI. **Stop and re-plan** — this invalidates ADR-0004's economics too |
| A node-level singleton is not reachable from the factory | T9 fails | Fall back to a static registry keyed by store config; if that fails, cost scales with shard count (R5 lost) |
| `_offset` as `LongPoint` conflicts with an existing mapping | T11 fails | Investigate; the pointer type is load-bearing for dedup |
| Helidon's streaming read materialises anyway | T6 fails | Drop to a lower-level connector or reconsider the framework |
| A 202 that means "buffered" leaks in | T4 fails | The ack contract is FR-4; do not relax it for skeleton convenience |

## Tasks

Each is one commit, cites a requirement, and leaves the tree green.

| ID | Task | Serves |
|---|---|---|
| M1.0 | `CredentialSource` SPI + `StaticCredentialSource` + `Principal`; trust domain threaded through the write path ([ADR-0021](../../decisions/0021-credential-source-spi.md)) | FR-20 |
| M1.1 | `BinStore` SPI + `MemoryBinStore` + conformance harness | FR-8 |
| M1.2 | `LocalFsBinStore`, `putIfAbsent` via `O_CREAT\|O_EXCL` | FR-8 |
| M1.3 | `CountingBinStore` + the cost-assertion test fixture | FR-8, R9 |
| M1.3b | `GoverningBinStore`: ratio-to-expected, LIST ceiling, priority classes; writes never refused | FR-21 |
| M1.4 | Segment v0 writer: preamble, directory, runs, footer; record framing carries `_id`/`_op_type`/`_version` ([ADR-0020](../../decisions/0020-record-envelope-and-mapper.md)) | FR-2 |
| M1.5 | Segment v0 reader + round-trip + golden file | FR-2 |
| M1.6 | **In-process `Ingest` API** + config; no HTTP on the classpath | FR-1 |
| M1.7 | HTTP adapter: Helidon, streaming `_bulk` parse, delegates to `Ingest` | FR-1 |
| M1.8 | Ingester: per-stream accumulator + 250 ms/8 MiB flush trigger | FR-2 |
| M1.9 | Ingester: segment build + PUT | FR-2 |
| M1.10 | Ingester: commit log v0 + offset assignment + ack after durable | FR-3, FR-4 |
| M1.11 | Ingester: `/v1/subscribe`, inline delivery | FR-5 |
| M1.12 | Consumer: subscription client, blocking queue, decode, **`DEFAULT`-envelope assembly** | FR-7 |
| M1.13 | Plugin: `BinStorePlugin`, factory, `BinStoreOffset`, `BinStoreMessage` | FR-7 |
| M1.14 | Plugin: blocking `readNext` + node-level singleton | FR-7, NFR-2 |
| M1.15 | T4 end-to-end: documents searchable in a single-node cluster | FR-7 |
| M1.16 | Zero-idle-cost: **T1** at 1,600 consumers, **T4** at 20 shards | **NFR-2** |
| ~~M1.17b~~ | ~~SPDX headers, licence gates, `LICENSE`/`NOTICE` wired into the build~~ — **delivered by `de66330` (M0.4)** | — |
| M1.18 | ~~Gradle memory caps, per-tier test tasks~~ — **delivered by `de66330`**. What remains: a test task with `-Xmx256m` for acceptance criterion 8, and container memory limits | — |
| M1.19b | Metrics with the allow-list labels; in-memory per-index counters; `/admin/cost` top-K | NFR-16 |
| M1.19 | Gate benchmarks: JMH `-prof gc`, allocation-per-record bounds, inside L1's budget | NFR-1 |
| M1.20 | GH Actions: L1 always; L2/L3 by path filter, label and nightly | — |
| M1.17 | Restart resumes from `batch_start` | NFR-11 |
