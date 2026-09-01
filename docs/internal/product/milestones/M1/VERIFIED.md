# M1 — Verified

One line per acceptance criterion in [SPEC.md](SPEC.md), naming the test or
command that demonstrated it. Per the `milestone` skill: this enumerates: it
does not certify the evidence is true beyond what is recorded here, and a line
naming something not actually run would violate non-negotiable 4.

-1. `DeleteAndVersionIT#testAnOlderReplayDoesNotOverwriteANewerLiveWrite` (T4).
    `./gradlew :plugin:clusterTest --tests binjava.plugin.DeleteAndVersionIT`.
    Mutation verified killed (omitting `_version`).
0. `DeleteAndVersionIT#testDeleteRemovesTheDocumentFromTheIndex` and
   `DeleteAndVersionIT#testStaleVersionIsRejectedOnReplayRatherThanResurrectingTheDocument`
   (T4). Same command as above. Two mutations verified killed (dropping
   `_version`; encoding a delete as an index).
1. `DefaultIngestTest#noAppendIsAckedBeforeItsCommitDeltaIsDurable` (T1,
   `./gradlew :ingest:test`). SPEC's test-table name for this row
   (`ackOnlyAfterSegmentAndCommitDurable`, T4) has drifted — tracked as M1.22.
2. `SearchableIT#testDocumentsAreSearchableAfterIngest` (first half, `_search`)
   and `OffsetMonotonicityIT#testOffsetIsPresentAndMonotonicAcrossTwoFlushes`
   (second half, `_offset` read directly off the shard's Lucene reader) — both
   T4, `./gradlew :plugin:clusterTest`. Mutation verified killed for the
   second half (stamping `_offset` from `0` on every delivery).
3. **OBSERVED-NOT, resolved by construction instead — see ADR-0023.** No
   runtime test asserts this; the runtime tests written to do so (a
   1,600-consumer T1 test, a `tick()`-driven variant, a 20-shard T4 variant)
   were all attempted and withdrawn as unfalsifiable, per ADR-0023. Verified
   instead by the import-absence grep ADR-0023 names: `grep -rn
   binjava.binstore plugin/src/main client/src/main` — no matches (checked at
   commit time of ADR-0023; not yet a script — M1.16e).
4. `RestartResumeIT#testBatchStartSurvivesARealEngineCloseAndReopen` (T4, node
   level) and M1.17's three T2 tests (pointer contract) — both
   `./gradlew :plugin:clusterTest` and `./gradlew :plugin:test`. Found and
   fixed a real production bug while writing the T4 half (`NodeSubscriptions`
   reference counting, M1.17b).
5. `ConsumerClientTest#readNextBlocksUntilAPushArrives` and
   `ConsumerClientTest#readNextBlocksForTheFullTimeoutWhenNothingArrives` (T1,
   `./gradlew :client:test`).
6. `NodeSubscriptionsTest#oneHundredShardsOfOneStreamOpenASingleSubscription`
   (T1, `./gradlew :plugin:test`). SPEC's test-table names for this row
   (`tailSubscriberIsOnePerNode`, `segmentCacheIsOnePerNode`) predate
   `NodeSubscriptions` unifying both concerns into one node-level singleton —
   tracked as M1.22.
7. `SegmentReaderTest#everyRunRoundTripsWithItsRecordsIntact` (T0) and
   `GoldenSegmentTest#theCommittedGoldenSegmentStillParses` (T0),
   `./gradlew :format:test`.
8. `MemoryFlatUnderTenXBodySizeTest#memoryFlatUnderTenXBodySize` (T12),
   `./gradlew :http:memoryBoundTest` (M1.18) — a real, dedicated task under
   `-Xmx256m` with JaCoCo disabled; the default `test` task's 512 MB, or
   `test` with JaCoCo attached, would prove nothing about this criterion's
   own number (measured: JaCoCo instrumentation alone added ~200 MB before a
   single body byte was sent). Sends a genuinely 200 MB `_bulk` body,
   streamed from the client and never held whole, through the real HTTP
   adapter and a `LocalFsBinStore`. Two proofs: the request completes without
   a real JVM `OutOfMemoryError` under the actual 256 MB constraint, and a
   periodic GC-then-sample of live heap stays under both an absolute ceiling
   and a 10x-body-bytes-written ratio. Mutation verified killed: disabling
   chunking produces a genuine `OutOfMemoryError: Java heap space`.
   `MAX_BODY_BYTES`/`MAX_RECORDS` raised to 256 MiB / 2,000,000 to let the
   test's body through at all.
