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
8. **NOT-RUN, but UNBLOCKED (M1.7b).** No test exists under any name still
   (checked: no reference to a memory/heap-measurement test anywhere in
   `http`, `ingest`, `client` or `plugin`). What WAS genuinely blocking it is
   fixed: `Ingest.append` no longer takes a `List<SegmentRecord>` — it takes
   a `RecordSource` (`ingest/src/main/java/binjava/ingest/Ingest.java`), and
   `BulkService` appends in bounded chunks of 1,000 records as it parses
   (`BulkServiceTest#bulkServiceNeverBuffersTheWholeBodyBeforeAppending`,
   `./gradlew :http:test`, proves the first record reaches `Ingest.append`
   after only a small prefix of a 4+ MiB body is read). Chunking, not a
   single streamed call, because review found a first draft held the pod's
   one accumulator lock for the duration of the request's own socket read,
   serialising every other producer behind it — bounding the chunk size
   bounds the lock's hold time independently of body size or network speed.
   `MAX_BODY_BYTES`/`MAX_RECORDS` are deliberately left at their pre-existing
   caps (32 MiB / 200,000): M1.18 (the `-Xmx256m` test task) and T12
   (`memoryFlatUnderTenXBodySize`) remain `todo` — they are what would PROVE
   200 MB is safe under a 256 MB heap and justify raising those caps, which
   this change makes possible rather than attempts itself.
