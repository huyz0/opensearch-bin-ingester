# M11 evidence and milestone review

One evidence line per acceptance criterion of [SPEC.md](SPEC.md). Each names
the test that demonstrated it; the task's backlog row carries the red record
(the mutation each new test was observed failing against) and its commit body
the review verdicts and the minors recorded. What was not run is marked
NOT-RUN, and what was observed not to hold is marked OBSERVED-NOT, rather
than implied.

M11 closes FR-21's per-index half: every data PUT, commit-log PUT and
data-segment GET the ingester issues is apportioned to indices, or to
`unattributed`, with sums exact to the micro-request; `GET /admin/cost` and a
periodic top-K line read the ledger; per-index quotas refuse `429` with
`Retry-After` before a body is read. No object-store request and no PER-INDEX
metric series were added (the new series are pod-level and unlabelled: the
governor's, M10.27, and the segment hold's fall-back counters, M10.25). ⚠️ **It does NOT fully close**: criterion 1, which held when it
landed and was eroded to 628 lines by later M11 wiring (open row M11.24);
H14, `PartitionVisibilityIT`, OBSERVED-NOT green and NOT-RUN on M9's rig
(open row M11.23); criteria 5, 6, 7 and 11 are delivered with the gaps their
lines state (5 shown in parts; 6's with/without-ledger comparison NOT-RUN; 7
with two SPEC deviations; 11's server `checkMutants` NOT-RUN); billed dollars and AWS S3 latency (no AWS account, as at
M9 and M10); and NFR-5's compact per-(node, segment) event, which ADR-0080
decides and defers.

## Acceptance criteria

1. OBSERVED-NOT at close for `Assembly.java`. The split held when it landed: `Assembly.java` 699 → 595 lines and `ConsumerClient.java` 700 → 545, no test changed, the full `./gradlew test` green on that commit (2,588 tests, 0 failures, 1 skipped) (M11.1, `d571f1a`). Later M11 wiring grew `Assembly.java` back past the bound: 601 (M10.27), 613 (M11.3), 619 (M11.22), **628 at close** (M11.5); `ConsumerClient.java` is 555, still under it. Carried as M11.24. (M11.1)
2. `FrontDoorGovernedStoreTest` (T1, 2 cases): a `/ctl/drain` POST's inbox LISTs all appear in `storeCounts().lists()` and in the governor's `recoveryLists`, so the drain is counted and runs in the recovery scope; an absent-key fetch's `404` adds exactly one `stat` to `storeCounts()`. (M10.26)
3. `GovernorMetricsTest` (T1, a scrape of an assembled pod): two LIST refusals and one discretionary refusal each counted under its own metric name, `binstore_governor_refusals_total` their sum, no labels at all; ratio 10.0 and alarm 1 read from the live governor at the halt, the kill switch 1 at 100x. `OrphanSweepGovernorTest#aREFUSEDInboxReadIsLOGGEDNotSilent`: a refused sweep inbox read logs a WARNING naming it (the test asserts the text, not that it appears once). (M10.27)
4. `IndexCostLedgerTest` (T0, 8): largest-remainder splits summing to exactly 10^6, a weightless request to `unattributed`. `DefaultIngestAttributionTest#everyDataPutIsSplitByRunBytesAndTheSharesSumExactlyToTheCountedPuts` (T1): over two-index flushes Σ shares = counted data PUTs × 10^6 and per-index bytes equal the directories' run bytes. `SegmentPublisherAttributionTest`: a failed data PUT is still charged. `CommitChargingBinStoreTest` and `CommitChargingBinStoreDelegationTest`: every commit-log PUT split by record count, CONTINUE, seal, junk, lost races and failed PUTs included. `AssemblyWriteAttributionTest` (T1): an assembled pod's data and commit PUTs all reach its ledger and sum to its counted PUTs. (M11.2, M11.22)
5. `CountingBinStoreGetPurposeTest` (T0): data-segment GETs counted apart. `SegmentProxyAttributionTest` (T1): held, streamed past the hold, deadline overload, failed, undecodable, and 8 joined cold callers giving one GET and one charge. `CatchUpAttributionTest` (T1): one catch-up GET for two streams split by run bytes, an over-budget read unattributed. Every case asserts Σ index GET shares + unattributed = counted data-segment GETs × 10^6. `AssemblyReadAttributionTest` pins the wiring on an assembled pod. ⚠️ Shown in parts, NOT as the one workload the criterion names: each GET kind is pinned with its exact sum, but no single assembled-pod workload exercises all three, and the streamed-past-the-hold case is component-level only (review; harvest H21). (M11.3)
6. `DefaultIngestAttributionTest` (T1): after a warm-up flush, each attributed flush issues exactly one data PUT and one commit PUT and no GET, STAT or LIST, which kills the test plan's mutation (a stat per apportionment); `./gradlew gates` (its metric-cardinality check refuses an `index` label) green on every M11 commit. ⚠️ The criterion's literal form -- the same workload run with the ledger and without it, `StoreCounts` compared -- is NOT-RUN as such: no test runs that A/B; the request shape above is the evidence. The test plan names `DefaultIngestAttributionTest#theLedgerIssuesNoRequest`, which does not exist; the assertion lives in the case above. (M11.2, M11.4)
7. `IndexCostReportTest` (T0, 10) and `AdminCostServiceTest` (T1): the top N by estimated USD in order (ids sorted against it), names from the catalog, bytes, exact apportioned requests, pod totals and `unattributed`; `top` outside 1–1000 or `by` other than `index` is `400` before any report is built. `AdminCostAssemblyTest` (T2, assembled pod after real appends): the route's data and commit shares plus unattributed equal the pod's counted data and commit PUTs. ⚠️ Two SPEC deviations, recorded here and not before: a missing `by` defaults to `index`, and the report carries the three prices rather than the price table's name. `AdminCostAssemblyTest` uses one index on the free in-memory backend, so it pins neither `top` reaching the report nor the prices wiring (harvest H21). (M11.4)
8. `CostTopKReporterTest` (T0, 7, injected clock): exactly one line per interval naming the top 3 by estimated USD, none before the first interval, none when disabled; `CostTopKReporterIdleTest` and `CostReportingAssemblyTest` (the wiring, and `PT0S` logging none). The line ranks what each index cost IN THAT INTERVAL (then by requests), a refinement of the SPEC's wording. (M11.5)
9. `BulkServiceRetryAfterTest` (T1): the lane-admission `429` carries `Retry-After: 1`; `BulkServiceQuotaTest`: the quota `429` carries its debt's repayment time; both through `BulkService.tooManyRequests`, the one place a `429` is sent. (M11.6, M11.8)
10. `AdmissionDurableWaitTest` (T1, a real `DefaultIngest` with the data PUT held): with a budget of 1 a second request is admitted while the first waits for durability, both `202`; `UnflushedBytesCeilingTest` bounds the memory that frees (ADR-0079). (M11.7)
11. `IndexQuotasTest` (T0, 7) and `BulkServiceQuotaTest` (T1): an index in debt refused `429` with `Retry-After` = its repayment time rounded up, before the body is read, another index admitted; a request admitted with tokens goes into debt and is never cut off; five concurrent requests against a cap of two admit two; `FrontDoorQuotaTest` on the assembled front door; with no quota configured nothing is refused (`IndexQuotasTest`'s unconfigured case). `checkMutants` for `:server` in M11.8 is NOT-RUN: its run was invalid (jzap baseline failures under a concurrent Gradle build); the reviewer's own mutations stand instead. (M11.8)
12. M10.24 `SubscriptionChannelTest#aBUSYStreamCostsONEPollPerROUNDTripRatherThanOnePerPUSH`; M10.25 `NodeSegmentSourceRefetchCountTest`, `PluginMetricRegistryTest`; M10.28 (split) `HeldFetchIsNotAnAttemptTest`, `HeldRoundAfterOwnPauseTest` (M10.28a) and `NodeSegmentSourceFailureHoldTest`, `NodeSegmentSourceFailureResetTest`, `FailureHoldWithRetryTest`, `FailureHoldWiringTest` (M10.28b); M10.30 `RoutedIngestExplicitWaitTest`, `ExplicitPartitionUnregisteredTest`; M10.36 `SharedClientsCloseConnectionsTest`, `PeerClientsCloseConnectionsTest`. (M10.24–M10.36)
13. ADR-0080 decides the floor (one compact event per (node, segment), implementation deferred; interim bound 200 B per (segment, cross-zone stream)). `CrossAzBytesIT#theEventFloorIsPerCrossZoneStreamAtKOverOne` (T3, RustFS) passed: 4 segments, 8 cross-zone deliveries, 1,334 B cross-AZ, 166 B each against the 200 B bound, 0.072%. (M10.34)
14. H2–H15, each in its row: H2 `BulkServiceRetryAfterTest`, `AdmissionDurableWaitTest` (M11.6, M11.7); H3 the split (M11.1); H4 `IngestLaneDefaultsTest` (M11.9); H5 `FlushCoordinatorHardeningTest`, `DefaultIngestSettlementPathsTest`, `DefaultIngestErrorTest` (M11.10); H6 `GovernorResidueTest`, `SegmentPrefetchAsksTest`, `SegmentPrefetchAccessOrderTest` (M11.11); H7 `TierTwoResidueTest`, `TierTwoEpochMoveTest`, `NodeLocalStoreReaderDeadlineResidueTest` (M11.12); H8 `ConsumerRetryResidueTest` (M11.13); H9 `SegmentProxyPutBeforeRemoveTest`, `SegmentFetchMidSegmentPresenceTest`, `HttpSegmentSourceGrantsOnlyTest` (M11.14); H10 `DurableSignalRingOwnerTest`, `RoutedIngestTest` margins (M11.15); H11 `PushQueuePinsTest` (M11.16); H12 `GoldenSegmentV1IdentityTest` (M11.17); H13 `WiredFactoryTest` and the reference fixes (M11.18); H14 `PartitionVisibilityIT` **OBSERVED-NOT green and NOT-RUN on M9's rig** -- M=1,000 drained in 2,889 and 2,830 ms against its 2,500 ms bound on a 4-core container, where M9 measured 2,209 ms; carried as M11.23 (M11.19); H15 `MilestoneEvidenceTest` (M11.20). Items not pinned are named as such in their rows: M11.10's before-detach check, M11.11's zero-PUT branch (equivalent), M11.13's race-guard rollback (unpinned: no deterministic test can make the two reads disagree; `catchUpDecodeLock`, once claimed equivalent, is NOT, and is pinned by `ConsumerRetryDefaultsTest#twoReadersNeverBothFetchTheCatchUpHead`), M11.14's too-large fallback (outcome-equivalent). (M11.1, M11.6, M11.7, M11.9–M11.20)

## Gates, as run for this close

- For every M11 commit, the authoring session ran `./gradlew gates` (repository gates, `checkHarnessTests`' native gate tests, the wired and override gates), `checkTdd`, `checkReviewed`, `checkTestIntegrity` and `checkCommitMessage` before committing, plus the affected modules' tests. That is the session's own record, not something the tree can show.
- **On this close's tree before the close edits** (`762eae2`), `./gradlew test --rerun` re-ran every module's test task (the buildSrc harness tests are not among them; they run through `checkHarnessTests` in `gates`): **2,749 tests, 0 failures, 1 skipped** (result files all from that run). A first `./gradlew test` without `--rerun` reused up-to-date results and is not counted.
- During M11.14's gating, one run of `:ingest:test` timed out `DefaultIngestTest#aCOMMITInFlightDoesNotBlockTheNextAccumulator` at its 30 s class limit; not reproduced in 5 runs alone, 8 whole-class runs under CPU load, or the module rerun, and its stack trace was lost. OBSERVED, not explained: open row M11.25. A candidate cause, unverified: the test's appends run on the ForkJoin common pool, 3 threads on this 4-core rig (M11.15 review T2).
- `./gradlew checkMilestoneVerified -PmilestoneDir=docs/internal/product/milestones/M11`, with the default moved to M11: "milestone evidence covers 14 criteria".
- `:server:integrationTest` ran only `PartitionVisibilityIT` in M11 (four runs, M11.19) and `CrossAzBytesIT#theEventFloorIsPerCrossZoneStreamAtKOverOne` (M10.34); other integration and soak suites are NOT-RUN for this close. `checkMutants` and `checkCoverage` are manual and were NOT-RUN over the whole milestone; per-module scores quoted in rows M11.2-M11.8 and M11.22 were run by those tasks, and reviewers ran targeted mutations for every task, recorded in each commit body. The cost meter does not exist, so no request-rate trend is claimed.

## Milestone review

Run by an agent that wrote none of it, over every commit in `608554f..762eae2`
per `milestone-review`: the diff, every commit body, the SPEC, the roadmap and
the backlog, reading code rather than running suites. Its criterion check
produced the corrections folded into criteria 1, 5, 6, 7, 8 and 11 above.

- **F1, moderate — the file splits did not hold.** `Assembly` 595 → 628 (criterion 1), `DefaultIngest` 586 → 700 → 696 (back under the limit only by a deletion), `BulkService` 487 → 628, `SegmentProxy` 680, `HttpSubscriptionTransport` 692 — M10's F5 again, just before fast mode lands in the same files. Harvest H1; M11.24.
- **F2, moderate — cost charging is built three ways** (a publisher hook for data PUTs, two call-site hooks for data GETs, a store decorator for commit PUTs), and four production constructors (`SegmentProxy`, `DurableCatchUpResponder`, `SegmentPublisher`, `DefaultIngest`) silently make a private ledger nothing reads: a new read path built with one breaks "shares sum to counted requests" with no signal. H2.
- **F3, moderate — silent `Ingest` defaults came back**: the two `buffered` overloads and `concreteIndex`, the shape M10's F6 flagged; `concreteIndex`'s default already caused M11.8's round-1 defect. H3.
- **F4, moderate — the unflushed-bytes ceiling has a race** (M11.7 P4): a finishing flush can reset the in-flight count after the next one set it, admitting about 2x the ceiling; the "only wait if a flush will come" guard is unpinned. H4.
- **F5, moderate — admission's parked-producer problem returns per index**: a default quota gives every index an in-flight cap of 8 across the durable wait, and no metric counts `429`s. H6.
- **F6, moderate (security.md rule 5)** — with a default quota, `IndexQuotas` makes a bucket for any name a producer sends and never removes it. H5.
- **F7, moderate (security)** — `/admin/cost` lists up to 1,000 index names on the unauthenticated producer port, against `BulkService`'s bodiless-403 rule. H7.
- **F8-F10, low** — tests that compute their expectation with the code under test; the plugin's re-fetch fall-backs counted but not bounded and the node failure hold without jitter; the NFR-5 figure omits poll and header bytes, and ADR-0080 did not weigh an own-zone relay. H12, H16, H22.

No new object-store request was found; M10.28b reduced them.

### Harvest for the specification of M12

Every `minor` recorded in an M11-window commit body was read and marked ROW or
DROP with its reason (the full table is the review's working file, not
committed); the ROWs consolidate into the following, which become backlog rows
when M12 is specified. Open rows M11.23, M11.24 and M11.25 are carried as they
stand.

- H1. **Split `DefaultIngest` (696), `Assembly` (628) and `BulkService` (628) before fast mode touches them.** Candidates: the unflushed ceiling and flush enqueue out of `DefaultIngest`; ledger, quota and cost wiring out of `Assembly`; `Admitted` and quota admission out of `BulkService` (F1).
- H2. Remove the ledger-less production constructors of `SegmentProxy`, `DurableCatchUpResponder`, `SegmentPublisher` and `DefaultIngest`, or have `/admin/cost` report counted minus charged (F2, M11.3 P1).
- H3. Make `Ingest`'s buffered overloads and `concreteIndex` abstract, or pin their defaults as M11.9 did (F3).
- H4. Fix the `inFlightBytes` race and pin both sides of the ceiling's "a flush will come" guard (F4; M11.7 P4, T5).
- H5. Bound quota buckets: none for an unregistered name, and expire idle ones (F6; M11.8 P4).
- H6. Export pod-level refusal counters (`binstore_ingest_refusals_total` split into admission and quota as separate names, no index label), and name refused indices in the top-K log (F5).
- H7. Gate `/admin/cost` behind an opt-in until M1.7c (F7).
- H8. A gate predicate: one `429` emitter (M11.6 P1).
- H9. `/admin/cost` survives one malformed registration (M11.4 P1).
- H10. Bound `cost.top-k-interval` and log the real window (M11.5 P1b, P3).
- H11. The explicit-partition wait gets a per-index cap, or releases its permit (M10.30 P1).
- H12. Jitter the node failure hold and pin `holdMillis` at 55 and 65 failures (M10.28b P1, T2).
- H13. Separate the live and catch-up retry state, or justify sharing it by test (M10.28 carried).
- H14. `QuotaProperties`: alias-named overrides (M11.8 P5).
- H15. Close the `checkMilestoneVerified` fail-open and duplicate-number gaps (M11.20).
- H16. ADR-0080: consider the own-zone relay, and measure the unmetered per-stream bytes (M10.34 P2, P3, T2).
- H17. `DefaultIngestTest` timeout: a thread dump on class timeout, and a recorded occurrence (M11.14).
- H18. `PushQueue` drain bound injectable, and abandoned pushes counted (M11.16 T1).
- H19. `GovernorMetrics` bound per registry (M10.27 P2).
- H20. `expiredOr` gets a typed exception; the consumer offset restore takes its lock (M11.12 R1, M11.13 R4).
- H21. One assembled-pod read workload with all three GET kinds; `AdminCostAssemblyTest` with `top` and prices (M11.3 T2; M11.4 T2; M11.5 T2).
- H22. Test-pin bundle: M10.27 T3, M11.22 T1, M11.4 T1, M11.6 T2, M11.8 T5, M10.30 T2, M10.25 T1/T2, M10.28a T2/T3, M10.28b T1/T3, M10.36 T1, M10.24 T1, M10.34 T1, M11.9 T1, M10.35 T1.
- H23. Doc/ADR text bundle: M10.37 P1, M11.0 R3/R4, M11.1 P1, M10.27 P4, M11.2 T3, M11.22 R1, M11.4 P3, M10.36 P1, M10.34 P1, M11.19 R5.
- H24. Flake and suite-time bundle: M11.12 T2, M11.13 T5.
- H25. JVM gates: the ADR short form and io-seam exact-path anchoring, if they share the scripts' gap (M10.37 T3/T4).

### Re-plan

The roadmap gives M12 fast mode (FR-17) "after the default path is measured".
The review recommends M12 NOT start with it: M11.23 is an unexplained +28-31%
on the default path's visibility latency, on the commit path fast mode will
share (bisect the governor on the drain and `CommitChargingBinStore` on the
container if M9's rig cannot be had); the files fast mode lands in are at or
over their limits (H1); and F2/F3 are hazards a second write path would
trigger. Order: M11.23, the splits, H2-H4, then fast mode's spec, with the
rest interleaved. ADR-0080's wire change should get an explicit milestone
(M13 suggested), after the relay alternative is weighed (H16). AWS S3
latency, TTFB and billed dollars remain NOT-RUN since M9.

### Research corpus

Updated in this close: `15-cost-governor.md` (per-index counters are an
apportionment, `{class}` became two unlabelled names, the kill switch's
top-three page is not built), `11-multi-tenancy-and-security.md` (quotas are
per pod, the in-flight cap is a concurrency limit, `maxUnflushedBytes` is
ADR-0079's ceiling) and `05-az-topology-and-data-flow.md` (the ~166 B
per-(segment, cross-zone stream) floor and the relay alternative). The
README's routing table is unchanged.
