# M9 evidence and checkpoint review

This is the final M9 evidence and milestone review. The enumeration gate checks
that each numbered row exists and names evidence; the review below checks the
claims against the current tree. M9 closes under the amended roadmap gate with
NFR-5 explicitly PARTIAL: all implemented cross-AZ transports are measured, but
the planned proxy segment-fetch payload route is absent and its byte term is
NOT-RUN. This is not a claim of full NFR-5 compliance; M10 owns that route and
the complete measurement. AWS S3 latency, TTFB and billed dollars also remain
NOT-RUN as already scoped by M9.22.

## Acceptance criteria

1. `MutantsGateTest#failsWhenSurvivorsPutTheScoreBelowTheEightyPercentFloor` and `#exactlyEightyPercentKilledPasses` run under `checkHarnessTests` and pin seeded-mutant scoring. The Gradle `checkMutants` task is manual-stage, not part of `gates`; the full mutation suite was NOT-RUN for this checkpoint.
2. `CostMeterTest` and its T0 arithmetic tests cover requests/MiB, idle rate and price-table dollars; `:binstore-spi:test` evidence is in the M9.1 backlog row.
3. `WriteRequestRateIT` full RustFS profile passed all three five-minute size-triggered points at 40/80/160 MiB/s: 2,515/2,509/2,323 aggregate PUTs, 0 LISTs, and 0.24936/0.24924/0.25009 requests/MiB. `LowRateWriteBudgetIT` passed the full profile at five minutes per 250 ms and 5 s point: 1,980/232 aggregate PUTs, partitioned by purpose in `measurements/results/low-rate.csv`; both had zero LISTs, and data+commit, checkpoint, and lease cadence bounds passed. The exact four-suite local profile took 26m; `check-cost-test-results.py --profile full` passed. The separate GitHub nightly workflow remains NOT-RUN.
4. `IdlePodCostSoakTest` passed locally (301.862 s, recorded under M8.75); the measurement workflow statically asserts the nightly soak job through `check-measurement-workflow.py`.
5. `ReadRequestRateIT` Test measured the current M8.56 prefetch path; see `m9.9-read-request-rate.md` and the M9.9 backlog evidence. Its result does not close the separate inherited catch-up and fallback work.
6. **PARTIAL — not full NFR-5 compliance.** `CrossAzBytesIT` measured 0.0598% on the implemented, named current-build transports (M9.10). The proxy segment-fetch payload route does not exist, so that term is NOT-RUN. Existing measurements do not establish the full design's byte budget. The unchanged NFR-5 requirement and full-route proof are assigned to M10 in the roadmap.
7. `VisibilityLatencyIT` Test passed at 250 ms, 1 s and 5 s; p99 values and RustFS lower-bound limitation are recorded in `m9.11-visibility-latency.md`.
8. `VisibilitySearchableIT` passed its 1-second `clusterTest` point; 537.919 ms is recorded in `m9.11-visibility-latency.md`.
9. `PartitionVisibilityIT` Test passed the M=10 and M=1,000 drain bounds and audited visible durable-intent acknowledgements; see `m9.12-partition-visibility.md`.
10. `m9.6-allocation-gate.md#noise-floor` records the `:bench:jmh` allocation gate and ten-fork noise-floor evidence; this is a leading indicator, not a new NFR-6 memory-flatness proof.
11. `codec.csv#M9.7` and `m9.7-codec-comparison.md` record B3 codec/block measurements and the generated curve; the M9.7 run and repository gates passed.
12. `DirectThresholdIT`, `m9.14-direct-threshold.md` and ADR-0067 establish the RustFS request-cost threshold at fan-out 1. S3 TTFB, the decisive latency term, is NOT-RUN.
13. `KillNodeMidBacklogIT` is the M8.24h evidence for the completed parent; the M8.24a–h rows record the protocol, server, consumer and T4 results. See their detailed evidence in the backlog. The separate full nightly workflow remains NOT-RUN.
14. `GapRereadIT#gapReplayIsContiguousAndCostsOneGetPerUnindexedSegment` (M9.13) measures the reachable-ingester gap reread through catch-up and does not claim to price the no-ingester Tiers 2/3 path.
15. `M8.24`, `M8.56`, `M8.58` and M8.60–M8.85 inherited rows are complete. M8.62's source map and every test/mutation disposition are in its backlog row; `AssemblyTest#aStoreCloseFailureIsSuppressedOnThePrimaryAssemblyFailure` was observed failing when cleanup suppression was removed and passes restored. M8.73/.78 are closed by children M8.76/.77 and M8.79/.80; `DurableSignalRingOwnerTest#multipleReadyCandidatesInOneAzWarmOnlyTheDeterministicRingOwner` closes M8.85. M8.64 is prose-only and excluded from automated criterion-line coverage by the spec; its review disposition is recorded in the backlog row.
16. `gradlew generateCostLatencyCurve` generated the committed Markdown/SVG from the final measured M9.8/M9.56 CSVs; `checkCostLatencyCurve` is wired into `gates`. Full five-minute profile evidence is recorded in the inputs; other result files retain their own measured/smoke status.
17. `measurement.yml`, the 60-second L1 subset and the `nightly-measurement` execution-layer docs are checked by `checkMeasurementWorkflow`. The complete full local profile passed in 26m; the separate nightly GitHub workflow remains NOT-RUN.
18. `LadderStoreTiersIT` and M9.21 are recorded done: the canonical Tier 3 episode measured five GETs plus one checkpoint-pointer STAT, with zero LISTs; the M8.24 replay path is complete. The test also covers a second episode where ingester service returns during recovery. See M9.21/M9.45 backlog evidence; the separate nightly workflow remains NOT-RUN.
19. M8.58 is recorded done: required pod UID is written by normal and GC lease paths, persisted-byte and takeover tests pass, and lease goldens cover compatibility. Fleet rollout and a real cluster remain NOT-RUN; this criterion claims implementation and local test evidence, not deployment.

**Explicit scope disposition:** FR-21's refusing half is owned by no milestone
after M9. M9 delivers request counting only; it does not build a production
cost governor that refuses writes.

## Inherited rows

Evidence references below point to the named backlog task's acceptance evidence and commit unless marked NOT-RUN. Statuses are transcribed from `docs/internal/product/backlog.md` at this checkpoint.

| Row | Evidence / disposition |
|---|---|
| M8.24a | `ADR-0065`; protocol decision done. |
| M8.24b | Bounded replay source/coordinator component tests; done, with the parent integration completed by M8.24f–h. |
| M8.24c | Versioned catch-up frames, goldens and refusal tests; done and exercised through the registered route. |
| M8.24d | HTTP catch-up control seam and production replay wiring; done. |
| M8.24e | Durable responder, registered route and T4 integration; done. |
| M8.24 | **done** — M8.24f/g/h and `KillNodeMidBacklogIT` complete; see the M8.24 backlog row for the full T4 evidence and conditions. |
| M8.56 | `AssemblyBatchingTest` and M8.84/M8.85 assembly, RustFS and multi-candidate ownership evidence; done. M8.85's focused mutation/red and restored-green result is recorded in its backlog row. |
| M8.58 | **done** — pod UID lease wire-format implementation and compatibility tests; fleet rollout and real-cluster execution remain NOT-RUN. |
| M8.60–M8.61 | **done** — bounded metrics and the numbered chaos assertion children are recorded complete in their backlog rows; the platform-specific force-kill caveats remain explicit there. |
| M8.62 | **done** — M8.62a–f and existing source-task tests close the 17-source audit; the close-diagnostic survivor's test/red/green evidence is in M8.62f. The `MAX_POOLED_CLIENTS` numeric mutation is equivalent for the stated bounded-pool behavior, as any positive fixed limit remains bounded. |
| M8.63–M8.72 | **done** — completed child and task evidence is recorded in the backlog; includes the bounded polling, prose, lifecycle, callback classification and logging work. |
| M8.65 | `InboxDrainRaceTest#aLocalAndBatchedDrainShareTheInnerTermLock`; done. |
| M8.66 | `ChainBackfillTest` deposition/all-or-nothing cases, assembly wiring test, and cost.md budget; done. |
| M8.67 | `NodeSegmentSourcePerKeyLockTest` proves independent keys progress and same-key fetch coalescing; done. |
| M8.68 | `PollOutcomeCountTest` callback-failure classifications; done. |
| M8.69 | `ServerPropertiesTest` blank duration/bytes/boolean refusal cases; done. |
| M8.70 | `NodeShutdownTest#aDRAINDelaysBulkRefusalAfterReadinessFails`; done. |
| M8.71 | `NodeProcessKillResumesPausedTest`, `RetryFloorCallerTest`, and assembly/positions ownership tests; done. |
| M8.72 | `PluginLoggingIT` captures all three paths in OpenSearch Log4j; done. |
| M8.73–M8.74 | **done** — M8.76 and M8.77 close the split gate work; M8.74's peer-response attribution tests and focused suites are complete. |
| M8.75 | `IdlePodCostSoakTest` passed locally and `check-measurement-workflow.py` pins nightly `soakTest`; done. |
| M8.76 | JVM and Python `check-wired` resolution tests; done. |
| M8.77 | **done** — qualified nested construction is pinned in both scanners. |
| M8.78 | **done** — children M8.79 and M8.80 complete the split runner hardening. |
| M8.79 | `test_tdd_scan.py` and `test_tdd_red.py` Windows path/selector tests; done. |
| M8.80 | `test_tdd_scan.py` and `test_tdd_red.py` prove stale-report cleanup failure refuses before Gradle and cannot mint red evidence; done. |
| M8.81 | `test_source_digest_uses_canonical_git_line_endings` plus `checkTdd`; done. |
| M8.82–M8.85 | **done** — ready AZ-labelled membership, authenticated durable hint, assembled RustFS fetch, and multi-candidate deterministic ownership; the M8.85 adversarial mutation result is recorded in the backlog. |
| M8.82 | `EndpointSliceViewTest` ready AZ-labelled peer snapshot; done. |
| M8.83 | BPDS versioned frame golden/authentication/byte-count component tests; done, integrated signal use is M8.56. |
| M9.42 | `NodeLocalStoreReaderKeyPolicyTest`, `NodeLocalStoreReaderTest`, `NodeLocalStoreReaderMainTest`, `NodeLocalStoreReaderProcessIT` including `--init-secret`, and POSIX secret-permission regression; `gates`, TDD, test-integrity, and diff-scoped mutation (111/138, 80.4%) passed. `check-coverage.sh` measured coverage below repository floors; Gradle `checkCoverage` could not parse missing JaCoCo `report.dtd`. Cost-meter gate is absent. |
| M9.43 | `TierTwoChainPollTest` with 16 distinct node subscriptions, identical and delayed cursors, repeated 404s, newly available deltas, one-time bounded handoff, no read after the last subscription closes, and rejection of a wrong-sequence delta; `TierTwoReleaseRaceTest` verifies a concurrent final release waits for an admitted read and prevents subsequent reads. Loopback plugin wiring asserts the canonical delta key and decoded delta. `CatchUpSchedulingWiringTest` asserts the node schedule invokes Tier 2. `:client:test :plugin:test` passed; repository gates passed; diff-scoped mutation killed 66/66 scorable mutants (100%). Automatic Tier 2 has one GET per node per 5 s interval only while subscriptions remain active, the cursor is unchanged, and no ingester answers; zero LIST/STAT capability and zero requests while idle or while an ingester answers. Cost-meter gate is absent. |
| M9.44 | **done** — `TierThreeRecoveryTest`, `TierThreeRecoveryCoordinatorTest`, and bounded replay/error tests prove ordered checkpoint/delta/segment replay, the per-episode 30-GET and 64 MiB caps, zero LISTs, unchanged cursor on incomplete recovery, and stop-on-ingester-return; ADR-0064 records the cap and latency consequence. Repository gates, TDD, integrity, cost-latency and diff-scoped mutation passed; repository coverage remains below existing floors. |
| M9.45 | **done** — RustFS `LadderStoreTiersIT` measured the canonical recovery at five GETs plus one checkpoint-pointer STAT, zero LISTs, one shared segment GET, contiguous offsets, and fallback cessation after ingester restoration; a second episode stopped local replay after six GETs and one STAT when an ingester returned. Full M9.8/M9.56 RustFS profile passed locally; separate GitHub nightly remains NOT-RUN. |

## Final milestone review

### Findings

1. **NFR-5 remains partial by explicit scope, not by measurement omission.** M9.10 measured the transports present and reports 0.0598%, but the proxy segment-fetch route and its potentially dominant payload bytes are absent. M10 owns implementing the planned same-AZ route and measuring the entire NFR-5 budget; do not claim full compliance before that evidence exists.
2. **Fallback criteria are complete.** M8.24, M9.13 and M9.21 have executable local evidence, including `LadderStoreTiersIT` and the measured recovery counts; the separate nightly workflow remains NOT-RUN.
3. **Mutation-gate wording is reconciled.** M0.14's checker behavior is tested and the Gradle task is wired, but `checkMutants` is manual-stage; `gates` does not execute the full mutation suite. M9 evidence no longer implies otherwise.
4. **FR-21's refusing half remains unowned.** M9 delivers counting only; M10 must decide the governor design/ownership before production is protected from a cost-budget breach.
5. **Minor review harvest for M10:** carry test/maintenance follow-ups for `5f7e242` (false-refusal baseline and empty `CHECK_RANGE` fixture), `34e4fcd` (independent byte expectation and mutation-run crash limitation), `2cd990f` (sleep-based chaos poll), `872efb9` (duplicate curve rows and zero-GET pricing pin), `8a0e9ec` (legacy singleton default), `eef4d88` (oversized end marker), `b65b9f9` (partial overlaps), `bf8877b`/`c1d9f54` (one consolidated refusal-permit assertion), `fb0f56b` (monitor held over bounded read), `895710a` (reachability change during final segment GET), `355899e` (test-plan tier label), and `1ed333b` (nested-package coverage). Keep `ae5b4c4`'s RustFS wording clarification. Explicitly drop stale scheduling/status notes in `3f141e6`, the M9.49 request-shape concern resolved by M9.50, the non-issue duplicate-table note in `d12d6ad`, and rationale-only/unspecified notes in `34e4fcd`/`3b696b7`.

### Re-plan

M9 is complete under the amended roadmap gate, with the above partial explicitly retained. M10 starts with the proxy segment-serving route/full NFR-5 proof, FR-21 governor ownership and design, and the review-harvest items listed above. The full M9.8/M9.56 RustFS profile passed locally in 26m; the separate nightly workflow remains NOT-RUN. M9.22's roadmap amendment remains accurate: AWS S3 tail latency, M3 TTFB and billed dollars are NOT-RUN on this rig.

No new research conclusion overturns the existing corpus. The checkpoint reinforces its distinction between backend-independent request counts and RustFS-only latency; the timeout is an execution-layer limitation, not evidence to change a cost threshold.
