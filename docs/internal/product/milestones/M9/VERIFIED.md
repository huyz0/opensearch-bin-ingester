# M9 evidence and checkpoint review

This is a source-tree audit, not a claim that M9 is complete. The enumeration
gate checks only that each numbered row exists and names evidence; the review
below checks the claims against the current tree. NOT-RUN and OBSERVED-NOT are
intentional results where the acceptance condition is outstanding or absent.

## Acceptance criteria

1. `check-mutants.sh` is wired through `gates` and `checkMutants`; M0.14's checker tests pin seeded-mutant behavior. The full mutation suite was not rerun for this checkpoint.
2. `CostMeterTest` and its T0 arithmetic tests cover requests/MiB, idle rate and price-table dollars; `:binstore-spi:test` evidence is in the M9.1 backlog row.
3. `WriteRequestRateIT` full RustFS profile passed all three five-minute size-triggered points at 40/80/160 MiB/s: 2,515/2,509/2,323 aggregate PUTs, 0 LISTs, and 0.24936/0.24924/0.25009 requests/MiB. `LowRateWriteBudgetIT` passed the full profile at five minutes per 250 ms and 5 s point: 1,980/232 aggregate PUTs, partitioned by purpose in `measurements/results/low-rate.csv`; both had zero LISTs, and data+commit, checkpoint, and lease cadence bounds passed. The exact four-suite local profile took 26m; `check-cost-test-results.py --profile full` passed. The separate GitHub nightly workflow remains NOT-RUN.
4. `IdlePodCostSoakTest` passed locally (301.862 s, recorded under M8.75); the measurement workflow statically asserts the nightly soak job through `check-measurement-workflow.py`.
5. `ReadRequestRateIT` Test measured the current M8.56 prefetch path; see `m9.9-read-request-rate.md` and the M9.9 backlog evidence. Its result does not close the separate inherited catch-up and fallback work.
6. PARTIAL: `CrossAzBytesIT` Test measured 0.0598% on its named current-build transports (M9.10). The proxy segment route is not wired and M8.83's prefetch signal is included only after its M8.56 integration; see the M9.10 caveat. This is not proof for transports that do not yet exist in the assembly.
7. `VisibilityLatencyIT` Test passed at 250 ms, 1 s and 5 s; p99 values and RustFS lower-bound limitation are recorded in `m9.11-visibility-latency.md`.
8. `VisibilitySearchableIT` passed its 1-second `clusterTest` point; 537.919 ms is recorded in `m9.11-visibility-latency.md`.
9. `PartitionVisibilityIT` Test passed the M=10 and M=1,000 drain bounds and audited visible durable-intent acknowledgements; see `m9.12-partition-visibility.md`.
10. `m9.6-allocation-gate.md#noise-floor` records the `:bench:jmh` allocation gate and ten-fork noise-floor evidence; this is a leading indicator, not a new NFR-6 memory-flatness proof.
11. `codec.csv#M9.7` and `m9.7-codec-comparison.md` record B3 codec/block measurements and the generated curve; the M9.7 run and repository gates passed.
12. `DirectThresholdIT`, `m9.14-direct-threshold.md` and ADR-0067 establish the RustFS request-cost threshold at fan-out 1. S3 TTFB, the decisive latency term, is NOT-RUN.
13. `KillNodeMidBacklogIT` is the M8.24h evidence for the completed parent; the M8.24a–h rows record the protocol, server, consumer and T4 results. See their detailed evidence in the backlog. The separate full nightly workflow remains NOT-RUN.
14. `GapRereadIT#gapReplayIsContiguousAndCostsOneGetPerUnindexedSegment` (M9.13) measures the reachable-ingester gap reread through catch-up and does not claim to price the no-ingester Tiers 2/3 path.
15. PARTIAL: inherited evidence is enumerated below. M8.24, M8.56, M8.58 and M8.60–M8.81 are complete except M8.62f, whose cleanup-diagnostic test is specified but not yet implemented; the other 16 source commits map to the listed test/harvest evidence or the explicitly non-functional pool-cap disposition. M8.73/.78 children M8.76/.77 and M8.79/.80 are done; `DurableSignalRingOwnerTest#multipleReadyCandidatesInOneAzWarmOnlyTheDeterministicRingOwner` now closes M8.85. M8.64 is prose-only and excluded from automated criterion-line coverage by the spec; its review disposition is recorded in the backlog row.
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
| M8.62 | **OPEN pending M8.62f.** Source mapping: 15d0277 → M8.62a–d; 42738fa → M8.62e; a85e95c → `ChainGcAcrossEpochsTest`/`PointedDeltaPinnedTest`; f83e841 → `SubscriptionByteBudgetTest`; 5ebac33 → M8.67; c2b5a8b → M8.22 presign tests; 0514a53 → `CommitRequestTest#theCopiedRecordCountsAreImmutableNotMerelyUnaliased` (the mutable-map substitution was observed red); 252a1a8 → M8.69; 001b63d → M8.62f; a299c24 → M8.29; 7760f40/08cb808/a8a2df2 → M8.71; 21703bc → `ClusterCaTrustTest` and M8.60; 6824988 → M8.75; f78eb96 → M8.74. Its `MAX_POOLED_CLIENTS` numeric mutation is equivalent for the stated bounded-pool behavior: every positive fixed limit still bounds the pool, so a changed internal default is not a functional survivor. d5fc36b → M8.68 callback/shutdown classification tests. |
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
| M9.42 | `NodeLocalStoreReaderKeyPolicyTest`, `NodeLocalStoreReaderTest`, `NodeLocalStoreReaderMainTest`, `NodeLocalStoreReaderProcessIT` including `--init-secret`, and POSIX secret-permission regression; `gates`, TDD, test-integrity, and diff-scoped mutation (111/138, 80.4%) passed. `check-coverage.sh` measured coverage below repository floors; Gradle `checkCoverage` could not parse missing JaCoCo `report.dtd`. Cost-meter gate is absent. M9.44–M9.45 and M9.21 remain open. |
| M9.43 | `TierTwoChainPollTest` with 16 distinct node subscriptions, identical and delayed cursors, repeated 404s, newly available deltas, one-time bounded handoff, no read after the last subscription closes, and rejection of a wrong-sequence delta; `TierTwoReleaseRaceTest` verifies a concurrent final release waits for an admitted read and prevents subsequent reads. Loopback plugin wiring asserts the canonical delta key and decoded delta. `CatchUpSchedulingWiringTest` asserts the node schedule invokes Tier 2. `:client:test :plugin:test` passed; repository gates passed; diff-scoped mutation killed 66/66 scorable mutants (100%). Automatic Tier 2 has one GET per node per 5 s interval only while subscriptions remain active, the cursor is unchanged, and no ingester answers; zero LIST/STAT capability and zero requests while idle or while an ingester answers. Cost-meter gate is absent. |

## Milestone review at this checkpoint

### Findings

1. **The fetch-cost and cross-AZ results are bounded by currently wired routes.** The M9.10 narrative correctly excludes proxy segment bytes because no production proxy segment route exists. The M8.24/M9.13/M9.21 work remains outstanding, so the cost curve does not yet cover those recovery paths or claim their request costs.
2. **The tier-2/3 decision exists without execution.** ADR-0064 makes M9.21 implementable; M8.24 is a prerequisite and `LadderStoreTiersIT` is absent. Do not report criterion 18 complete until the actual outage/gap paths and request counts execute.
3. **FR-21's refusing half remains unowned after M9.** M9 explicitly excludes the production governor and delivers only counting. The next milestone needs an ownership decision and refusal design before production can be protected from a cost-budget breach.
4. **Recorded minor findings carried forward:** M9.15-R4/T1 (duplicate size-rate rows accepted) and M9.15-T2 (zero GET pricing lacks an independent mutation pin); M9.14's chaos poll uses `Thread.sleep(25)`; M8.19 noted the RustFS commit-protocol wording distinction; inherited M8 commit-body minors include test expectations derived from production constants and the unused-baseline inference. These belong in the next milestone's review-harvest work, not as silent claims of closure here.

### Re-plan

M9 is not complete. Resume with the next unblocked task from the current backlog, while preserving the dependency order: finish M8.24 before M9.13 and M9.21; then execute the M9.21 no-ingester tiers and M9.13 reachable-ingester gap reread, and finish M8.58 plus the remaining M8.60–M8.80 harvest rows. The full M9.8/M9.56 profile has passed locally in 26m; the separate nightly workflow remains NOT-RUN. The next milestone must own FR-21's refusing half and the recorded review-harvest items. M9.22's roadmap amendment remains accurate: AWS S3 tail latency, M3 TTFB and billed dollars are NOT-RUN on this rig.

No new research conclusion overturns the existing corpus. The checkpoint reinforces its distinction between backend-independent request counts and RustFS-only latency; the timeout is an execution-layer limitation, not evidence to change a cost threshold.
