# M9 evidence and checkpoint review

This is a source-tree audit, not a claim that M9 is complete. The enumeration
gate checks only that each numbered row exists and names evidence; the review
below checks the claims against the current tree. NOT-RUN and OBSERVED-NOT are
intentional results where the acceptance condition is outstanding or absent.

## Acceptance criteria

1. `check-mutants.sh` is wired through `gates` and `checkMutants`; M0.14's checker tests pin seeded-mutant behavior. The full mutation suite was not rerun for this checkpoint.
2. `CostMeterTest` and its T0 arithmetic tests cover requests/MiB, idle rate and price-table dollars; `:binstore-spi:test` evidence is in the M9.1 backlog row.
3. PARTIAL: `WriteRequestRateIT` 60-second fast point passed (M9.16); the 5-second smoke CSV is not the required five-minute measurement. The attempted full profile was interrupted at Gradle's 10-minute `integrationTest` timeout, so the ≥5-minute points and complete low-rate profile are NOT-RUN to completion.
4. `IdlePodCostSoakTest` passed locally (301.862 s, recorded under M8.75); the measurement workflow statically asserts the nightly soak job through `check-measurement-workflow.py`.
5. `ReadRequestRateIT` Test measured the current M8.56 prefetch path; see `m9.9-read-request-rate.md` and the M9.9 backlog evidence. Its result does not close the separate inherited catch-up and fallback work.
6. PARTIAL: `CrossAzBytesIT` Test measured 0.0598% on its named current-build transports (M9.10). The proxy segment route is not wired and M8.83's prefetch signal is included only after its M8.56 integration; see the M9.10 caveat. This is not proof for transports that do not yet exist in the assembly.
7. `VisibilityLatencyIT` Test passed at 250 ms, 1 s and 5 s; p99 values and RustFS lower-bound limitation are recorded in `m9.11-visibility-latency.md`.
8. `VisibilitySearchableIT` passed its 1-second `clusterTest` point; 537.919 ms is recorded in `m9.11-visibility-latency.md`.
9. `PartitionVisibilityIT` Test passed the M=10 and M=1,000 drain bounds and audited visible durable-intent acknowledgements; see `m9.12-partition-visibility.md`.
10. `m9.6-allocation-gate.md#noise-floor` records the `:bench:jmh` allocation gate and ten-fork noise-floor evidence; this is a leading indicator, not a new NFR-6 memory-flatness proof.
11. `codec.csv#M9.7` and `m9.7-codec-comparison.md` record B3 codec/block measurements and the generated curve; the M9.7 run and repository gates passed.
12. `DirectThresholdIT`, `m9.14-direct-threshold.md` and ADR-0067 establish the RustFS request-cost threshold at fan-out 1. S3 TTFB, the decisive latency term, is NOT-RUN.
13. NOT-RUN: M8.24's `KillNodeMidBacklogIT` is absent; M8.24 parent remains todo after the protocol and component rows M8.24a-e. No catch-up starvation or T4 result is claimed.
14. NOT-RUN: M9.13 `GapRereadIT` is absent and depends on unfinished M8.24. No reachable-ingester gap reread cost is claimed.
15. PARTIAL: inherited-row evidence/status is enumerated below. The M8.60–64, M8.74, M8.77 and M8.80 rows remain open; M8.58 and M8.24 parent remain open (NOT-RUN). M8.64 is prose-only and explicitly excluded from automated criterion-line coverage by the spec; it is still an open review action, not silently treated as tested.
16. `gradlew generateCostLatencyCurve` generated the committed Markdown/SVG from `measurements/results`; `checkCostLatencyCurve` is wired into `gates`. The checked-in inputs explicitly mark M9.8 and M9.18 as smoke, not full acceptance runs.
17. `measurement.yml`, the 60-second L1 subset and the `nightly-measurement` execution-layer docs are checked by `checkMeasurementWorkflow`. The full nightly GitHub workflow is NOT-RUN here; the separate full local profile timed out at 10 minutes.
18. NOT-RUN: ADR-0064 selects `NodeLocalStoreReader`, but M9.21 `LadderStoreTiersIT` and M8.24 are unfinished. Tiers 2 and 3 have not executed; their GET/STAT cost is not measured, and zero automatic-tier LISTs remain to be proven.
19. NOT-RUN: M8.58's pod-UID lease wire-format change is absent; no lease format, readers/writers, fake or golden change is claimed.

**Explicit scope disposition:** FR-21's refusing half is owned by no milestone
after M9. M9 delivers request counting only; it does not build a production
cost governor that refuses writes.

## Inherited rows

Evidence references below point to the named backlog task's acceptance evidence and commit unless marked NOT-RUN. Statuses are transcribed from `docs/internal/product/backlog.md` at this checkpoint.

| Row | Evidence / disposition |
|---|---|
| M8.24a | `ADR-0065`; protocol decision done. |
| M8.24b | Bounded replay source/coordinator component tests; done, parent integration remains open. |
| M8.24c | Versioned catch-up frames, golden and refusal tests; done, HTTP registration remains open. |
| M8.24d | HTTP catch-up control seam tests; done, production replay wiring remains open. |
| M8.24e | Durable responder tests; done, HTTP registration/T4 remain open. |
| M8.24 | NOT-RUN: `KillNodeMidBacklogIT` absent; parent todo. |
| M8.56 | `AssemblyBatchingTest`/prefetch assembly and real-RustFS peer-hint proof (M8.84/M8.85); done. |
| M8.58 | NOT-RUN: pod UID is not yet in the lease; wire-format task todo. |
| M8.60 | NOT-RUN: bounded-label runtime counters are not exported; todo. |
| M8.61 | NOT-RUN: all ten enumerated chaos assertion mutations are not closed; todo. |
| M8.62 | NOT-RUN: inherited surviving mutants remain; todo. |
| M8.63 | NOT-RUN: inherited `Thread.sleep` polling/costly LIST loops remain; todo. |
| M8.64 | OBSERVED-NOT: stale prose sweep is still todo; prose truth is reviewed, not asserted by a test. |
| M8.65 | `InboxDrainRaceTest#aLocalAndBatchedDrainShareTheInnerTermLock`; done. |
| M8.66 | `ChainBackfillTest` deposition/all-or-nothing cases, assembly wiring test, and cost.md budget; done. |
| M8.67 | `NodeSegmentSourcePerKeyLockTest` proves independent keys progress and same-key fetch coalescing; done. |
| M8.68 | `PollOutcomeCountTest` callback-failure classifications; done. |
| M8.69 | `ServerPropertiesTest` blank duration/bytes/boolean refusal cases; done. |
| M8.70 | `NodeShutdownTest#aDRAINDelaysBulkRefusalAfterReadinessFails`; done. |
| M8.71 | `NodeProcessKillResumesPausedTest`, `RetryFloorCallerTest`, and assembly/positions ownership tests; done. |
| M8.72 | `PluginLoggingIT` captures all three paths in OpenSearch Log4j; done. |
| M8.73 | Split; M8.76 done, M8.77 remains todo (see both split rows). |
| M8.74 | NOT-RUN: `BodyTooLargeException` response attribution remains todo. |
| M8.75 | `IdlePodCostSoakTest` passed locally and `check-measurement-workflow.py` pins nightly `soakTest`; done. |
| M8.76 | JVM and Python `check-wired` resolution tests; done. |
| M8.77 | NOT-RUN: qualified nested construction case remains todo. |
| M8.78 | Split; M8.79 done, M8.80 remains todo (see both split rows). |
| M8.79 | `test_tdd_scan.py` and `test_tdd_red.py` Windows path/selector tests; done. |
| M8.80 | NOT-RUN: stale JUnit cleanup failure refusal remains todo. |
| M8.81 | `test_source_digest_uses_canonical_git_line_endings` plus `checkTdd`; done. |
| M8.82 | `EndpointSliceViewTest` ready AZ-labelled peer snapshot; done. |
| M8.83 | BPDS versioned frame golden/authentication/byte-count component tests; done, integrated signal use is M8.56. |

## Milestone review at this checkpoint

### Findings

1. **NFR-1's full acceptance data is still smoke-only.** The committed size-regime CSV is 5 seconds per rate and the low-rate CSV is 3.07/5.05 seconds, while criterion 3 requires five minutes per point. The full local invocation exceeded Gradle's 10-minute integration-test task ceiling before completing; it must not be turned into acceptance data. Keep the existing numbers labelled smoke and run the nightly profile in an execution environment whose per-task budget covers it.
2. **The fetch-cost and cross-AZ results are bounded by currently wired routes.** The M9.10 narrative correctly excludes proxy segment bytes because no production proxy segment route exists. The M8.24/M9.13/M9.21 work remains outstanding, so the cost curve does not yet cover those recovery paths or claim their request costs.
3. **The tier-2/3 decision exists without execution.** ADR-0064 makes M9.21 implementable; M8.24 is a prerequisite and `LadderStoreTiersIT` is absent. Do not report criterion 18 complete until the actual outage/gap paths and request counts execute.
4. **M9.16 reduced validation turnaround but did not complete the named nightly run.** Its 60-second 40 MiB/s subset passed and the workflow is statically checked; a local profile attempt timed out. Keep the complete workflow result NOT-RUN until a full scheduled/manual run finishes and produces non-skipped reports.
5. **FR-21's refusing half remains unowned after M9.** M9 explicitly excludes the production governor and delivers only counting. The next milestone needs an ownership decision and refusal design before production can be protected from a cost-budget breach.
6. **Recorded minor findings carried forward:** M9.15-R4/T1 (duplicate size-rate rows accepted) and M9.15-T2 (zero GET pricing lacks an independent mutation pin); M9.14's chaos poll uses `Thread.sleep(25)`; M8.19 noted the RustFS commit-protocol wording distinction; inherited M8 commit-body minors include test expectations derived from production constants and the unused-baseline inference. These belong in the next milestone's review-harvest work, not as silent claims of closure here.

### Re-plan

M9 is not complete. Resume with the next unblocked task from the current backlog, while preserving the dependency order: finish M8.24 before M9.13 and M9.21; then execute the M9.21 no-ingester tiers and M9.13 reachable-ingester gap reread, and finish M8.58 plus the remaining M8.60–M8.80 harvest rows. In parallel, give the full M9.8/M9.18 profile a named run budget that can finish rather than weakening thresholds or shortening criterion 3. The next milestone must own FR-21's refusing half and the recorded review-harvest items. M9.22's roadmap amendment remains accurate: AWS S3 tail latency, M3 TTFB and billed dollars are NOT-RUN on this rig.

No new research conclusion overturns the existing corpus. The checkpoint reinforces its distinction between backend-independent request counts and RustFS-only latency; the timeout is an execution-layer limitation, not evidence to change a cost threshold.
