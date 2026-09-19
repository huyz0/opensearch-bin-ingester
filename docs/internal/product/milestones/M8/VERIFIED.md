<!-- SPDX-License-Identifier: Apache-2.0 -->
# M8 — verified

One line per acceptance criterion in [SPEC.md](SPEC.md), naming what
demonstrated it. ⚠️ **It forces enumeration; it cannot make the evidence true**
— AGENTS.md non-negotiable 4, and nothing checks it.

What was run on this tree, 2026-09-20: `./gradlew build` green at **2,126
tests, 0 failures, over 271 suites** (before M8.59, which changed integration
tests only); `./gradlew :server:integrationTest` over every class, with three
red classes (`AssembledGcCostIT`, `ClockSkewIT`, `KillSequencerMidCommitIT`)
fixed by M8.59 and each re-run green on its own; `./gradlew :plugin:clusterTest`
green over all 12 classes; `./scripts/check-wired.sh` and
`GATE_SCOPE=full ./scripts/check-module.sh` green. ⚠️ **The result directories
have since been overwritten by targeted re-runs, so these counts are the runs
as observed, not a sum re-derivable from `build/test-results` as it stands** —
re-derive them with a fresh run.

⚠️ **ONE LINE IS NOT EVIDENCE**: criterion 18 is deferred to M9 by ADR-0057
and `NOT-RUN`. Criterion 17 is met in part (see its line). The idle soak
(criterion 3) was re-run on this tree for this document, because the inbox and
the backfill gate both changed the assembled pod after its last run.
Criterion 11 is met under the ADR-0058 amendment (a deferred write is acked on
a durable intent), not as the spec first wrote it.

1. **The assembled process accepts a write and a consumer reads it back** — `AssembledWriteReadIT#theASSEMBLEDProcessACCEPTSAWriteAndAConsumerREADSItBackFromTheBUCKET`, against MinIO.
2. **Nothing constructs a store, a clock or a socket outside the composition root** — `ModuleGateTest#onlyTheCompositionRootMayDependOnABackend` and `GATE_SCOPE=full ./scripts/check-module.sh` (M8.29), with `check-io-seam.sh` on every commit.
3. **An idle assembled pod issues exactly its lease cost** — `IdlePodCostSoakTest#anIDLEPodOwesOnlyITSLeaseAtOneSubscriberAndAtATHOUSAND`, run by `./gradlew soakTest --rerun-tasks` on this tree after M8.59 (302 s, 0 failures): over 5 minutes idle, 1 subscriber cost 100 lease requests and 0 other, and 1,000 subscribers the same 100 and 0.
4. **The GC loop runs in the assembled process at zero LIST per pass** — `AssembledGcCostIT#TENPassesOnTheREALTimerWithNothingExpiredCostZEROListGetAndDelete`, red in the full run and green after M8.59 (a first term no longer backfills).
5. **Graceful shutdown in research 08 §7's order** — `ShutdownOrderTest#theSTEPSRunInSECTION7sOrder`, `#aFAILEDFlushStillRELEASESTheLease`, and over a real SIGTERM `ShutdownDrainIT#aSIGTERMDrainsInSECTION7sOrderWithinTheBUDGETAndLosesNOTHING`.
6. **RPO 0: a kill mid-flush loses no acked record** — `KillMidFlushIT#aSIGKILLMidFlushLOSESNoAckedRecord`.
7. **Kill after PUT, before commit: the orphan is swept** — `OrphanAfterKillIT#anORPHANLeftByAKillIsSWEPTTheRETRYLandsAndNOTHINGCommittedIsLost`, on `local-fs` with a 3-hour skew because MinIO refuses more than 15 minutes of signature skew.
8. **Kill the sequencer mid-commit: I1–I5 hold, every seal succeeds** — `KillSequencerMidCommitIT#killingTheSEQUENCERMidCommitKEEPSI1ToI5AndEverySEALSucceeds`: 3 kills, 3 seals, 23,750 acked and all committed, on the re-run after M8.59.
9. **SIGSTOP on a sequencer: fenced, and it commits nothing on waking** — `StoppedSequencerIT#aSTOPPEDSequencerIsReplacedWithinTheTTLAndCommitsNOTHINGWhenItWAKES` and `ChallengeResumeIT#aPAUSEDLeaderLOSESItsTermAndOnRESUMECommitsNOTHINGUnderIt`.
10. **NFR-9: visibility resumes in under 5 s** — `EarlyChallengeIT#withTheWATCHVisibilityResumesInUNDER5sBeforeTheOldLeaseCOULDHaveExpired` with its control `#withoutTheWATCHTheSameKillWaitsOUTTheTTL`; the forwarding pause measured 2,133 ms in `ChallengeResumeIT` after M8.57.
11. **An AZ partition engages the inbox and the heal applies every intent once** — `AzPartitionIT#aPARTITIONEDPodDEFERSToTheInboxAndTheHEALAppliesEveryIntentONCE` (as amended by ADR-0058), with the ordering cases `InboxDrainTest#aDEFERRINGPodForwardsNothingPastItsIntentsAndEVERYFlushLandsONCE` and `InboxDrainRaceTest#TWOConcurrentDrainsThroughABATCHINGTermCommitEACHIntentONCE`.
12. **A pod partitioned from the store fails readiness and stops acking** — `StorePartitionIT#aStoreONLYPartitionFAILSReadinessAndSTOPSTheAcks`.
13. **A rolling restart leaves no visibility gap over 1 s** — `RollingRestartIT#aROLLINGRestartKeepsVISIBILITYAndSPREADSTheReconnects`.
14. **±5 minutes of clock skew leaves safety unaffected** — `ClockSkewIT#aSLOWPodCannotKEEPTheTermAndNOTHINGIsLostOrReassigned`, `#aLIVEFastPodTakesTheTermONCEAndHoldsItAndNOTHINGIsLost`, and `#aFASTLeaderWhoDIESHoldsTheTermPASTItsTTLAndEveryAckMeanwhileIsANDURABLEIntent` (renamed by M8.59: those acks are now intents, not refusals).
15. **The S3 backend passes store conformance and the presign obligations** — `S3BinStoreConformanceIT` (the conformance suite, inherited cases), `S3PresignTest#aSIGNINGFailureCarriesNEITHERTheCredentialNORAUrl`, `S3RetryScopeIT#aCONDITIONALWriteIsTriedONCEAndAGETIsRetried`.
16. **Every unwired mechanism is wired or re-deferred with an owner** — `./scripts/check-wired.sh`: M5.6e, M5.91a, M5.91c, M6.15, M6.19, M7.17, M7.18, M7.21n, M7.24, M7.25, M7.26 WIRED; M5.91b (the segment prefetcher) OWNED by M8.56, deferred to M9.
17. **The inbox write rate is bounded, and it returns to normal after the heal** — `InboxTest#aRETRIEDFlushWritesNoSECONDIntent`, `#anUNREACHABLESequencerMeansONEIntentAndADEFERREDCommit`, and `AzPartitionIT#aPARTITIONEDPodDEFERSToTheInboxAndTheHEALAppliesEveryIntentONCE` (an intent spans indices; a flush after the heal writes none). ⚠️ MET IN PART: the bound of one intent PUT per pod per flush is pinned in-process by `InboxTest` only, not counted at the store, and `AzPartitionIT` would stay green with one intent per record — carried as M8.61.
18. **Killing a node mid-backlog, with no tail starvation** — NOT-RUN: deferred to M9 by ADR-0057, carried as M8.24.
19. **Measurement M1: the lease TTL survives realistic pauses** — `LeaseTtlMeasurementIT#theSHIPPEDTtlTakesOverONLYForAPauseItCannotAbsorb`, with `ChallengeResumeIT#aPAUSEDLeaderLOSESItsTermAndOnRESUMECommitsNOTHINGUnderIt` for the pause past the TTL.
20. **The fallback ladder executes (tiers 0 and 1, per ADR-0057)** — `LadderExecutionTest#aCONNECTIONLostAndRegainedRunsTheLadderPUSHThenRECONNECTThenPUSH`, `#ONESubscriptionDownOnASHAREDTransportIsTheNodesTIER`.
21. **Configuration is parsed once and a bad one fails at start** — `ConfigFileTest#aMISSINGFileIsREFUSEDWithThePATHInTheMessage` and `ConfigExitCodeIT#theTHREEExitCodesArePAIRWISEDistinctAndNONEOfThemIsZERO`.
22. **A consumer at a collected position is refused, from a real GC** — `RetentionRefusalTest#aPOSITIONTheNodesOWNGCCollectedIsREFUSEDByAConsumerOverHTTP`.
23. **The assembled fetch path's GETs scale with segments** — `ProductionFetchPathTest#GETsScaleWithSEGMENTSAndDoNotMoveWhenTheSHARDCountDOUBLES`.
24. **The routed path serves a write through the assembled server** — `RoutedWriteTest#aROUTEDWriteLANDSInThePartitionOpenSearchsROUTINGChooses`, with the refusals `#anEXPLICITPartitionOUTSIDETheIndexsRangeIsREFUSEDWith400`.
25. **Commit forwarding works across the fleet** — `PeerCommitTest#aCOMMITCrossesTheSocketAndTheDELTAComesBack`, `#aPEERThatDoesNotHoldTheLeaseREFUSESAndTheCallerMayRESEND`, and over real processes in `LeaseTtlMeasurementIT`, `ChallengeResumeIT` and `AzPartitionIT`.

## The unwired set

One line per entry of [SPEC § *The unwired set*](SPEC.md), as
`./scripts/check-wired.sh` reported it over the full tree on 2026-09-20. The
list itself lives only in the spec; this is the verdict per entry, not a copy
of what each one is.

- M5.6e — WIRED.
- M5.91a — WIRED.
- M5.91b — ⚠️ STILL UNWIRED: nothing constructs `SegmentPrefetcher`. Owned by M8.56, deferred to M9 by the user's decision of 2026-09-20.
- M5.91c — WIRED (`call tierFor`, by M8.28).
- M6.15 — WIRED.
- M6.19 — WIRED (M8.32).
- M7.17 — WIRED.
- M7.18 — WIRED.
- M7.21n — WIRED.
- M7.24 — WIRED.
- M7.25 — WIRED.
- M7.26 — WIRED.
