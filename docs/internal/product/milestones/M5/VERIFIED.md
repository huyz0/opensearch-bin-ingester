# M5 — verified

One line per acceptance criterion in [SPEC.md](SPEC.md), naming what
demonstrated it. ⚠️ **It forces enumeration; it cannot make the evidence true**
— that is AGENTS.md non-negotiable 4, and nothing checks it. Everything below
was run on this tree: `./gradlew build` green at **1367 tests, 0 failures, 0
errors**, and `GATE_SCOPE=full ./scripts/check-module.sh` green at 8 modules.

⚠️ **Three criteria are met only in part, and each says which part.**
Criterion 9 carries the literal `NOT-RUN` (the script's own count includes this
paragraph, which matches its regex too); criteria 3 and 4 are marked not-met and
not-verifiable-as-a-whole in words, because the SPEC itself says so in those
places and forbids marking them verified. A milestone whose evidence document
is uniformly affirmative is the one to distrust.

1. **A non-leaseholder pod commits, and only the leaseholder writes the chain** — `CommitProtocolForwardingTest.aPodThatDoesNotHoldTheLeaseCOMMITS_AndItsRecordsAreInTheChain`
   and `.aForwardIsWRITTENByTheLeaseholder_SoITSPartitionRefusesIt`;
   `ForwardingIngestTest.theFollowerWritesItsOwnSEGMENTButNotTheCHAIN` asserts a
   PUT-count delta of 2 over a SHARED store -- the follower's segment plus the
   LEASEHOLDER's single delta -- which pins that the follower adds no second
   chain write; the "only the leaseholder writes the chain" half rests on
   `.aForwardIsWRITTENByTheLeaseholder_SoITSPartitionRefusesIt`, not on that
   count. I1–I5 over a
   mostly-following fleet:
   `CommitProtocolForwardingSweepTest.theInvariantsHoldOverAFleetWhereMostPodsDoNotHoldTheLease`.
   ⚠️ **The transport is a test fixture**: no production `SequencerTransport`
   exists (M5.6e, owned by M8), so what is verified is the protocol, not a
   deployed hop. `RemoteSequencer`'s own javadoc says so.
2. **A forwarded commit whose reply is lost is answered, not duplicated** — `AmbiguousCommitRetryTest`, `SequencerDedupTest`, and
   `CommitLogAmbiguityTest`; the production-path form is criterion 4's.
3. **And that still holds across a takeover** — `DedupAcrossTakeoverTest.aReplayARRIVINGATTheSuccessorIsAnsweredFromTheChain`
   and `.anAMBIGUOUSLYLandedFlushIsInheritedAcrossATakeover`.
   ⚠️ **NOT MET UNCONDITIONALLY, as the SPEC says in as many words.** M5.34
   enumerates the limits, and two are asserted in the failing direction rather
   than argued away:
   `.aSUPERSEDEDIncarnationsReplayIsAppliedTWICE_AndTheSAMEFixtureAnswersITSOwn`
   and `.aBAREV0CheckpointSlotLeavesItsPodUNPROTECTED_AndThatIsRecorded`.
   Read M5.34 before treating this as closed.
4. **A retry reuses its triple end to end** — `AmbiguousReplyResentTest.aLOSTREPLYIsResentWithTheSameTripleAndTheRecordsCommitONCE`, which
   kills the reply inside `DefaultIngest` and asserts one delta;
   `.aSECONDLostReplyIsNOTResentAgainAndTheFlushIsAbandoned` pins the bound.
   ⚠️ **NOT VERIFIABLE AS A WHOLE, and the SPEC forbids marking it so.** The
   resend covers a reply lost to a sequencer that REMAINS the leaseholder;
   `Sequencer`'s enumeration and M5.52b's incarnation limit are the rest, and
   `RemoteSequencerTest.anAMBIGUOUSFailureIsNOTResent` records the deliberate
   refusal on the forwarding path.
5. **All three fetch modes deliver identical bytes** — `ThreeModesAgreeTest.allTHREEModesDeliverBYTEIdenticalRecords`, which drives all
   three through `ConsumerClient`'s own decode;
   `.aFanOutABOVETheThresholdIsNOTServedDIRECT` is the consumer that asks and is
   served something else, and
   `FetchPolicySeamTest.noPublicENTRYPointIntoTheFetchPolicyTAKESAFetchMode`
   makes asking unrepresentable rather than refused.
6. **`proxy` streams through, memory flat in K** — `SegmentProxyTest.thePeakHandOffAndTheReadCountAreIDENTICALAtKONEAndKSIXTYFOUR`
   at K = 1 and K = 64, with `.noConsumerIsEverHandedMoreThanONECHUNK` as the
   bound independent of K, and
   `AssembledServingPathTest.theServingPathHandsOffNoMoreThanACHUNKAtAnyK`
   through the assembled path.
7. **`direct` issues a short-lived, read-only, single-key grant** — `GrantIssuerTest.theGrantIsForEXACTLYTheKeyRequested`,
   `.bothCeilingsAreADR0010sSIXTYSECONDS`, `.aCeilingBeyondTheMAXIMUMIsREFUSED`;
   `DirectEnablementTest.aPodThatWantsDIRECTOnABackendThatCannotSIGNRefusesToSTART`;
   `GrantNeverLoggedTest.issuingAGrantWritesNOTHINGContainingTheSignature`,
   which captures output while one is issued, and
   `.aSigningFailureNamesTheKEYAndNeverTheGRANT`.
8. **Zero requests attributable to idle consumers, at fan-out** — `IdleConsumerCostTest.aTHOUSANDIdleConsumersOverTHREETHOUSANDIntervalsCostZEROOfBothNumbers`:
   1,000 consumers, 3,000 intervals of an injected clock, zero through
   `CountingBinStore` and zero grants. The recorded red is against a timer that
   fires on an empty stream; the class javadoc names the two sites it takes.
   ⚠️ **The clock is read ZERO times on the idle path and that is what is
   asserted** — `isFlushDue` returns at its empty guard before reading it, so a
   polling design (one read per interval) fails the case. The "clock is
   exercised" half is
   `.anIDLEFanOutAddsNOTHINGToAnACTIVEStreamsRequestCountOrItsDeliveries`, whose
   driver asserts production read it every round.
9. **Prefetch is per segment per AZ** — `SegmentPrefetchTest.threeAZsCostTWOGetsWhateverTheNodeCount`: three AZs,
   TWO GETs, at 1, 3 and 30 pods per AZ, counted over the whole run.
   ⚠️ **NOT-RUN: the consumer-count half.** The criterion asks for 300 and 600
   consumers; the cases vary PODS, not consumers. `SegmentPrefetcher` takes no
   consumer count and no subscriber list — it decides from the membership ring
   alone — so consumer-independence holds by construction there and is
   therefore exactly the shape M1.16 was withdrawn for. Recorded as **M5.88**
   rather than claimed here.
10. **Cross-AZ peer fetch is not addressable** — `MembershipSeamTest.theSeamDeclaresNOMethodThatTakesAnAZ` and
    `PeerRingTest.aMIXEDAZViewCannotBeBuilt`, `.anAZViewCannotBeWIDENEDAfterItIsBuilt`:
    enforced by the types, which is what ADR-0012 asks for.
11. **A session resumes exactly, and a reset re-sends full state** — `SessionResumeTest.aRESUMEAnswersONEPastTheLastOffsetDELIVEREDOnEveryStreamItKeeps`,
    `.aRETRIEDResumeAtTheSameEpochIsANSWEREDNotREAPPLIED`,
    `.aConsumerThatTOOKAPREFIXDoesNotAdvanceItsResumePoint`;
    `SessionResetTest.aResumeOnAResetSessionSaysRESENDFULLSTATE` and
    `.reESTABLISHINGFullStateDeliversEachRunONCE`.
12. **The subscription epoch is not the lease epoch** — `SessionResetTest.aSEQUENCERFailoverDoesNOTResetASession` and
    `.aRESETNamesASESSIONAndNOTHINGAboutTheSEQUENCER`, with
    `SequencerEpochOnThePushTest.theOFFSETAndTheEPOCHAreDistinctFieldsWithDistinctValues`.
13. **The ladder's tier 4 never runs on a hot path** — `FallbackLadderTest.theAUTOMATICLadderHasNoTierThatLISTs` (no automatic tier
    can LIST, by the return type),
    `.threeHUNDREDDegradedNodesIssueZEROListsAndAFlatGetRate` at 300 nodes, and
    `.aSCANIsNOTREACHABLEWithoutAnExplicitRecoveryAction`.
    ⚠️ **The 300 nodes are simulated as policy calls, not as running plugins**,
    and nothing in the tree executes a tier: `FallbackLadder` performs no I/O
    and nothing constructs it (ADR-0044 (a)). What is verified is that no
    automatic path can name a LIST, not that a degraded fleet was observed.
14. **No Helidon in `sequencer`/`client`/`plugin`, no `binjava.binstore` in `client`/`plugin`** — `GATE_SCOPE=full ./scripts/check-module.sh` green (8 modules, rule5 tested
    4, rule2 tested 2), and `grep -rn '^import binjava.binstore' client/src/main
    plugin/src/main` returns nothing. `DirectFetchTest` and `SegmentSource` are
    the seam that keeps it that way.

## What M5 does NOT close

⚠️ **A multi-pod deployment is still not correct in production.** M5 ships the
commit-forwarding protocol and every test of it runs over a test
`SequencerTransport`; no production one exists, and **M5.6e** (owned by M8) is
where it lands. Any sentence in this repository saying M5 closes the multi-pod
hole means the protocol, not the deployment.

⚠️ **`direct` ships disabled and has no production fetcher** — ADR-0044 (a).
Neither shipping backend can presign, so the mode is exercised by fixtures
only; `NodeSegmentSource` (M5.45h) holds the one-fetch-per-(node, segment)
property for when one arrives.

⚠️ **The fallback ladder is a policy, not an execution.** Tiers 1-4 need a
reconnecting transport and a consumer-side reader; neither is in M5.

⚠️ **Prefetch has no trigger.** `SegmentPrefetcher` is constructed by nothing:
the durability broadcast is M8's (M5.6e).

Open rows carried forward, each with its own backlog entry: M5.6e, M5.6g,
M5.6h, M5.6i, M5.6j (⚠️ **not M5.6f, which is done** -- an earlier draft wrote
`M5.6e-M5.6j` inside the very sentence claiming to enumerate),
M5.17 (M8), M5.42 (⚠️ **M8 SINCE [ADR-0052](../../decisions/0052-m8-owns-assembly-and-the-first-real-backend-because-its-evidence-is-unbuyable-without-them.md)**, which gave the first real backend to M8; this said M9 while the backend itself had no milestone), M5.58b, M5.63, M5.64, M5.66, M5.67, M5.68, M5.71-M5.85,
M5.88 and M5.89. ⚠️ Enumerated from the file rather than as a range: M5.86 and M5.87
have no rows, and an earlier draft of this line invented them by writing
"M5.71-M5.88".
