<!-- SPDX-License-Identifier: Apache-2.0 -->
# M7 — verified

One line per acceptance criterion in [SPEC.md](SPEC.md), naming what
demonstrated it. ⚠️ **It forces enumeration; it cannot make the evidence true**
— AGENTS.md non-negotiable 4, and nothing checks it. Everything below was run
on this tree: `./gradlew build --rerun-tasks` green at **1,735 tests, 0
failures, 0 errors** over **198 suites** (`plugin:clusterTest`'s 16 are separate
and are NOT run by `build`), summed from `*/build/test-results/test/TEST-*.xml`
written 2026-09-18 — ⚠️ **re-derive it with a fresh `./gradlew build` rather
than from the directory as you find it**, because a reviewer's task-filtered
mutation run overwrites one module's results and the sum then reads low, `./gradlew :plugin:clusterTest --rerun` green,
`GATE_SCOPE=full ./scripts/check-module.sh` green at 8 modules, and
`GATE_SCOPE=full ./scripts/check-metric-cardinality.sh` green over the tree's
433 Java files. ⚠️ The gate PRINTS a tracked-file count beside its result; that
number is not what it examined, and quoting it here is how the first draft of
this line came to state a stale one.

⚠️ **THIS IS A RETENTION MILESTONE, SO EVERY LINE SAYS WHAT WAS DELETED AND
WHAT WAS KEPT.** A retention milestone whose evidence is uniformly affirmative
is the one to distrust: the plausible wrong implementation here deletes
promptly and passes every delete-case, and it is the KEEP-cases that red it.
Each line below therefore names its keep sibling, not only its delete.

⚠️ **AND SEVERAL THINGS ARE NOT WHAT THE SPEC ASSUMED**, each stated in its own
line rather than in a footnote: criterion 18's interval is a **floor, not a
figure** (`OBSERVED-NOT` for the production-rate number); criterion 6's
reporter has **no production source of positions**, is unwired (M7.17), and
**two of its clauses are `OBSERVED-NOT`**; criterion 12's refusal is asserted
against a **hand-fed floor** rather than reached from a GC pass; criterion 14's
43,200-key figure is **arithmetic, not an assertion**; and criterion 17's
cardinality gate is green **partly by accident**, because
`check-metric-cardinality.sh` does not see a label map (M7.20).

⚠️ **SIX OF THESE LINES WERE CORRECTED BY THE M7 MILESTONE REVIEW, WHICH READ
THIS DOCUMENT AGAINST THE TREE RATHER THAN AGAINST THE SPEC.** Each correction
is stated where the line is, with what the earlier draft claimed. That is what
`check-milestone-verified.sh` cannot do: it checks that a line EXISTS and names
a test, never that the test asserts what the line says.

1. **A progress frame round-trips and refuses malformed input** — `ConsumerProgressTest` —
   20 cases. `aBATCHAcrossTWOIndicesRoundTrips` carries 3 entries across 2
   indices, and the refusals are
   `aTRUNCATEDFrameIsREFUSED`, `aFORWARDVersionIsREFUSEDAndTheMessageNamesIt`,
   `aWRONGMagicIsREFUSED`,
   `anENTRYCountBiggerThanTheFrameIsREFUSEDBeforeItAllocates` (refused BEFORE
   it allocates, so a hostile count is not an OOM) and
   `aFrameWithTRAILINGBytesIsREFUSED`. The bytes are pinned by
   `GoldenConsumerProgressTest` against `golden/consumer-progress-v1.bin` and
   `golden/consumer-progress-zero-v1.bin`, in both directions —
   `aBATCHEncodesToItsStoredBytes` and `aSTOREDBatchStillDecodesToWhatItMeant`.
2. **The watermark is the minimum across copies** — `WatermarkTableTest#theMINIMUMAcrossCopiesIsTheAnswer` —
   three copies at 100, 90 and 300 answer **90**;
   `#aDEREGISTEREDCopyLeavesTheOthersBehind` (→ 100), and the backwards half:
   `#aCopyThatREPORTSBackwardsIsBELIEVED` (300 → 150 answers 100, not 150) with
   `#aREGRESSINGCopyHoldingTheMINIMUMDragsTheAnswerDOWN` (90 → 40 answers 40) as
   the case that separates the latest-report rule from a per-copy `max()`.
3. **A silent copy freezes the watermark rather than being dropped** — `WatermarkTableTest#aSILENTCopyFREEZESTheMinimumRatherThanLeavingIt` —
   the 90 copy goes quiet while the others reach 500 and the answer stays **90**. The
   KEEP that follows from it is
   `RetentionRuleTest#aSegmentPASTTheFloorButABOVETheWatermarkIsKEPT`.
   ⚠️ **AN EARLIER DRAFT OF THIS LINE CITED `#aStreamNOBODYHasReportedOnKEEPS`,
   WHICH IS THE WRONG CASE** (found by the M7 milestone review): that one tests
   an UNKNOWN watermark, not a frozen one, and the two are different facts.
4. **A silent copy past `reportTimeout` makes the table unfresh, and GC keeps** — `WatermarkTableTest#oneSILENTCopyMakesTheTableUNFRESHWhileTheOthersReport` —
   the mixed fixture: one stale, two reporting in this interval, so
   freshness-from-the-newest is red, with
   `#aTableWhoseEveryCopyReportedIsFRESH` as the control, and
   `RetentionRuleTest#anUNFRESHWatermarkKEEPSWhateverItsValue` for the GC half.
   Only the ceiling still fires: `#theCEILINGFiresEvenWhenTheTableIsUNFRESH`.
5. **A relocation does not retire the old copy** — `WatermarkTableTest#aNEWAllocationIdDoesNOTRetireTheOldOne` —
   and **both retirement paths asserted to FIRE**:
   `#aCopySilentPastTheEXPIRYIsRETIRED` and
   `#deregisteringTheLASTCopyMakesTheStreamUNKNOWNAgain`. The configuration that
   would turn the outage budget off is refused at construction —
   `#anEXPIRYNoLongerThanTheRETENTIONFloorIsREFUSED`.
6. **The plugin pushes progress for every partition on the node, batched, at zero store cost, and each entry carries that shard's consumed position** — `ProgressReportTest#ONEFrameCarriesEVERYPartitionOnTheNode` —
   8 partitions across 2 indices, **ONE** frame, and eight DIFFERENT positions
   asserted exactly, so a reporter sending one number eight times is read and
   not merely counted. `#EACHEntryCarriesTHATShardsOwnPosition` pins per-entry
   identity and `#EVERYIntervalCarriesTheCURRENTPositions` pins that positions
   are read afresh each interval, so a cached list is red.
   `#aNodeHostingNOTHINGSendsNOFrame` is the empty-node case.
   ⚠️ **TWO CLAUSES OF THIS CRITERION ARE `OBSERVED-NOT`** (found by the M7
   milestone review; an earlier draft of this line asserted both as met).
   (a) **THE ZERO-REQUEST CLAUSE IS NOT MEASURED BY `CountingBinStore`** — the
   class appears nowhere in `ProgressReportTest`. What exists is
   `#theReporterHoldsNOClockNOThreadNOTimerAndNOStore`, a reflection check that
   no field type is a store: a structural proxy, not a recorded zero.
   (b) **"AT LEAST ONE POSITION BEHIND THE TAIL" HAS NO FIXTURE** — no test
   here has any notion of a stream tail, because `ProgressReporter` FORWARDS
   what `Positions` hands it and computes nothing. The tail-sending mutation
   therefore lives in the `Positions` implementation, which does not exist
   (M7.17): a constant-sender IS red here, a tail-sender would not be. Both
   clauses are M7.21.
   ⚠️ **AND IT IS NOT WIRED INTO A NODE — M7.17, carried.** `Positions` has no
   production implementation, so on a real cluster nothing reports and every
   watermark is UNKNOWN, which KEEPS rather than deletes. The direction is safe
   and the criterion is met at the class, not in a deployment.
7. **`Checkpoint` v2 carries the watermark and the older versions still decode** — `CheckpointWatermarkTest#aWATERMARKRoundTrips` —
   with `#MIXEDStreamsKeepTheirOWNWatermarkOrLackOfOne` (absent is not zero:
   `#aStreamWithNOWatermarkCarriesNONERatherThanZero`), with
   `GoldenCheckpointV2Test#aV0CheckpointComesBackWithNOWatermarkAtAll` and
   `#aV1CheckpointComesBackWithNOWatermarkAndKEEPSItsPodPointers` pinning that
   the stored v0 and v1 objects decode to what they decoded to before.
8. **The rule keeps what it must keep** — `RetentionRuleTest`, 19 cases, ten of
   them keeps. (a) `#aFULLYConsumedSegmentINSIDETheTimeFloorIsKEPT`.
   (b) `#aSegmentPASTTheFloorButABOVETheWatermarkIsKEPT`, with the boundary
   `#theBOUNDARYIsTheEndOffsetPLUSTheMargin` and `#theMARGINItselfIsLOADBearing`,
   which both red when `RetentionRule`'s `end + safetyMargin`
   becomes `end` — MEASURED by review: 19 tests, 3 failed, the third being
   `#anOffsetNearOverflowDoesNotWRAPIntoADelete`. (c)
   `#aSegmentPASTTheFloorAndBELOWTheWatermarkIsDELETED`. (d)
   `#aSegmentPASTTheCEILINGIsDELETEDAndALARMS` — asserted as the ALARM firing —
   with `#aSegmentEXACTLYAtTheCeilingDoesNOTAlarmYet` pinning the boundary.
   ⚠️ **THE ALARM FIRES AT VERDICT TIME, NOT AT DELETE TIME** (M7.30(b)): it
   means "data a consumer had not read is gone" and is raised for an object
   that may still be in the bucket if the DELETE then fails. Conservative
   direction — a false page, not a silent loss — and criterion 12's line
   inherits it.
9. **GC from the commit log issues zero LIST and batches its deletes** — `CommitLogGcTest` —
   `#TWOThousandFiveHundredExpiredSegmentsCostZEROListAndTHREEDeletes`:
   `CountingBinStore` records **0 LIST, 0 GET, 0 PUT, 3 DELETE** over 2,500
   expired segments, with `#EXACTLYOneThousandKeysIsONECall` on the batch
   boundary. The keeps: `#aSegmentINSIDETheFloorIsKEPTAndSTILLThere`,
   `#aSegmentWhoseOTHERStreamIsUNREADIsKEPT`,
   `#aSegmentNamedTwiceIsJudgedOnALLOfItsRuns` (one object named by two deltas
   is judged on the union, not on the first mention that says yes) and
    `#aPassWhereEVERYTHINGIsKeptIssuesNODeleteAtAll`.
    ⚠️ **`stat` IS NOT IN THE ASSERTED SET, THOUGH `SegmentGc`'s JAVADOC CLAIMS
    IT** (M7 milestone review, M7.30): the 2,500-segment case asserts lists,
    gets, puts and deletes, and `total()` only on the EMPTY chain — so a
    mutation adding one `stat` per segment, which is exactly the
    proportional-to-collection cost this case exists to refuse, survives.
10. **A delta a checkpoint pod-slot points at is never deleted, and a consumer below `oldestRetainedOffset` is told so** — `PointedDeltaPinnedTest` —
   (a)
    `PointedDeltaPinnedTest#aDeltaAPodSlotPOINTSAtIsKEPTEvenWhenEVERYTHINGElseSaysCollect`
    and `#aPointerOLDERThanEverythingElseStillPinsIt`, with
    `#theNEWESTCheckpointIsNEVERDeleted`. (b) `CheckpointRetainedOffsetTest#aCheckpointCarriesWhatGCReportedRetained`
    carries the boundary into the checkpoint and
    `#aBoundaryThatArrivesOUTOFOrderDoesNotWALKBackwards` keeps it monotonic
    there, joined to
    `RetainedOffsetRefusalTest#aPositionBELOWTheFloorIsREFUSED`,
    `#theREFUSALIsDistinguishableFromAGAP` and
    `#theREFUSALCarriesTheRANGEThatWasLost` and `#theFLOOROnlyEverRISES` — a
    `PositionCollectedException`,
    not silence and not an empty delivery. `#aPositionATTheFloorIsFINE` is the
    keep.
11. **NFR-13: a consumer offline for the whole retention window loses nothing** — `ConsumerOutageToleranceTest` —
    `#aConsumerOfflineForTheWHOLERetentionWindowLosesNOTHING`:
    a simulated seven hours of committing with GC running throughout, the
    consumer resuming at *k* and reading every record, asserted **by count AND
    by content**, not by absence of exception.
    `#aConsumerThatKEEPSUpLetsItsREADRecordsGo` is the other direction — proof
    the pass is not simply keeping everything.
12. **Past the ceiling, data is lost and it is loud** — `ConsumerOutageToleranceTest#PASTTheCeilingTheDataGoesAndTheAlarmFIRES` —
   and
    `#theBOUNDARYAConsumerWouldBeREFUSEDAgainstIsREPORTED`, with the alarm of
    8(d) asserted as fired — at verdict time rather than at delete time, as
    8's line records.
    ⚠️ **THE REFUSAL IS ASSERTED SEPARATELY AGAINST A HAND-FED FLOOR, NOT
    REACHED FROM THIS PASS** (M7 milestone review; an earlier draft of this
    line said "reached"). `ingest` does not depend on `client`, so this case
    asserts the boundary is REPORTED and `RetainedOffsetRefusalTest` asserts
    the refusal from a floor set by hand. Nothing joins a GC deletion to a
    consumer refusal, in production or in a test — M7.23, which also owns the
    T4 `RetentionRefusalIT` the test plan names and that does not exist.
13. **NFR-2 re-asserted with the progress path live** — `RetentionIdleCostTest` —
    `#theWATERMARKPathAndTheGCLoopCostZEROOfBothNumbersWhenIDLE`:
    1,000 idle consumers over 3,000 intervals of an injected clock, a progress
    frame observed into the watermark table EVERY interval and a `RetentionPass`
    run EVERY interval over a NON-EMPTY chain — **zero object-store requests and
    zero grants**. Its red record is the listing-GC mutation, which is the
    natural implementation this criterion exists to refuse, and
    `consulted.get() == 3000 * 4` is what proves the pass went through the
    WATERMARK branch rather than returning early at the time floor.
    ⚠️ **THE THOUSAND CONSUMERS AND THE PROGRESS FRAMES ARE DISJOINT
    POPULATIONS** (M7 milestone review, M7.29): the 1,000 subscriptions are on
    one index and the frames carry FOUR entries on another, so the frame volume
    exercised is 4 per interval, not 1,000. ⚠️ **AND THE GRANT ZERO IS BY
    CONSTRUCTION** — the `GrantIssuer` is handed to nothing, which the test's
    own javadoc discloses and an earlier draft of this line did not.
14. **The orphan sweep keeps what is merely slow to commit, and its LIST budget holds** — `OrphanSweepTest` —
    `#anUNCOMMITTEDSegmentWITHINTheGraceIsKEPT` (the
    case that matters) and `#aSegmentEXACTLYAtTheGraceIsKEPT` on the boundary,
    with `#anUNCOMMITTEDSegmentPASTTheGraceIsDELETED` as the easy half and
    `#aCOMMITTEDSegmentIsNEVERACandidateWhateverItsAge`.
    `#ONEHourPrefixPerPassAndNOOther` bounds the scan and
    `#theLISTBudgetIsONECallPerThousandKeys` asserts **3 LIST over 2,500 keys**
    at the 1,000 page size, and `#theCONFIGUREDPageSizeIsWhatIsAskedFor` pins a
    CONFIGURED page size of 40 over 100 keys, so the page size is read rather
    than defaulted.
    ⚠️ **THE "44 LIST OVER A 43,200-KEY HOUR" IS ARITHMETIC, NOT AN ASSERTION**
    (M7 milestone review, M7.22): 44 appears only inside an `as()` description,
    and the R15 claim rests on proportionality pinned at two points. The `ceil`
    correction to the corpus's rounded 43 stands — it rounds in the unsafe
    direction — but the 43,200-key pass is `NOT-RUN`.
15. **A delta is collected only when nothing needs it, and a retained offset stays locatable** — `PointedDeltaPinnedTest` —
   `PointedDeltaPinnedTest#aDeltaWhoseSegmentIsSTILLRetainedIsKEPT`
    and `#ONEStillRetainedSegmentIsEnoughToKeepADeltaNamingSEVERAL` (the
    locatability half), `#aDeltaWhoseSegmentsAreGONEAndNothingPointsAtIsDELETED`
    (the collection half, all three conditions together),
    `#theNEWESTCheckpointIsNEVERDeleted`, and
    `#aPassIssuesNOListAndNOGetAndBATCHESItsDeletes` for the cost.
    ⚠️ **MET AT THE CLASS: `ChainGc` IS CONSTRUCTED ONLY BY THIS TEST, AND THE
    RETAINED-SEGMENT SET IT JUDGES AGAINST IS A TEST LITERAL** (M7 milestone
    review, M7.24). Nothing joins `SegmentGc.Result.deletedKeys()` to it.
    ⚠️ **AND CRITERION 10(a)'s REPLAY HALF IS `OBSERVED-NOT`**: the test
    asserts the pointed delta's OBJECT still exists, not that a replay of that
    pod's `(incarnationId, flushSeq)` is answered from it.
16. **GC is leased, and a fenced GC deletes nothing** — `GcLeaseTest#aPassThatLOSESTheLeaseIssuesNOFurtherDELETE` —
    asserted as the ABSENCE of a further delete, COUNTED AT THE STORE
    (`deletes() == 1`, the one issued before the fence). ⚠️ The case does catch
    the fence's `IOException` deliberately — what it refuses is a pass that
    keeps deleting, not a pass that throws, and
    `#aFencedDELETEDoesNotTakeTheNodeDown` is the separate case for that. Also
    `#aPodThatDoesNotHOLDTheLeaseRunsNOTHING` for criterion 13's other half —
    zero store requests of any kind. `#theLEASEIsRELEASEDEvenWhenThePassTHROWS`
    keeps a crashed pass from wedging the role, and
    `#theFENCEDStoreOnlyGuardsDELETE` pins the fence to the destructive verb.
17. **The operator can see the outage budget, and the alarms have no high-cardinality label** — `RetentionAlarmsTest` (29 cases) —
    `#theOUTAGEBudgetCOUNTSDOWNAsTheOldestUnreadDataAges` (read at two fixture
    ages, the later reading SMALLER by the elapsed time, so a constant
    `minRetention` and a sign flip are both red),
    `#theBudgetIsTheWORSTStreamsNotTheBest`,
    `#theBudgetGoesNEGATIVERatherThanClampingAtZero` (an hour over and a day
    over are different incidents) and
    `#aStreamNOBODYHasReportedOnDoesNOTSetTheBudget` (null, not zero — zero
    would page an operator about a cluster that has not started). The three
    alarms each fire independently: `#aCopySILENTPastTheTimeoutALARMS`,
    `#dataAPPROACHINGTheCeilingALARMSBeforeItIsDeleted` (BEFORE, not at) and
    `#aPAUSEDStreamOlderThanTheFloorALARMS`, with `#EACHAlarmIsRaisedINDEPENDENTLY`.
    `#anALARMCarriesNOStreamIdentityInITSLabels` and `#theTOPKIsBOUNDEDHoweverManyStreamsThereAre`
    hold the label rule, and `GATE_SCOPE=full ./scripts/check-metric-cardinality.sh`
    is green over the tree's Java files.
    ⚠️ **THE GATE'S GREEN IS PARTLY AN ACCIDENT — M7.20, carried, MEASURED.**
    `check-metric-cardinality.sh` matches `tag|label|attribute|put` followed by
    a forbidden name and does **not** see `Map.of("stream", …)` inside a
    `labels()` method: the reviewer added exactly that and the gate stayed
    green, with only `#anALARMCarriesNOStreamIdentityInITSLabels` catching it.
    The rule holds; the gate that is supposed to hold it is too narrow.
    ⚠️ **AND M7 ADDED A LABEL OUTSIDE observability.md RULE 1's CLOSED
    ALLOW-LIST** (M7 milestone review, M7.28):
    `RetentionObservable.Alarm.labels()` returns `Map.of("kind", …)`, and
    `kind` is none of `op`/`purpose`/`domain`/`lane`/`result`/`tier`. Three
    values, so the cardinality cost is 3x on a 1,080-combination budget and
    small — the defect is that the standard's table was not updated and no ADR
    was written. A deny-list gate can never enforce a closed allow-list, which
    is a second hole in that script beyond M7.20's.
18. **`safetyMargin` is measured, or recorded as not measured** — `CommitIntervalProbeIT` (`./gradlew :plugin:clusterTest`) —
    a real single-node cluster, which reports
    `batchStart 0000000000000000000 → 0000000000000000059` over 60 records and
    `naturalWaitsMillis=[10029, 10026, 10024]` — ⚠️ **the triple is a TIMING
    observation and differs run to run**; this one is from the recorded run in
    `plugin/build/test-results/clusterTest/`, while the M7.14 commit body and
    ADR-0051 quote the earlier runs they were written from, and what all three
    agree on is that the pointer did not advance in any of the waits — and
    asserts both that the
    committed pointer MOVED and that `RetentionRule.DEFAULT_SAFETY_MARGIN`
    exceeds what it moved by.
    ⚠️ **M6 IS ANSWERED YES: ADR-0051.** `batch_start` is in the shard's last
    Lucene commit user data and a plugin inside the node reads it, so ADR-0005's
    "OpenSearch does not expose it" is true of the ingestion-state API and false
    in-process; ADR-0005's Consequences bullet is amended in that commit.
    ⚠️ **M5 IS `OBSERVED-NOT` AT PRODUCTION RATES, AND THE NUMBER IS A FLOOR.**
    The pointer did **not** advance on its own across three consecutive
    ten-second observations, because a commit follows the translog flush policy
    (512 MB or 30 minutes), not ingestion. One shard and 60 records cannot size
    a margin at Scenario A's rate, so `DEFAULT_SAFETY_MARGIN = 10_000` remains
    **an upper bound on a guess**, said in as many words in the javadoc,
    ADR-0051, the M7.14 backlog row and `50-open-questions.md` § 3. What
    reporting the COMMITTED pointer buys is not a smaller margin but the error's
    DIRECTION — behind-durable rather than ahead — so being wrong costs storage
    rather than records.

## What M7 does NOT close

- **M7.17** — the reporter has no production source of positions.
  `ProgressReporter.Positions` is unimplemented on a real node, so nothing
  reports, every watermark is UNKNOWN and GC keeps. Latent, and in the safe
  direction. ADR-0051 now names the source (`IndexShard.acquireLastIndexCommit`,
  `batch_start`), so the row is specified rather than open — and the
  `GatedCloseable` it returns pins segment files, which a leaking reporter would
  turn into silent node-disk growth.
- **M7.21n** — ⚠️ **AND THESE TWO CARRIED THE WRONG IDs WHEN THIS DOCUMENT WAS
  WRITTEN**, which M8.0's spec review found: `RetentionPass` is not wired into a
  running ingester (it is exercised by tests and by `RetentionIdleCostTest`'s
  loop, not by a deployed loop) and the GC lease has no production role
  registration (`LeasedGc` is driven by fixtures). This document called them
  **M7.18** and **M7.19**; M7.18 is the retained-floor frame and M7.19 is a
  flaky `RoutedIngestTest` deadline that names no mechanism at all. Both
  mechanisms, with `RetentionObservable`, now have a row of their own — M7.21n —
  and M8's SPEC § *The unwired set* is the single list.
- **M7.18** — the retained floor does not travel from GC to the consumer; the
  two halves are asserted separately in two modules.
- **M7.20** — `check-metric-cardinality.sh` does not see a label map, above.
- **M7.24** — `ChainGc` is wired to nothing and is constructed only by its own
  test; the retained-segment set it judges against is a test literal.
- **M7.25** — ⚠️ **NO PRODUCTION CODE CAN PRODUCE THE CHAIN EVERY GC COST CLAIM
  RESTS ON.** `List<CommitDelta>` is consumed by `RetentionPass`, `SegmentGc`
  and `RetainedOffsets` and produced by nothing in `src/main`. ⚠️ **AND THERE IS NO WAY TO MATERIALISE ONE AT ALL** —
  not one expensive way. `CommitLog.recover()` returns `void`, and
  `ChainReplay.Result` carries offsets, the next sequence, the seal, index
  entries and pods; the deltas it reads are matched and DISCARDED. So criterion
  9's "zero LIST, zero GET" is true of `SegmentGc` and **unproven
  of a pass**: the meter is wrapped around the consumer of the chain rather
  than around its acquisition.
- **M7.26** — "≤1 DELETE per 1,000 keys" is a constructor argument passed by
  every test and has no named default in the tree, so a wiring that passes 1 is
  a 1,000x DELETE-cost regression that compiles and passes everything.
- **The T4 `RetentionRefusalIT` the test plan names does not exist** (M7.23).
  `CeilingAlarmOnGcTest`, also named, is genuinely covered on the real GC path
  by `ConsumerOutageToleranceTest#PASTTheCeilingTheDataGoesAndTheAlarmFIRES` —
  a substitution, recorded here rather than left silent.
