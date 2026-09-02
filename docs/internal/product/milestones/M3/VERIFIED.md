# M3 -- Verified

One line per acceptance criterion in `SPEC.md`, each naming the test or command
that demonstrated it.

Suite re-run fresh for this record, with the build cache and the up-to-date
checks both defeated so nothing was served from a previous run:
`./gradlew test --rerun-tasks --no-build-cache` (ALL modules, 48 tasks
executed) -- **415 tests, 0 failures**: binstore-backends 86, binstore-spi 29,
client 15, format 122, http 36, ingest 93, plugin 34. All
`*/build/test-results` deleted beforehand and confirmed regenerated.

⚠️ ALL modules, not the four this milestone touched. An earlier draft of this
record ran only `:format:test :ingest:test :http:test :plugin:test` and
reported 285. Review found that `client`'s `ConsumerClient` reads
`RunEntry`/`SegmentReader` and so is one of the "every existing reader and
writer of the directory entry" that criterion 6 requires to pass at the new
49-byte width -- it was covered, but unattributed. Running everything is
cheaper than arguing about which modules the format change reaches.

⚠️ And recorded with `--rerun-tasks --no-build-cache` because the first two
attempts at this record reported `BUILD SUCCESSFUL` in 1-2 seconds having
executed NOTHING: `--rerun` alone left the other tasks `FROM-CACHE`/
`UP-TO-DATE`. Caught by checking task outcomes and result-XML timestamps
rather than the exit code -- which is the only reason this file does not
carry a green line for a run that never happened.

The two `-Xmx`-bound tiers are separate Gradle tasks and are named per
criterion below.

1. **Lengthens on sustained low `fillRatio` AND shortens at 0.9, proven against the threshold logic itself** -- `binjava.ingest.AccumulatorTest#theIntervalLengthensToTheCeilingOnceFillRatioSustainsLowForTheFullDelay`,
   `#theIntervalDoesNotLengthenBeforeTheSustainedDelayElapses` (one millisecond
   short of `intervalLengthenDelay`, so the boundary is pinned from both sides),
   `#theIntervalStartsAtTheFloor` (every pod earns lengthening rather than
   starting anywhere else), and
   `#aMiddleBandFillRatioResetsTheLengthenStreakWithoutChangingTheInterval`
   (only a CONTINUOUSLY sustained condition may fire). The SHORTEN half of
   this criterion is
   `#theIntervalShortensToTheFloorImmediatelyOnceFillRatioReachesTheHighThreshold`,
   detailed under criterion 3. ⚠️ Named here too because an earlier draft of
   this line restated AC1 as only its lengthening half -- and
   `check-milestone-verified.sh` enforces enumeration, not faithfulness, so a
   narrowed heading passes while covering half a criterion.
   All of these drive `fillRatio` and the thresholds directly, not "a flush
   eventually happened", which is what AC1 asks for.
2. **Never exceeds the ceiling, never drops below the floor** -- `binjava.ingest.AccumulatorTest#theIntervalNeverExceedsTheCeilingNorDropsBelowTheFloorUnderOscillation`,
   which drives `fillRatio` adversarially between the bands and asserts the
   interval's own band on every iteration, not only at the end. Config-side
   bounds: `binjava.ingest.IngestConfigTest#aCeilingBelowTheFloorIsRefused`,
   `#aCeilingEqualToTheFloorIsAccepted`,
   `#fillRatioThresholdsOutsideZeroToOneAreRefused`,
   `#aLowThresholdAtOrAboveTheHighThresholdIsRefused`,
   `#negativeHysteresisDelaysAreRefused`, `#aZeroLengthenDelayIsAccepted`,
   `#theDefaultsIncludeM3sAdaptiveRange`.
3. **Asymmetric hysteresis** -- shortening reacts within `T_shorten` (default zero, the very next flush): `binjava.ingest.AccumulatorTest#theIntervalShortensToTheFloorImmediatelyOnceFillRatioReachesTheHighThreshold`;
   lengthening requires the full `T_lengthen`:
   `#theIntervalLengthensToTheCeilingOnceFillRatioSustainsLowForTheFullDelay`
   against `#theIntervalDoesNotLengthenBeforeTheSustainedDelayElapses`. The
   control signal itself: `#lastFillRatioIsZeroBeforeAnyDrain`,
   `#drainComputesFillRatioFromTheRealSegmentBytesNotTheEstimate`,
   `#lastFillRatioUpdatesOnEachDrainIndependently`,
   `#lastFillRatioSurvivesAnEmptyDrainAfterARealOne`.
4. **Measured PUT rate within +/-20% of `1 / intervalCeiling` = 0.2 PUT/s at a modeled 1 MiB/s** -- `binjava.ingest.IntervalPutRateTest#theSustainedRunSettlesToTheCeilingsPutRate`,
   measured through `CountingBinStore` over a 60-second window after the
   interval settles, not inferred from the interval value. The same test also
   pins that it settles within 1s of the configured 2-minute
   `intervalLengthenDelay` and then HOLDS the ceiling.
5. **NFR-6 under the adaptive interval, at the ceiling** -- `binjava.http.MemoryFlatAtIntervalCeilingTest#memoryFlatWithTheIntervalAtItsCeiling`,
   re-run for THIS record under a real 256 MB heap:
   `./gradlew :http:memoryBoundCeilingTest --rerun-tasks --no-build-cache` --
   **BUILD SUCCESSFUL in 3m20s, tests=1 skipped=0 failures=0 errors=0,
   time=199.245s**, results in `http/build/test-results/memoryBoundCeilingTest/`.
   ⚠️ An earlier draft of this line quoted "200.0s ... on the committed bytes"
   from the M3.5 commit body -- a run made BEFORE that commit and never
   repeated, with no results on disk to back it, while this same file
   explicitly declared its sibling `memoryBoundTest` NOT RE-RUN. Review caught
   it; the line above is now a run that actually happened for this record.
   24 concurrent producers stream 200 MB with the interval warmed to its 5s
   ceiling; peak sampled heap < 220 MiB and every sample <=
   `max(128 MiB, 10x bytes written)`. ⚠️ Per ADR-0026 this is the CONCURRENT
   shape: a single producer holds only ~223 KB at any interval and so cannot
   reach the state this criterion names. M1.18's floor-side proof
   (`MemoryFlatUnderTenXBodySizeTest`, `./gradlew :http:memoryBoundTest`) is
   unchanged and was NOT re-run for this record -- it is M1's evidence, not
   M3's.
6. **The reserved lane byte round-trips** -- `binjava.format.GoldenSegmentV1Test#theCommittedGoldenV1SegmentStillParsesAtTheRightVersion`,
   `#theReservedLaneByteRoundTripsAsZeroForEveryRunInTheGoldenV1Segment`,
   `#aFreshlyWrittenSegmentRoundTripsTheReservedLaneByteThroughTheRealWriterAndReader`,
   `#theLaneByteIsReadFromItsOwnOffsetNotAliasedToCodecFlags`,
   `#aVersion0SegmentSynthesisesLaneAsZeroNotGarbage`,
   `#aVersion0SegmentSynthesisesZeroEvenWhenTheByteAfterTheEntryIsGenuinelyNonZero`,
   `#everyRecordInTheGoldenV1SegmentDecodesToWhatItWasWrittenAs`,
   `#aSegmentClaimingAnUnknownVersionIsRefused`,
   `#directoryEntryBytesForRefusesAnyVersionOtherThanTheTwoKnownOnes`,
   `#directoryEntryBytesForMapsEachKnownVersionToItsOwnWidthNotTheOther`.
   ⚠️ `golden/segment-v0.bin` was NOT regenerated -- it is the old shape's
   fixture and must keep parsing; `golden/segment-v1.bin` is new. Both are
   committed and read by tests, per the `wire-format-change` checklist's
   "golden files for the old AND the new shape". Verified against a REAL
   OpenSearch node for this record:
   `./gradlew :plugin:clusterTest --rerun-tasks --no-build-cache` --
   **BUILD SUCCESSFUL in 30s, 9 tests, 0 failures** across `SingleNodeBootIT`,
   `SearchableIT`, `OffsetMonotonicityIT`, `DeleteAndVersionIT`,
   `ShardFanOutIT`, `RestartResumeIT`. The widened 49-byte directory entry is
   therefore read back by the real plugin in a real node, not only by unit
   tests.
7. **No module below `http` gains a Helidon dependency; the interval logic is testable with no socket** -- `GATE_SCOPE=full ./scripts/check-module.sh`,
   run for this record: `ok 8 module(s) checked, rule5 tested 4, rule2 tested 2`.
   Every test for criteria 1-4 above lives in `ingest`'s own suite and touches
   no socket; `Accumulator` takes its `Clock` injected (non-negotiable 7).

## Gates that did NOT run, and why

⚠️ Stated because a green line for something that did not run is worth less
than no line (non-negotiable 4).

- **`check-tdd` for `MemoryFlatAtIntervalCeilingTest`** -- SKIPPED, and the
  M3.5 commit says so. `tdd_scan.py` maps tests by SOURCE SET, so this class
  resolves to `:http:test`, a task that EXCLUDES it (it runs only under
  `memoryBoundCeilingTest`), and the script therefore runs nothing and can mint
  no red record. Same structural gap `MemoryFlatUnderTenXBodySizeTest` (M1.18)
  has. Falsifiability was verified BY HAND: disabling `adaptInterval`'s
  lengthen branch makes the warm-up probe return 256ms instead of >2000ms and
  the test fails in 3 seconds with a real JUnit `<failure>`. Backlog **M0.52**
  is the deterministic fix. Every OTHER new test in this milestone has a
  mechanical red record.
- **`check-reviewed`'s round cap for M3.5** -- SKIPPED. Both verdicts ARE
  recorded against the committed bytes, both `pass`, both zero findings; only
  the 2-round cap failed, after a 4th round that reverted a wrong number the
  3rd round had introduced. The commit body carries the reasoning.
- **`check-coverage.sh`, `check-suite-time.sh`** -- exist in `scripts/` but
  are not wired into `.pre-commit-config.yaml` and were NOT invoked for this
  record.
- **`check-mutants.sh`, the cost meter, the benchmark gates** -- DO NOT EXIST
  (AGENTS.md § Gates names all three as absent).
  ⚠️ An earlier draft of this section said the milestone was verified "without
  the two strongest automated quality gates", which understated it: the absent
  set is five, not two. It matters most for the **cost meter**, because M3's
  own completion condition is a request-rate reduction ($311 -> $15.55/month,
  NFR-1) and NO gate measures that. Criterion 4's evidence is a modelled test
  asserting PUT/s through `CountingBinStore`, which is real but is a test, not
  a budget gate that would fail on a regression.
- **`memoryBoundTest` (M1.18's floor-side NFR-6 proof)** -- NOT RE-RUN, as
  noted under criterion 5.
