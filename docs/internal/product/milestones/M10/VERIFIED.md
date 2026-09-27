# M10 evidence and milestone review

One evidence line per acceptance criterion of [SPEC.md](SPEC.md). Each names
the test that demonstrated it; the task's backlog row carries the red record
(the mutation or stub each test was observed failing against) and its review
verdicts. What was not run is marked NOT-RUN rather than implied.

M10 closes with NFR-5 **measured on every transport, proxy payloads
included** — the term M9 left PARTIAL — on a two-AZ RustFS fleet, both above
the 256 KiB inline cap (0.040%) and below it (0.072%). ⚠️ **NFR-5 is NOT
claimed at every segment size**: about 166 B of event frame still crosses the
zone per cross-zone-served STREAM per segment, so 0.1% holds only while a
segment carries more than roughly K x 166 KB for its K such streams: an 8 MiB
segment serving 400 streams across the zone is at about 0.79%, and a trickle
index's few-KiB segment is over budget on its event alone. The ITs measure
K = 1 (ADR-0076, open row M10.34). The milestone review's major finding — sub-cap segments
were pushed inline across the zone — was fixed inside M10 (M10.33,
ADR-0076). It does NOT close: AWS S3 latency, TTFB and billed dollars (no AWS
account, as at M9); FR-21's per-index attribution, `GET /admin/cost` and
quotas (M11, ADR-0075); and the open follow-up rows M10.24–M10.28, M10.30, M10.34 and
M10.35, each owned in the backlog.

## Acceptance criteria

1. `SegmentFetchServiceTest` (11 cases, T1): the body equals the stored bytes and a second fetch is a cache hit with zero further GETs; a key outside the data grammar or the trust-domain prefix is `400` with zero store requests; a missing object `404`; a store failure on an existing object `502`; `FrontDoorSegmentFetchTest` pins the route on the assembled front door. (M10.1)
2. `SegmentFetchServiceTest` same-AZ, cross-AZ and unattributed cases: a same-AZ fetch adds to same-AZ `PROXY_READ` only, another AZ adds its exact byte count to cross-AZ, and no AZ is cross-AZ and unattributed. (M10.1)
3. `ProxyFetchTest` (3) and `HttpSegmentSourceProxyTest` (5): an empty `PROXY` delivery is resolved through `SegmentSource.fetchSegment` by its exact key and yields the written records; one carrying bytes is not fetched; no source fails loudly; the HTTP source sends the key and its AZ and refuses any answer but 200. (M10.2)
4. `NodeSegmentSourceProxyTest#SIXTEENShardSubscriptionsReadingONESegmentKeyIssueONERouteFetch`: 16 shard subscriptions on one node reading one segment key issue exactly one `/seg` fetch, carrying `az`; `#TWODistinctSegmentKeysCostTWOFetchesAndEACHGetsItsOWNBytes` pins the dedup key. (M10.3)
5. `SegmentProxyInFlightTest` (10 cases): K >= 8 racing callers on one cold key share one store GET, a segment larger than the cache included; a throwing GET, or a first caller dying of an `Error`, fails every attached caller and removes the in-flight entry; "caches nothing and the next caller reads again" is asserted for the throwing GET (`gets == 2`), not separately for the `Error` case. (M5.63)
6. `CrossAzBytesIT#fullNfr5IncludesProxyPayloads` (T3, RustFS, two AZs) passed: 842,180 producer bytes, all consumed; 830,406 B delivered through the az-b proxy in two fetches; **335 B cross-AZ over both pods, 0.040%** (< 0.1%, against both accepted and consumed bytes), all of it the writer's `PROXY_READ` event frames; az-b same-AZ `PROXY_READ` equal to the bytes delivered; a control fetch from the az-a pod as az-b adds exactly its 415,203 B to cross-AZ. Red against an 8x event-frame over-count. (M10.4) `CrossAzBytesIT#nfr5HoldsBelowTheInlineCap` (T3, same topology, three 228,363 B segments under the cap) passed: 499 B cross-AZ, **0.072%**, `inlinePush` 0, all 685,089 payload bytes served by the az-b pod's `/seg`; red against the pre-M10.33 production code (685,597 B inlined across the zone). (M10.33, ADR-0076) ⚠️ Measured at one stream per segment; the per-stream event frame is the floor (~K x 166 KB per segment for K cross-zone streams, M10.34).
7. `SegmentWriterLaneTest` (T0) and the pinned `golden/segment-v1-lanes.bin`: a run's lane is the maximum of its records' and round-trips through `SegmentReader`; `segment-v1.bin` is unchanged and still decodes with lane 0 in every entry (`GoldenSegmentV1Test`); byte identity of a FRESH all-lane-0 write against it was checked by hand in M10.5's review and is NOT pinned by a test (OBSERVED-NOT as a test; harvested below). (M10.5)
8. `BulkServiceLaneTest`, `BulkServiceRoutedLaneTest`, `LaneSetTest`, `ServerPropertiesLaneTest`: absent `lane` is 0; an active value reaches the stored run entry on the explicit and routed paths; outside the active set, non-integer or outside `i8` (including values that would wrap into an active lane) is `400` and writes nothing; a set above 8 lanes or with a lane above `+2` is refused at configuration. (M10.6)
9. `AccumulatorLaneTest` (T0, injected clock): `+l` alone is due at `max(floor, interval / 2^l)` and not before, never before the floor; a negative lane alone only at the ceiling; under a sustained positive stream a buffered negative record flushes within the ceiling; a `+2` trickle at the ceiling interval causes exactly 4 flushes per ceiling and a lane-0 trickle 1. (M10.7)
10. `DefaultIngestLanePushTest` (T1): one segment whose key order is the reverse of its lane order is pushed positive run first, then lane 0, then negative, to separate consumers and within one consumer's run list. (M10.7)
11. `LaneAdmissionTest` (9, T0) and `BulkServiceAdmissionTest` (5, T1): with the budget saturated a lane at or above its floor is `429` (never 5xx) while one below it is admitted; with the budget free every lane is admitted; floors are proportional to `2^l`, exact for wide sets such as `{-128, 0}`; a released permit readmits its lane; the request's own lane reaches admission; an inactive lane is its permanent `400` before any permit. (M10.8)
12. `LaneOvertakeTest#aPLUS2AppendIsAckedWithinTheCEILINGOver4AndCarriesTheBUFFEREDMinus1RecordsAtTheirOFFSETS` (T2, real `BulkService` on a socket, injected clock, interval at the 5 s ceiling): the `+2` append is acknowledged within `ceiling / 4` of clock time (the clock is advanced once, by exactly that), the buffered `-1` records commit in the same segment at their original offsets; red recorded against positive lanes treated as lane 0; the SPEC's named mutation (the lane dropped in `BulkService.appendChunk`) was observed failing by the author only and not recorded as a red. (M10.9)
13. `CostGovernorTest` (10) and `GoverningBinStoreTest` (5), T0, plus `GoverningBinStoreConformanceTest`: an undeclared LIST past the bucket is refused and counted whatever its prefix, the commit log included; a declared recovery LIST is never refused; data, commit, checkpoint and lease PUTs, GETs, STATs and DELETEs are never refused even with the kill switch tripped; alarm at 3x, discretionary halt at 10x, kill switch at 100x holding until `reset()`; a per-record regression at 10 records/s with the spacing at the 5 s ceiling reads above the halt. (M10.10)
14. `GovernedRecoveryTest` (3), `GovernorAssemblyTest` (2), `GovernorHaltAssemblyTest` (3), `GovernorStoreOrderTest`, `OrphanSweepGovernorTest` (2), `SegmentPrefetchGovernorTest`: the assembled store is governed with the counter below the governor; a takeover's chain-end, replay, backfill and inbox drain succeed with the LIST bucket empty; a refused sweep LIST defers its hour and the next tick sweeps it; a halted node's prefetcher issues no GET and its retention pass lists nothing; `GovernorAssemblyTest#aSTEADYStateAppendCommitReadAndSweepRecordsZEROREFUSALS` records zero refusals (NFR-16) at T1, its steady-state read going through `segmentProxy().streamTo` rather than the `/seg` route. ⚠️ "Governed" holds for the NODE store: `FrontDoor`'s commit-route inbox drain LIST and `/seg`'s absent-key `stat` still run on the raw backend, neither governed nor counted (open row M10.26). A fleet-scale steady-state soak with the governor wired is NOT-RUN. (M10.11)
15. `RoutedIngestTest#aROUTEDWriteToAnUNREGISTEREDIndexWAITSAndIsRELEASED`: root-caused in the test (it waited for the write to finish rather than to be held) and fixed there, no timeout moved; M10.13 (`FlushCoordinatorTest`, `DefaultIngestSettlementTest`) and M10.31 (`DurableSignalRingOwnerTest`) root-caused the two other load-sensitive failures seen during M10. (M10.12)
16. M10.20a–d are done: the items of M9/VERIFIED.md finding 5 are fixed by named tests in those rows (`CostLatencyCurveGeneratorTest`, `WiredGrammarTest`, `WiredFactoryTest`, `CatchUpOfferTest`, `CommittedDeltaSourceTest`, `DurableCatchUpResponderTest`, `CatchUpRetryContinuityTest`, `TierThreeAvailabilityTransitionTest`, `DrainAskBytesTest`) or dropped with their reasons in M10.20a's row (and `34e4fcd`'s record-semantics drop in M10.20d's); the one real defect a drop exposed became M10.22 (`NodeLocalStoreReaderBodyDeadlineTest`, `TierTwoReleaseNarrowingTest`). ⚠️ `PartitionVisibilityIT`'s Awaitility rewrite compiles but was NOT-RUN green (it failed three runs on the loaded rig; harvested below). M9's four explicit drops (`3f141e6`, the M9.49 request-shape concern, `d12d6ad`, `3b696b7`) stand as dropped in M9/VERIFIED.md itself and are not re-recorded in an M10 row. (M10.20)

## Gates, as run for this close

- For every M10 commit, the authoring session ran `./gradlew gates` (repository gates, `checkHarnessTests`' native gate tests, wired and override gates), `checkReviewed`, `checkTdd`, `checkTestIntegrity` and `checkCommitMessage` before committing, and the affected modules' tests before each push. That is the sessions' own record, not something the tree can show.
- **On this close commit's tree**, `./gradlew test` ran once over all modules: **2,587 tests, 1 failure** — `LadderExecutionTest#ONESubscriptionDownOnASHAREDTransportIsTheNodesTIER` ("ONE stream fell down the ladder, once: expected 1 but was 2"), which then passed 5 of 5 on `--rerun`. OBSERVED, not explained: open row M10.35 root-causes it and must rule M10.23's consumer retry in or out. The full run is therefore NOT green, and this file does not claim it is.
- `./gradlew checkMilestoneVerified` over this directory: "milestone evidence covers 16 criteria".
- `:server:integrationTest` ran for `CrossAzBytesIT` (all three cases) only; other integration and soak suites are NOT-RUN for this close. `checkMutants` and `checkCoverage` (manual stage) are NOT-RUN; reviewers ran targeted mutations per task instead, recorded in each commit body.
- The historical script-backed `buildSrc` tests (`AdrRefsGateTest`, `FaultStoreRecordsTest`, `IoSeamGateTest`, `ReviewPacketCostTest`) fail in the full `-p buildSrc test`; they fail identically at M9's close commit `aacd00c` and are not in `nativeGateTest`. OBSERVED-NOT as an M10 regression; not fixed here.

## Milestone review

Run over every M10 commit (`fe257cf^..HEAD`, per `milestone-review`), from
the commit bodies, the SPEC, the backlog and the code; the cost meter the
skill asks for does not exist, so request-rate trends were reasoned from code,
not measured. Its criterion check produced the corrections already folded into
the lines above (criteria 5, 7, 12, 14, 16), and its seven findings are:

- **F1, major — fixed in M10 by M10.33 / ADR-0076.** The writer chose one
   fetch mode per segment and never consulted the subscriber's AZ, so a
   segment under the 256 KiB inline cap was pushed INLINE to a cross-AZ
   subscriber and NFR-5 failed exactly at low and medium rates;
   `fullNfr5IncludesProxyPayloads` passed only because its bulks exceeded
   the cap. See criterion 6. The fix exposed the remaining floor — the
   per-segment event frame, open as M10.34.
- **F2** — the admission budget counts `_bulk` requests parked on the
   durable-ack wait, so at low rate it caps concurrent producers rather than
   load; and **F3** — the new `429` carries no `Retry-After`, which research 14
   §3 and ADR-0010 ask for. Harvest H2.
- **F4** — "fetch a segment once" is built two ways that fail oppositely (the
   ingester shares one failure among attached callers; the plugin re-fetches
   per waiter); folded into open row M10.28.
- **F5** — `ConsumerClient` (700) and `Assembly` (699) are at the file limit,
   and M11's attribution wiring lands in `Assembly`. Harvest H3.
- **F6** — the lane default methods on `Ingest` let fakes compile without lane
   support, which hid defects twice (M10.6 T4, M10.9 P1). Harvest H4.
- **F7** — row and ADR text errors, corrected in this close: the M10.7 row's
   "M10.31" (it is M10.32), M10.22's "counted from the request", M10.26's
   "GETs" (it is the absent-key `stat`), the backlog header, and ADR-0014's
   missing pointer to ADR-0074's amendments.

No finding for duplicated retry/backoff logic, single-caller abstractions, or
steady-state request rates; the lane PUT cost is bounded and amended into
NFR-1.

### Harvest for the specification of M11

Every `minor` recorded in an M10 commit body was read. The following become
backlog rows when M11 is specified, or are dropped with
their reason; open rows M10.24–M10.28 and M10.30 are confirmed still real and
carried as they stand, with M10.34 and M10.35, opened by this close.

- H1. *(F1 — done in M10 as M10.33.)*
- H2. Admission and `429` hygiene: no in-flight permit held across the durable-ack wait (or budget by buffered bytes); `Retry-After` on every `429`, from one place.
- H3. Split `Assembly` and `ConsumerClient` below ~600 lines, no behaviour change, before M11 wires attribution.
- H4. Pin the lane seam: make the lane-taking `Ingest` methods abstract or pin both defaults; the lane `append` on `RoutedIngestInactiveLaneTest`'s fake; `LaneOvertakeTest`'s javadocs; the lane arrays' growth past 8.
- H5. Flush coordinator: refuse a second batch loudly in `enqueueFlushLocked` before it detaches; an `Error` from the handler must not end the worker silently; pin the deferral and missing-stream settlements, the flush-time epoch hand-in, the per-settlement try/catch, push-after-acks, and a deferred buffer behind a FAILED queued flush.
- H6. Governor residue: a concurrent multipart per-part map; pin `observePut` on `putIfAbsent`/`putIfMatch`, the idle-gap and zero-PUT resets, "a declared LIST spends no token" and window re-anchoring; the prefetcher's already-fetched check before the governor; one test counting how often the governor is asked.
- H7. Tier-2 reader: a non-emptying release must not wait behind a stalled read through the poller lock `observeCursor` shares with `deliverCatchUp`; the deadline counted from the request or stated as ~60 s; the deadline in the error text; remove cancelled timers; pin `refCount <= 1`, the exact exception, `timer.cancel` and the non-positive refusal.
- H8. Consumer retry: the race guard must not throw after `reportAnyGap` has committed the offset; pin reset-on-surfacing, doubling and jitter, `wouldDecode`, `catchUpDecodeLock` and the subscribing constructor's default; replace the one test that really sleeps with the injected sleeper.
- H9. Proxy-path pins: rename `SegmentProxyInFlightTest`'s `diesOnItsThirdChunk` (M5.63's recorded minor); put-before-remove ordering; the too-large fallback re-caching; the mid-segment throw vs `answerFailure`; a guard on the streaming test's header wait; `HttpSegmentSource`'s null-endpoint refusal.
- H10. Test-timing margins: keep lease-renewal PUTs out of `DurableSignalRingOwnerTest`'s store-wide windows; give `RoutedIngestTest`'s `write.get` a margin over the pool's 10 s timeout.
- H11. `PushQueue` pins: the shutdown drain, and the budget's `>` against `>=`.
- H12. Pin criterion 7's byte identity: a fresh all-lane-0 write equals `segment-v1.bin`.
- H13. Hygiene residue: `check-mutants.sh` references in the jzap comment, build.md and the milestone skill; `WiredFactoryTest`'s exit-code-only case; `DefaultIngest`'s unused `LOG` and `Duration` import.
- H14. One green `PartitionVisibilityIT` run on an unloaded rig, with its drain latencies recorded.
- H15. Scope `checkMilestoneVerified` to the SPEC's criteria section of `VERIFIED.md`: it takes the first `N. ` line anywhere in the file, so a numbered list below the criteria can mask a deleted criterion (measured in the M10.21 review, criteria 10 and 13); and pin its default milestone with a test.

**Dropped:** M10.7 R4 and M10.12's "row sentence too broad" (text, corrected
here); M10.7 T4 (at most 62 ms on a positive lane, below the lane's own
granularity); M10.4 T2 (the consumed-bytes budget exists to guard against
re-padding); M10.29 P1 (no config filters by logger name); M10.3 P1 (it is
M10.25) and P2 (a harmless package-private test accessor); M10.0's round-3
minor (done by M10.11); M10.1's "`checkMutants` cannot run for `:http`"
(fixed by M10.20b).

### Re-plan

The roadmap's Deferred table gives M11 both fast mode (FR-17) and the FR-21
remainder (per-index attribution, `GET /admin/cost`, quotas); the specification of M11 must write
its roadmap row and take one. For the FR-21 remainder the review recommends:
harvest H3 (the file splits) first, then M10.26 and M10.27 (per-index
counts over a store stack with raw, uncounted paths would apportion a wrong
total), then an apportionment ADR, `/admin/cost`, and quotas carrying H2's
`Retry-After`. M10.28 and M10.25 should land before `direct` becomes a default.

### Research corpus

Updated: `15-cost-governor.md` gains a banner for ADR-0075's departures (this
close); `04` and `05` gained ADR-0076 banners in M10.33. Noted and not edited: research 12's "a lane reorders in the buffer"
callout (amended by ADR-0074), research 14 §4's lane labels (no longer the
`-2..2` default), and research 10's "one GET, but a failure fails all K"
trade-off.
