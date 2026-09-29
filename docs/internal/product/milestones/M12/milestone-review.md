# M12 milestone review

The reviewer wrote none of M12's commits. The review covers the 34 commits `f4bae4dd^..d58c6786` (M12.0 Specify M12 through M12.26 Let a backing-off catch-up give its quantum turn to live). It reads every commit body, the full diff, `milestones/M12/SPEC.md`, the M12/M13 roadmap rows, and the M12.x and M11.23-25 backlog rows. It follows `.agents/skills/milestone-review/SKILL.md`.

## What I ran, and what I did not

- **`./gradlew gates` at HEAD (`d58c6786`): BUILD SUCCESSFUL.** Output: "repository gates passed (JVM/Gradle; 1795 repository files scanned)". `checkHarnessTests`, `checkWired` and `checkOverride` also passed.
- **`./gradlew test --rerun` at HEAD: 2,816 tests, 0 failures, 0 errors, 1 skipped.** I counted this from `*/build/test-results/test/*.xml`, and every file was written between 19:44 and 19:47 during this run. That matches M12.26's "2,815 + one case in round 2".
- **NOT run:**
  - any Docker or integration test (`PartitionVisibilityIT` included, as instructed);
  - `./gradlew -p buildSrc test`;
  - `checkMutants` and `checkCoverage`;
  - `checkMilestoneVerified` for M12, because `milestones/M12/VERIFIED.md` does not exist yet (M12.23 Close M12 is open).
- **The cost meter does not exist (AGENTS.md § Gates), so no requests-per-MiB trend is available.** I did two substitute checks, which are not measurements:
  - I grepped the production diff for new store calls and found no added `get`/`getRange`/`list`/`put*`/`stat`/`delete` call on a store, delegate or backend.
  - I read the three commits that touch a GET or retry path: M12.11, M12.12 and M12.26.

---

## 1. Did M12 deliver what was specified?

| # | Verdict | Evidence I checked, and the gap |
|---|---|---|
| 1 | **Delivered by a route the SPEC did not list. The causal claim is weaker than written** | See 1a below. |
| 2 | Met, with the headroom already mostly spent | The splits are `20c6c99c`, `ebb0ba8b` and `18af4177`. The gate is `RepositoryGateChecks.kt:217-238` (`SPLIT_CEILINGS`, refused at `>= 600`), and `FileSizeCeilingTest` exists. `gates` is green at HEAD (my run). But within M12 itself: DefaultIngest 570 → **592**, Assembly 585 → **594**, BulkService 494 → 533 (see § 2, E1). |
| 3 | Met | `RepositoryGateChecks.kt:249-266` (`ledgerOwnership`, owner `StoreStack.java`). A grep of every `src/main` shows exactly one `new IndexCostLedger()`, at `StoreStack.java:40`. `LedgerlessConstructorGateTest` exists. |
| 4 | Met | `IngestHasNoDefaultsTest` exists (`b4cc39a5`). The price: 25 hand-copied `concreteIndex` overrides in 21 test files (E5). |
| 5 | Met as amended (T0 + T1) | `UnflushedCeiling.java:88-104`: the generation check in `flushEnded`. `DefaultIngest.java:475-481`: the completion passes its own generation. `UnflushedCeilingOverlapTest` and `UnflushedCeilingGuardTest` exist. Open minors: T2 (release with no next batch), T3 (`!pending.isEmpty()` half) and T4 (`waitingForRoom` hook unchecked) (`6866078c`). |
| 6 | Met; **ADR-0078 not amended** | `FrontDoor.java:120-125` passes `catalog().resolve(name).isPresent()`, and `IndexQuotas.java:189` makes no bucket for an unknown name. `IndexQuotasBoundTest` and `FrontDoorQuotaBoundTest` exist. ADR-0078 decision 2a still says "an index's debt is at most `maxInFlight` request bodies past a non-negative balance". M12.4's own P2 says a request unknown at admission binds "past the cap if need be" and holds no slot for its first chunk. No commit amends the ADR for M12.4. `grep M12.4` in ADR-0078 finds nothing. |
| 7 | Met; the log line misleads | `RefusalMetrics.java` exports two unlabelled counters, and `RefusalCountersTest` and `CostTopKRefusedTest` exist. But `RefusedIndices.record` increments `more` once per REFUSAL of a ninth-or-later index, so "and 5000 more" can mean one index (M12.5 P1, confirmed at `RefusedIndices.java:36-45`). |
| 8 | Met | `AdminCostOptInTest` (`5d3ed581`). |
| 9 | Met | `singleTooManyRequests` (`RepositoryGateChecks.kt:279-305`) and `SingleTooManyRequestsGateTest`. The character-literal hole is closed by M12.22's scanner (`GateScannerEdgesTest`). |
| 10 | Met at the route | `AdminCostMalformedRegistrationTest`. The top-K log line still drops an undecodable registration silently (M12.8 P3). |
| 11 | Met | `CostTopKIntervalBoundsTest`. The parser's boundary at exactly `PT24H` is unpinned (M12.9 T1). |
| 12 | Met as written | `ExplicitPartitionWaitCapTest`. Three things weaken it: the cap is per NAME, so N invented names hold 8×N permits (P3); it is checked after the first chunk is parsed (P2); and it is counted under a counter described as the pod's in-flight budget (P1). |
| 13 | **Partly** | `NodeFailureHoldJitterTest` pins 55/65 and shows two SEEDS part at the `upJitter` function. "Two nodes … under different seeds" is not shown through the production entry point: M12.11 T1 says `new SplittableRandom(42)` in `NodeSubscriptions` survives every test. |
| 14 | **Met in the plugin's wiring only** | See 1b below. |
| 15 | Met | `QuotaPropertiesAliasTest` and `FrontDoorQuotaTest#anOverrideNamedByAnAliasReachesTheFrontDoor`. The sorted-alias precedence is unpinned (M12.13 T2). |
| 16 | Met | `MilestoneEvidenceTest`'s two new cases (`c91da9c6`). Side effect: M1-M8's VERIFIED.md would now be refused if checked. |
| 17 | Met, with one reinterpretation and one unpinned edit | See 1c below. |
| 18 | **Criterion text met; the H21 harvest item is NOT fully closed** | See 1d below. |
| 19 | **Partly: the dump is delivered, and the conclusion is still worded past its evidence** | See 1e below. |

### 1a. Criterion 1 (`16005918` M11.23 and `34d229e6` M12.24)

The measurement files exist and record the rig, M=1,000 drain times, the trigger `202` apart from the drain (`m11.23-partition-visibility.md` table), and RustFS memory across consecutive runs. What is wrong is the outcome and the causal claim.

- **None of outcomes (a), (b) or (c) describes the final state.** M11.23 recorded outcome (c). M12.24 then raised the fixture's `mem_limit` from 256m to 1g (`docker-compose.test.yml`) and observed 3 of 3 green at the unchanged bound. This is "(c), closed by a fixture change". It is not (a): (a) attributes M11.19's gap to the 4-core container, and M11.23 attributes it to memory instead, without ever running on that container. VERIFIED.md must say so rather than pick a letter.
- **"The cause is the fixture's memory" is a correlation on small samples, and the samples do not order cleanly by limit:**
  - 256m after the join fix: F1-F3, 3 of 3 green (1,635 / 1,837 / 2,157 ms);
  - 512m in M11.23: D1-D3, 3 of 3 green;
  - 512m in M12.24: 1 of 5 over (run 6, 4,074 ms at 90%, while run 4 at 93% was green);
  - 1g: 3 of 3, but run 9 at 2,095 ms is 405 ms from the bound, and usage grew 462 → 685 → 709 MiB with no plateau shown.

  M12.24's own P2 says the same.
- **The SPEC's warning applies to this explanation too.** The SPEC warned that "rig capacity" cannot explain a regression on M9's own rig. The same holds for fixture memory:
  - M9 measured 2,209 ms at 256m (`m9.12-partition-visibility.md:25,40`), on the same rig and the same RustFS 1.0.0.
  - `mem_limit: 256m` was unchanged from `ae5b4c47` (M9.22) until `34d229e6`.

  Either something since M9 pushes RustFS to ~94%, or M9's 2,209 ms was one draw from the same wide distribution. Nothing measured tells these apart.
- **Bisection step 5(b) never completed.** The variant without `CommitChargingBinStore` has one valid run (4,197 ms) and two lost stores (m11.23 B1-B3).
- **About 1.03 s of the 2.5 s bound goes to the trigger write, "not investigated further"** (m11.23 Conclusion). M12.24's table does not record `trigger202` for runs 7-9.

**Recommended evidence line:**

> OBSERVED green 3 of 3 at 1g (M12.24) after OBSERVED-NOT at 256m (M11.23); cause attributed to fixture memory by correlation, not shown; plateau not shown; variant B inconclusive; I did not run it.

### 1b. Criterion 14 (`467127e2` M12.12 and `d58c6786` M12.26)

Production is fixed where `BinStorePlugin.java:401` calls `subscriptions.holdFailuresWith(threadPool::relativeTimeInMillis)`, which sets `fetchRetry = fetchRetry.withClock(...)` (`NodeSubscriptions.java:241-250`). Outside that wiring the old behaviour silently returns:

- `SegmentFetchRetry.DEFAULT` has no clock (`SegmentFetchRetry.java:58-60`).
- Both of `ConsumerClient`'s shorter constructors use it (`ConsumerClient.java:127,156`), and so does `NodeSubscriptions`' three-argument constructor (`NodeSubscriptions.java:275-280`).
- Without a clock, `backingOff()` is `false` (`SegmentFetcher.java:272`).

So any consumer-library user, or any `NodeSubscriptions` built without that call, keeps the M12.12 defect: live waits out every catch-up backoff. This is F3's silent-default shape again (E3).

The `fetchRetry` field is also mutated after construction and must be set "before the first shard starts a client". That ordering is unpinned (M12.26 T3).

### 1c. Criterion 17: every source finding and its disposition

| Item | Source finding | Disposition |
|---|---|---|
| H18 | M11.16 T1 | Closed: `PushQueuePinsTest#pushesStillQueuedWhenTheDrainBoundRunsOutAreCountedAbandoned` and `#theProductionDrainBoundIsFiveSeconds` (M12.15). |
| H19 | M10.27 P2 | **Closed by a different remedy.** The finding asked for binding per registry. M12.16 unbinds on close and keeps the process-wide static `BOUND` (`GovernorMetrics.java:44`), so with several pods the gauges still read the last-bound one. Test: `GovernorMetricsUnbindTest`. VERIFIED.md should say "reinterpreted". |
| H20 | M11.12 R1 | Closed: `NodeLocalStoreReaderDeadlineResidueTest#anotherFailureMentioningADeadlineAfterExpiryIsStillNamedAsTheBodyDeadline`. |
| H20 | M11.13 R4 | Closed by a named file edit (`offsetLock`) with **no test**. M12.17 T2: removing the lock passes `:client:test`. |
| H22 | M10.27 T3, M11.22 T1, M11.4 T1, M11.6 T2, M11.8 T5, M10.30 T2 | Closed by named tests (M12.19a). I confirmed all six test files exist. |
| H22 | M10.25 T1/T2, M10.28a T2/T3, M10.28b T1/T3 | Closed (M12.19b). The oversize-failure half of M10.25 T2 is dropped with its reason. |
| H22 | M11.9 T1, M10.24 T1, M10.36 T1 | Closed (M12.19c). |
| H22 | M10.34 T1 | Dropped with a measured reason. |
| H22 | M10.35 T1 | Dropped. It opened **M12.27, still open**. |
| H23 | All 11 findings | Edited (M12.20). |
| H24 | M11.12 T2, M11.13 T5 | Closed (M12.21). |
| H25 | M10.37 T3/T4 | Closed by `GateScannerEdgesTest` (M12.22). |

All 33 source findings have a disposition.

### 1d. Criterion 18 (`7123c46c` M12.18)

- `AssemblyReadWorkloadAttributionTest` has premises that each step issued exactly one GET (lines 133, 150, 164).
- `AdminCostAssemblyTest#topAndTheBackendsPricesReachTheRouteWithTwoIndices` exists.
- **M11.5 T2, H21's third source (the periodic top-K log's price wiring, `Assembly.java:330` → `CostReporting.scheduleTopK`), is not closed.** The commit body says so, and M12.18 T2 says `CostTable.free()` passed there survives every test.
- **No backlog row carries it.** `grep "M11.5 T2" backlog.md` matches only the M12.18 row.
- Criterion 17 excludes H21, so no mechanism would have caught it. The completion condition ("every item of M11's review harvest is either a closed M12 row or dropped with its reason") is therefore **not met for H21**.
- Also: the catch-up GET charged whole to unattributed passes (M12.18 T1), because step 3 asserts only the sum.

### 1e. Criterion 19 (`773d70d1` M11.25)

- **The dump:** `junit.jupiter.execution.timeout.threaddump.enabled` is set in the conventions plugin (`java-conventions.gradle.kts:138`).
- **It cannot show the threads most likely to hang.** Virtual threads are excluded (commit body), and `DefaultIngest`'s flusher and pusher are virtual. The dump shows the test helpers, not production.
- **The starvation candidate was "confirmed as a susceptibility"** by `CommitInFlightPoolStarvationTest`, which fills the pool on purpose. That is not the recorded occurrence.
- **The backlog row contradicts itself.** It opens "**The cause found, … measured**" and then says "Which of the two caused the one recorded timeout is not recoverable". The 20 s setup is measured; its role in the one timeout is not. VERIFIED.md should use the second phrasing.
- **The more important finding is buried.** `GatedCommit` counted `entered` down on the lease write, so "the flush reached its commit" held with no flush. Three `DefaultIngestTest` cases constrained nothing from when they were written until `773d70d1`, and a class that took roughly 60 s per run went unnoticed across milestones because `checkSuiteTime` is manual.

### In scope, quietly not done

- **M11.5 T2** (above).
- **The SPEC's cost table has no row for M12.26**, which changes when a consumer re-issues a segment GET. Its commit states no cost either (E7).
- **The roadmap's M12 row still says "decomposed into M12.0-M12.23 (M12.24 opened by M11.23)".** M12.19a-c and M12.25-M12.28 exist. This is for the close.
- **Open rows at review time:** M12.27 (a subscription connection seen open after close), M12.28 (`NodeProcess` port race), and M12.23 (Close M12).

### Delivered, never in scope

Nothing large. Small additions:

- M11.23 fixed a `PartitionVisibilityIT` writer-join defect (a writer still running surfaced as "999 of 1000"). It is a needed fix and is disclosed.
- M12.26 changed the public `SegmentFetchRetry` record's shape (a fifth component). Source-compatible through the kept four-argument constructor, but a public client-library API change the SPEC did not name.

---

## 2. Erosion across commits

**E1 (moderate): the split headroom was spent within the milestone that bought it, and M11's other split has already failed.** Line counts per commit, measured with `git show <c>:<file> | wc -l`:

| File | Start of M12 | After the split | HEAD | Growth in M12 after the split |
|---|---|---|---|---|
| `DefaultIngest.java` | 696 | 570 | **592** | M12.2 +21, M12.3 +10 (8 of them the test seam `waitingForRoom`, `DefaultIngest.java:395-402`) |
| `Assembly.java` | 628 | 585 | **594** | M12.5 +8, M12.16 +1 |
| `BulkService.java` | 628 | 494 | 533 | M12.5 +31, M12.10 +8 |
| **`ConsumerClient.java`** | 555 | — | **606** | M12.12 +9, M12.17 +34, M12.20 +7 (a docs-only commit), M12.26 +1 |

- `ConsumerClient` is the file M11.1 split from 700 to 545. M11 VERIFIED.md criterion 1 notes it "still under" 600. It is now over 600, ungated, because `SPLIT_CEILINGS` names only the three.
- Near the global 700: `LocalSequencer` 700 (exactly at the limit; the gate refuses `> 700`, `RepositoryGatesTask.kt:118`), `SubscriptionService` 695, `HttpSubscriptionTransport` 692, `SegmentProxy` 671, `NodeSubscriptions` 655 (+12 in M12).
- This is M10's F5 and M11's F1 a third time. The difference is that the gate now makes it fail loudly for three files only.

**E2 (moderate): one concept, two implementations.**
- The consumer's backoff is now either a debt or a due time, chosen by a nullable clock (`SegmentFetcher.java:236-272`, four `clockMillis() == null` branches).
- There are also two jitter policies on the same retry loop:
  - two-sided `[0.5b, 1.5b]` for the consumer fetch and reconnect (`HttpSubscriptionTransport.java:604`, used by `SegmentFetcher.java:219`);
  - up-only `(b, 1.5b]` for the node hold (`NodeSegmentSource.java:295-302`), adopted in M12.11 precisely because a shortening jitter "ran the consumer's attempts out".

  I did not find a defect: held answers are not attempts (M10.28a). But the reasoning that forced up-only jitter at the node was never applied to the consumer's own jitter in writing.

**E3 (moderate): silent defaults came back in four places, in the milestone that removed them from `Ingest` (M12.2).**
- `SegmentFetchRetry`'s null clock (1b above).
- The two-argument `ConsumerDeliveryQueues(capacity, decoder)` defaults `catchUpBackingOff` to `() -> false`. Only `DeliveryQueuePermitTest:58,76` calls it.
- `ServerConfig`'s older constructors leave `adminCost` off (M12.6 T1).
- `IndexQuotas`' three-argument constructor passes no aliases (M12.13).

Each is a production constructor whose only caller is a test or a legacy path, and each is a way for a second write or read path (fast mode) to inherit the wrong behaviour silently.

**E4 (moderate): test seams added to production classes.** About ten seams in one milestone. The one in `DefaultIngest` costs 8 of its 8 remaining lines of headroom.
- `DefaultIngest.waitingForRoom` (`:395`) and `UnflushedCeiling.hasWaiters` (M12.3)
- `IndexQuotas.bucketCount` (public, `:174`) and `FrontDoor.quotas()` (`:395`) (M12.4)
- `RoutedIngest.waitingForRegistration` (public, `:367`, no production reader) (M12.10)
- `NodeLocalStoreReaderClient.deadlined` (package-private, `:150`) (M12.17)
- `NodeLocalStoreReaderClient.pendingDeadlines(Duration)` (`:70`) (M12.21)
- `PushQueue.abandoned()` (`:159`, no production reader, M12.15 P1)
- `NodeSubscriptions.fetchRetry()` (`:253`, "for a wiring test") (M12.26)
- the two-argument `ConsumerDeliveryQueues` constructor (M12.26)

**E5 (low): test harness debt.**
- The 23 doubles M12.2 had to edit are now 25 copies of `concreteIndex` in 21 test files, with no shared test base. The next abstract method on `Ingest` is another 25-file edit.
- The tests also couple more tightly to implementation details:
  - `ConsumerRetryDefaultsTest` detects a waiter only if it is BLOCKED on a `synchronized` monitor, read through `ThreadMXBean` (M12.21 T2);
  - `FileSizeCeilingTest`'s wiring case matches the call's text (M11.24 T4).

**E6 (moderate): ADR drift.**
- ADR-0078 decision 2a is contradicted by M12.4 (1, criterion 6), and no amendment was written.
- ADR-0078 decision 4, as amended by M12.13, describes "a bucket … made before its index's registration was known". Since M12.4 no bucket is made for an unknown name (`IndexQuotas.java:189`), so the sentence describes pre-M12.4 behaviour, apart from an alias added after a bucket exists.
- M12.10 adds a new `429` cause (`MAX_EXPLICIT_WAITERS_PER_INDEX` = 8, hard-coded). No ADR or standard names it: grep across `decisions/` and `standards/` finds nothing.

**E7 (moderate, process): the commit record thinned mid-milestone.**
- From `034fe7cc` (M12.13) through `d58c6786` (M12.26), **15 consecutive commits carry neither the gates-run line nor a `Cost:` line**. M12.0-M12.12 all carry both (my grep over every body).
- M11's VERIFIED.md relied on "the authoring session ran X for every commit". For those 15 commits the record cannot show it. The hooks enforce `checkReviewed`/`checkTdd` anyway, unless skipped.
- **M12.26 changes the consumer's GET retry timing with no cost statement** (git.md rule 5), and the SPEC's cost table has no row for it. Its P2 says attempts stay capped, which is probably cost-neutral, but that is not stated.

**E8 (low): request rates.** I found no new store call site in the production diff.
- M12.11 only lengthens holds.
- M12.12 allows up to 2× attempts per consumer across its two lanes: a constant factor per shard consumer, bounded per node by the hold. It is stated, but the cross-lane bound is not asserted (M12.12 T2).
- M12.26 keeps `maxAttempts`.
- Non-negotiable 6 holds as far as reading can show. No trend is available without the meter.

**E9 (low): harness and fixture debt I found running things.**
- M12.14 records that `./gradlew -p buildSrc test` has **154 failures on the development rig** (legacy shell-script tests on Windows), "the same 154 without this change". A permanently red test task on M9's rig is a broken tree in all but name. I did not run it.
- The compose `mem_limit` is enforced by nothing:
  - `RepositoryGateChecks.testBudget` (`:153-164`) reads `gradle.properties` and the conventions plugin, not `docker-compose.test.yml`;
  - `build.md:38,43` still names the retired `scripts/check-test-budget.sh` (M12.24 P4/T1).
- In my test run, **`PeerCommitTest#aFRAMEBiggerThanTheCapIsREFUSEDWHILEItIsREAD` took 38.2 s**, although its comment says a correct reader "answers 413 in milliseconds". Its `catch (RuntimeException refusedMidStream)` branch asserts only `isNotNull()` (`PeerCommitTest.java:238-246`), so any client exception passes. It predates M12, but it has the same shape M11.25 found: a slow case that may not be observing what it names.

**E10 (low): one-caller extractions.** `StoreStack` (46 lines), `ServingTerm` (58), `BatchFlusher` (87), `PlacementParser` (86), `Admitted` (74) and `CostReporting` each have exactly one caller. This is expected of a no-behaviour-change split and is not a defect. But the headroom they bought is nearly gone (E1), so the next split should cut along a seam fast mode needs, not along whatever brings the line count down.

---

## 3. Harvest of the minors

Every open minor in an M12-window commit body is listed once. **ROW** means a proposed M13 backlog row, keyed R1-R17 (listed below the table). **DROP** gives its reason. A minor fixed or taken into a later M12 commit is marked CLOSED.

| Commit | Minor | Disposition |
|---|---|---|
| M11.23 `16005918` | R5: the 230-254 MiB band (readings to 223.7) | DROP: the point stands, and M12.24 superseded the memory data |
| M11.23 | R6: the F3 reading not in the per-run file | DROP: appended since (body) |
| M11.23 | T1: trigger and write timings printed, never asserted | ROW **R2** |
| M11.24a `20c6c99c` | P1: the row named the wrong move | DROP: the row's done note corrects it |
| M11.24a | P2: `ResendOnceSequencer` comment names `DefaultIngest.flushLocked` | ROW **R16** |
| M11.24a | P3: three capturing lambdas per append, unmeasured | DROP: performance.md says measure first. Fold into fast mode's append-path benchmark if one is written |
| M11.24a | T1: four ceiling mutations survive | CLOSED by M12.3, except M12.3's own T2/T3 below |
| M11.24b `ebb0ba8b` | P1: the ledger owner is `StoreStack` | CLOSED by M12.1 |
| M11.24b | T1: `ServingTerm` guards | CLOSED by M12.19c (serving half), with the incomplete-chain half dropped there with its reason |
| M11.24c `18af4177` | P1: quota admission did not move | DROP: the row says so |
| M11.24c | T1: negative partition | CLOSED by M12.19c (`BulkNegativePartitionTest`) |
| M11.24 `ea6c2c03` | T4: the wiring case matches text | ROW **R13** |
| M12.2 `b4cc39a5` | T2: `LaneOvertakeTest.BufferedWitness` and `BulkServiceQuotaTest.Recording` run `buffered` in the pre-M11.7 order | ROW **R11** |
| M12.3 `6866078c` | T2: `flushEnded(generation-1)` survives | ROW **R12** |
| M12.3 | T3: `!pending.isEmpty()` unpinned | DROP: no deterministic test reaches the transient window (stated) |
| M12.3 | T4: `waitingForRoom` hook unchecked | ROW **R12**; also see R5 |
| M12.4 `806cdf78` | P2: an unknown-at-admission first chunk is outside the per-index cap | ROW **R4** (ADR-0078 amendment, and bound it or state it) |
| M12.4 | T5: the owed-tally reset unpinned (overcharge) | ROW **R12** |
| M12.4 | T2: admission racing a sweep | ROW **R12** |
| M12.4 | T3: idle boundary and once-per-expiry limit | DROP: bucket churn only, no correctness or security effect |
| M12.5 `b1928241` | P1: "and N more" counts refusals | ROW **R8** |
| M12.5 | T1: suffix unseen in a line | ROW **R8** |
| M12.5 | T2: name as sent vs concrete | ROW **R8** |
| M12.5 | T3: overflow case indistinguishable | ROW **R8** |
| M12.6 `5d3ed581` | P1: docs | CLOSED by M12.20 |
| M12.6 | T1: old `ServerConfig` constructors | ROW **R5** (remove them rather than pin them) |
| M12.7 `fba345aa` | P3: character literals | CLOSED by M12.22 |
| M12.7 | P4: a status READ refused as a send | DROP: errs toward refusing, and no production reader exists |
| M12.7 | T4: `'"'` before a 429 | CLOSED by M12.22 (`GateScannerEdgesTest`) |
| M12.8 `ce27e25b` | P3: the top-K line hides undecodable registrations | ROW **R8** |
| M12.8 | T3: JSON separator and sort | DROP: cosmetic, and one case covers the shape |
| M12.9 `198f5d10` | P1: toNanos wording | CLOSED by M12.20 |
| M12.9 | T1: parser at exactly `PT24H` | ROW **R12** |
| M12.9 | T2: reporter cases live in `:server` | ROW **R12** |
| M12.10 `5c3b991a` | P1: counted as an admission refusal | ROW **R9** |
| M12.10 | P2: checked after the first chunk | ROW **R9** |
| M12.10 | P3: cap per name, 8×N across names | ROW **R9** |
| M12.10 | T1-T4: the zero-count entry, one pod-wide key, the interrupt path, the refusal count | ROW **R9** |
| M12.11 `58a35f05` | P1: up to +15 s recovery at the ceiling; stale javadoc | ROW **R16** (the javadoc), and state the latency in the M13 spec's NFR-7 reasoning |
| M12.11 | T1: fixed seed in `NodeSubscriptions` survives | ROW **R10** |
| M12.12 `467127e2` | P3: M12.26 has no numbered criterion | DROP here: M12.26 is done, and M12.23 must enumerate it under criterion 14 (instruction to the close) |
| M12.12 | T2: the cross-lane 2× not asserted | ROW **R10** |
| M12.13 `034fe7cc` | T2: sorted-alias precedence unpinned | ROW **R12** |
| M12.14 `c91da9c6` | T1: `MilestoneVerifiedTask` prefix | DROP: cosmetic, and the message names the file |
| M12.15 `9eee4b1b` | P1: `abandoned()` not reachable from `DefaultIngest` | ROW **R14** |
| M12.15 | T1: sentinel re-queue unpinned | ROW **R14** |
| M12.15 | T2: the byte release unpinned | ROW **R14** |
| M12.15 | T3: `pusher.interrupt()` unpinned | ROW **R14** |
| M12.15 | T4: the 5 s pin reads the constant | ROW **R14** |
| M12.16 `89a70553` | P1: an assembly failing after bind stays bound | DROP: a failed assembly ends the process |
| M12.16 | P2: multi-pod gauges read the last-bound | DROP: a process runs one pod. VERIFIED.md must still call H19 reinterpreted (1c) |
| M12.17 `334dc298` | P1: the gap report through a field the caller must clear | ROW **R15** |
| M12.17 | T1: `expiredOr` self-recognition unpinned | ROW **R15** |
| M12.17 | T2: `offsetLock` unpinned | DROP: no deterministic test makes the reads disagree (M11.13 T4) |
| M12.18 `7123c46c` | P1 / T2: M11.5 T2, the top-K log prices at the assembly | ROW **R3** |
| M12.18 | T1: a catch-up GET charged whole to unattributed survives | ROW **R3** |
| M12.21 `689cc8da` | T2: detection limited to a `synchronized` monitor | DROP: a `ReentrantLock` would make the test slow, not wrong |
| M12.22 `ffdee9ad` | T4: `skipQuoted` stops at a newline | DROP: differs only on source that does not compile |
| M11.25 `773d70d1` | T2: dump property pinned only in `:ingest` | ROW **R13** |
| M12.24 `34d229e6` | P2: thin case for 1g over 512m | ROW **R2** |
| M12.24 | P4/T1: `mem_limit` unenforced; build.md names a retired script | ROW **R7** |
| M12.24 | T2: a fourth or later run unmeasured | ROW **R2** |
| M12.26 `d58c6786` | P2: `relativeTimeInMillis` cached (~200 ms) | DROP: latency only, bounded by one refresh, attempts capped |
| M12.26 | T3: the wiring test reads the node's policy, not a client's | ROW **R5** |

M12.0, M12.1, M12.19a-c, M12.20 and M12.25 left no open minor.

**Open rows:**
- **M12.27** (a subscription reader's connection seen open 10 s after close, 3 of 7 runs): carry to M13 as an early task. It sits on the FR-6 live read path that fast mode's readers share, and an interrupt landing between polls is a production leak question, not a test question.
- **M12.28** (`NodeProcess` probe-then-hand-over port race): carry to M13 as an early task. It already cost one of the nine M12.24 runs, and fast mode's latency evidence will run on the same harness.

**Proposed M13 rows:**

- **R1** Before fast mode's first code task: re-split `DefaultIngest` (592/600) and `Assembly` (594/600) along the seam the WAL write path needs. Add `ConsumerClient.java` (606) back under a named ceiling after re-splitting it. Decide the same for `SubscriptionService` (695), `HttpSubscriptionTransport` (692) and `NodeSubscriptions` (655) if fast mode's reader path lands there.
- **R2** `PartitionVisibilityIT` as a distribution:
  - ten or more runs at 1g on one container, recording `trigger202` and RustFS memory every run, to find the plateau;
  - complete bisection variant B (without `CommitChargingBinStore`);
  - investigate the ~1.03 s trigger write (likely the 1 s TTL-bound forward) that takes 41% of the bound;
  - assert `trigger202 <= drainEnd` so the printed timings are constrained.
- **R3** Close M11.5 T2: an assembly test that fails with `CostTable.free()` passed to `CostReporting.scheduleTopK`. In `AssemblyReadWorkloadAttributionTest` step 3, assert the catch-up GET's per-index share, not only the sum.
- **R4** Amend ADR-0078:
  - decision 2a, for M12.4's known-names rule, the unknown-name ticket that binds past the cap, and idle expiry;
  - decision 4's stale "bucket made before registration" sentence;
  - either bound the first chunk of an unknown-at-admission request by the cap, or state the bound it has.
- **R5** Remove silent defaults. `SegmentFetchRetry`: make the clock required, or make due-time the only backoff mode. Also:
  - `ConsumerDeliveryQueues`' two-argument constructor;
  - `ServerConfig`'s older constructors;
  - `IndexQuotas`' three-argument constructor.

  Pin that a client started after `holdFailuresWith` receives the clocked policy (M12.26 T3).
- **R6** Delete or quarantine the 154 legacy shell-script tests that fail `./gradlew -p buildSrc test` on the development rig, so the task is green there.
- **R7** Extend `RepositoryGateChecks.testBudget` to read `docker-compose.test.yml`'s `mem_limit` values and the build.md ceiling (a file predicate, gate-design rung 3), and fix build.md's references to `check-test-budget.sh`.
- **R8** Fix the top-K cost line:
  - "and N more" says refusals or counts distinct indices;
  - pin the suffix in a line;
  - post a refusal through an alias to pin the concrete name;
  - name the count of undecodable registrations the line leaves out.
- **R9** Explicit-partition wait residue:
  - its own refusal counter, or a corrected description;
  - check the cap before the body is opened, as every other 429 is;
  - bound the waiters across names, not only per name;
  - pin T1-T4.
- **R10** Consumer and hold pins: two nodes built through `holdFailuresWith` draw different holds (criterion 13's "two nodes"); and the cross-lane 2× attempt bound is asserted.
- **R11** `LaneOvertakeTest.BufferedWitness` and `BulkServiceQuotaTest.Recording` delegate the buffered form in production's order.
- **R12** Admission and quota residue pins: M12.3 T2/T4, M12.4 T2/T5, M12.9 T1/T2, M12.13 T2.
- **R13** Harness wiring pins behaviourally: `verify()` wiring cases stop matching the call's text (M11.24 T4, and the same shape in the other gate tests). Pin the thread-dump property for every test task (M11.25 T2).
- **R14** `PushQueue` abandonment: exposed as `DefaultIngest` exposes dropped and undeliverable pushes, and M12.15 T1-T4 pinned.
- **R15** `ConsumerClient` returns the gap report instead of a field the caller clears, and `expiredOr` self-recognition is pinned.
- **R16** Docs: `ResendOnceSequencer`'s stale reference, and `holdFailures`' javadoc above the ceiling.
- **R17** (from E7 and E9) Two items:
  - `checkCommitMessage` requires a `Cost:` line, so the body states it even when it is "none". This is a file predicate on the message (rung 3), not a reviewer instruction.
  - `PeerCommitTest#aFRAMEBiggerThanTheCapIsREFUSEDWHILEItIsREAD`: find why it takes 38 s where the comment promises milliseconds, and replace its vacuous `isNotNull()` catch.
- **R18** (low) A shared forwarding test base for `Ingest` doubles, so the 25 copies collapse.

---

## 4. Re-plan

**M13 is still the right milestone, but it should not open on fast mode's first code task.** Four things should come first, as M13's opening rows rather than a new milestone:

1. **R1, the headroom.** `DefaultIngest` has 8 lines and `Assembly` 6 below the gate (HEAD). Fast mode's WAL write path and its composition cannot land without an immediate split. Splitting *along the fast-mode seam* (a write-path interface `DefaultIngest` delegates to) is cheaper before that code exists than after. ConsumerClient is already over M11's split.
2. **M12.28, the port race.** Fast mode's evidence is T3 latency on the same `NodeProcess` harness, and M12.24 already lost a run to it.
3. **R2, the measurement method.** Fast mode claims milliseconds (FR-17: ~1.5-6 ms), on a fixture whose latency tail M12 showed depends on its memory limit and was characterised from 3-run samples. Settle how a latency is recorded (distribution, trigger/drain split, fixture memory beside the number) before the first fast-mode number is taken.
4. **M12.27.** Fast mode's live readers share that connection path.

Then fast mode, with ADR-0080/H16 weighed in its spec as the M12 SPEC already assigns. R3-R18 interleave. None blocks fast mode, although R4 and R5 are hazards of the same kind as H2 and H3: a second path inheriting a default or an unstated admission bound. Put them early.

AWS S3 latency, TTFB and billed dollars remain NOT-RUN since M9. The cost meter still does not exist, and fast mode adds a write path whose request rate it cannot measure.

---

## 5. Research corpus

M12 changed nothing in `docs/research` (`git diff --stat f4bae4dd^ HEAD -- docs/research` is empty). It taught five things the corpus should say. I did not edit any file, as instructed.

- **`40-implementation/03-benchmarking-plan.md:59`** still names "MinIO (Testcontainers)". The T3 fixture has been RustFS in docker-compose since M9.22 (`ae5b4c47`). Add that:
  - a T3 latency on RustFS depends on the fixture's `mem_limit` (M11.23, M12.24);
  - a single run is not a baseline, and M9's 2,209 ms was one;
  - a bound's clock should be split into trigger and drain, since the trigger took ~1.03 s of 2.5 s.
- **`30-design-space/11-multi-tenancy-and-security.md` §2 mechanism 1** needs a revision banner:
  - buckets exist only for registered indices and expire when idle and full (M12.4);
  - an alias-named override applies to its concrete index (M12.13);
  - refusals are exported as two unlabelled counters with names in the top-K line (M12.5);
  - the explicit-partition wait is capped per index (M12.10).
- **`30-design-space/10-client-library-and-fetch-modes.md`** says nothing about fetch retry or lane interaction. Add:
  - the live and catch-up lanes retry separately (M12.12);
  - a backing-off catch-up must yield its quantum turn, which needs a host clock because a debt-style backoff paid only by waiting starves a yielding lane (M12.26).
- **`30-design-space/08-failure-domains-and-resilience.md`**: a jitter on a hold in front of a bounded-attempt client must only lengthen. M12.11 round 1 measured a `[b/2, b]` jitter exhausting the consumer's attempts in 6 of 8 seeds over a 75 s outage.
- **`30-design-space/15-cost-governor.md`**: `/admin/cost` is opt-in (`admin.cost.enabled`, M12.6) until the producer port is authenticated.
