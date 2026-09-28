# M12 — Measure and clean the default path before fast mode

## Completion condition

The roadmap's M12 row, restated: **`PartitionVisibilityIT`'s drain overrun
(M11.23) is either root-caused and fixed, or measured and localised with the bound
untouched; `Assembly`, `DefaultIngest` and `BulkService` are each below 600 lines,
and a gate keeps them there; the three hazards a second write path would trigger
are closed: no production constructor makes a ledger nothing reads, `Ingest` has
no silent defaults, and the unflushed-bytes ceiling has no reset race; and every
item of M11's review harvest is either a closed M12 row or dropped with its reason
below.**

M12 collects the obligations M11 handed it:

| Obligation | Assigned by |
|---|---|
| Open rows M11.23, M11.24, M11.25 | M11/VERIFIED.md § Gates, criteria 1 and 14 |
| Harvest H1–H25 | M11/VERIFIED.md § Harvest for the specification of M12 |
| The re-plan: M11.23, the splits, H2–H4, then the rest | M11/VERIFIED.md § Re-plan |

⚠️ **M12 IS NOT FAST MODE.** The roadmap gave M12 fast mode (FR-17) "after the
default path is measured". M11's milestone review found the default path measured
but not clean: an unexplained +28–31% on the drain-bound visibility latency, on the
commit path fast mode would share (M11.23); the three files fast mode lands in at
or over their limits (H1, M11.24); and two hazards a second write path would trip
at once (F2's ledger-less constructors, F3's silent `Ingest` defaults). Fast mode
moves to **M13**, unchanged. ADR-0080's compact per-(node, segment) event, a wire
change, moves to **M13 or later**, after the own-zone relay alternative is weighed
(H16, below).

⚠️ **THE RIG IS M9's RIG**: this workstation matches the one
`measurements/m9.12-partition-visibility.md` records -- Intel Core i5-13600KF,
14 physical / 20 logical cores, Windows 11 Pro build 26200, Temurin 25.0.4.1+1.
M11.19's 2,830-2,889 ms were measured on a different machine, a 4-core Linux
container. So M11.23 can do what its row asks first: measure on M9's rig. M11.23
re-records the rig beside every number it measures, so a result is never compared
across rigs without saying so.

## Requirements

| ID | What M12 does to it |
|---|---|
| **NFR-7** (end-to-end p99 < 3× the flush window), **FR-4** (ADR-0058's partitioned ack) | The drain-bound visibility latency after a partition heal is measured on M9's rig and the M9 bound of 2,500 ms held, never widened (M11.23) |
| **FR-21** (cost governor) | Attribution cannot silently lose a read path (H2); refusals exported pod-level (H6); quota buckets bounded (H5); `/admin/cost` hardened (H7, H9, H10) |
| **FR-15** (per-index admission) | The explicit-partition wait bounded per index (H11); quota overrides by alias (H14) |
| **NFR-6** (bounded memory) | The unflushed-bytes ceiling holds under overlapping flushes (H4) |
| **FR-6, NFR-4** (read modes, read rate) | The node failure hold jittered (H12); live and catch-up retry state separated or justified (H13) |
| **FR-17** (fast mode) | **Not served.** Moved to M13 |

## Scope

- M11.23: measure, bisect and — if a regression is found — fix the drain overrun.
- M11.24: split `DefaultIngest`, `Assembly` and `BulkService` below 600 lines with
  no behaviour change, then a gate that refuses any of the three at 600 or more.
- H2–H4: the three hazards.
- H5–H15, H17–H25: the remaining harvest, each a named row below or dropped with its reason.
- M11.25: the `DefaultIngestTest` class-timeout flake.

### Not in scope

- **Fast mode (FR-17)** — M13.
- **ADR-0080's compact per-(node, segment) event** — a wire-format change, M13 or
  later; H16's relay alternative is weighed in that milestone's spec, before the
  wire change it would replace.
- **Splitting the other files over 600 lines** (`LocalSequencer` 700,
  `SubscriptionService` 695, `HttpSubscriptionTransport` 692, `SegmentProxy` 680
  and the rest). M11's F1 names them, but only the three fast mode lands in are
  M11.24's; the 700-line file-size gate still bounds the rest.
- **Widening `PartitionVisibilityIT`'s 2,500 ms bound.** Never. If M11.23 cannot
  meet it on M9's rig, it says so with the measurement.
- **AWS S3 latency, TTFB and billed dollars** — NOT-RUN since M9; no account.

## Design

### M11.23 — the drain overrun, bisected rather than guessed

M11.19 measured M=1,000 drained in 2,889 and 2,830 ms on a 4-core container
against M9's 2,209 ms on its 20-core rig -- this workstation. Two changes since M9
sit on that path:
the cost governor wrapping the commit route's inbox drain (M10.11, M10.26), and
`CommitChargingBinStore` decoding every commit-log delta it passes on (M11.22).
The task:

1. **Record the rig** — CPU model, physical and logical cores, OS and Docker
   version — in the measurement file, beside every number.
2. **Measure as-is on M9's rig first**, and **time the post-heal trigger write's `202`
   separately from the drain** (the IT's clock starts before `heal()`; M11.19
   review T1), so a slow trigger write is not read as a slow drain.
3. **Check RustFS memory across runs** before anything touches `mem_limit`: the
   fixture's container outlives a run, and two of three M11.19 runs were
   OOM-killed at 256m. Record the container's memory at the start and end of
   consecutive runs.
4. **If M9's rig still drains near its 2,209 ms** (under the 2,500 ms bound), the
   M11.19 gap was the 4-core container's capacity: recorded with both machines'
   numbers, the IT green here, and no code change.
5. **Otherwise bisect**, each variant a measured run, never a code change landed on
   a guess: (a) the drain without the governor wrapper; (b) the stack without
   `CommitChargingBinStore`. Either variant closing the gap names the regression.
   ⚠️ On M9's own rig "rig capacity" is not an available explanation for a
   regression against M9's own 2,209 ms.
6. A regression found is fixed in production code with a test that pins the cost
   it removed (for the governor or the decode, an unconditional per-delta operation
   made lazy or skipped when nothing reads it). The bound is unchanged in every
   outcome.

The bisection variants are measurement-only switches used by the IT's harness, or
temporary patches measured and reverted. ⚠️ Nothing that weakens a production path
is committed to make the IT pass.

### M11.24 — the splits, and the gate that keeps them

Three commits, no behaviour change in any of them, the full `./gradlew test` green
after each:

- **M11.24a** `DefaultIngest` (696): the unflushed-bytes ceiling and flush
  enqueue into their own class.
- **M11.24b** `Assembly` (628): the ledger, quota and cost wiring into their own
  assembly helper, as M11.1 split out before.
- **M11.24c** `BulkService` (628): `Admitted` and quota admission out.

Then **M11.24**, the gate: a repository-gate predicate naming the three files and
refusing any of them at 600 lines or more, pinned by a case for each file in the
harness tests. ⚠️ A named list rather than a global 600 limit, because a global one
would fail the tree today on the dozen files M12 does not split. Lowering the
global 700 to 600 is a later milestone's decision.

⚠️ **The split order matters.** H4's race lives in the code M11.24a moves, so
M11.24a moves it unchanged and H4 fixes it in its new home.

### The three hazards

- **H2 (M12.1): no ledger-less production constructor.** `SegmentProxy`,
  `DurableCatchUpResponder`, `SegmentPublisher` and `DefaultIngest` each have a
  constructor that makes `new IndexCostLedger()`, which nothing reads: a read path
  built with one issues GETs no report can see, breaking "Σ shares = counted
  requests" silently. Remove those constructors; every caller passes the ledger it
  reports from. Tests that do not care pass one explicitly. Rejected: having
  `/admin/cost` report counted minus charged — it names the gap after the fact
  rather than making it unrepresentable (gate-design rung 1).
- **H3 (M12.2): no silent `Ingest` defaults.** The two `buffered` overloads and
  `concreteIndex` become abstract, so every `Ingest` states them. Rejected: pinning
  the defaults as M11.9 did — the default already caused M11.8's round-1 defect,
  and a pin still lets a new implementation inherit the wrong one.
- **H4 (M12.3): the ceiling has no reset race.** A finishing flush's completion
  sets `inFlightBytes = 0` even when the next flush has already set it, so the pod
  can admit about 2× the ceiling. The completion releases only its own flush's
  bytes; both sides of the "a flush will come" guard are pinned.

### The remaining harvest

Each item is one row in the Tasks table and specified there; the criteria below
say how each is checked. Items dropped:

| Item | Dropped because |
|---|---|
| H1 | It is M11.24, carried under its own ID |
| H16 | ADR-0080's relay alternative and its unmetered per-stream bytes belong with the wire change they would replace, which moves to M13 or later; weighed in that milestone's spec |
| H17 | It is M11.25's own first step (the thread dump on class timeout and a recorded occurrence), so it is M11.25's row |

## Cost impact

| Change | Store requests |
|---|---|
| M11.23 | **none added**; a fix, if one is found, can only remove per-delta work on the commit path, never add a request |
| M11.24, M11.24a–c | none: a move of code |
| H2 (M12.1) | none; every existing request stays charged, now always to the reported ledger |
| H3, H4, H5–H11, H14, H15 | none: admission, memory, quotas, metrics and routing are not store paths |
| H12 (M12.11) | none added; jitter spreads the same number of retries |
| H13 (M12.12) | the consumer's fetch retries ARE a GET and proxy-fetch path: separating live and catch-up state must not raise the retry rate of either above today's per-(node, segment) bound (M10.28b). The row states the retry count per failing segment before and after, and a test asserts it does not rise |
| H21 (M12.18) | none: a test workload |

NFR-1's bound and NFR-3's ceiling are unchanged. No budget moves.

## Acceptance criteria

1. **The drain overrun is measured and localised on M9's rig** (M11.23):
   `measurements/m11.23-partition-visibility.md` (created by M11.23) records the
   rig (CPU, cores, OS), M=1,000's drain time as-is, the post-heal trigger write's
   `202` time apart from the drain, and RustFS container memory across at least
   three consecutive runs. Exactly one of three outcomes is recorded: (a) as-is,
   `PartitionVisibilityIT` is green at the unchanged 2,500 ms bound, and M11.19's
   gap is attributed to its 4-core container with both machines' numbers; (b) it is
   over, the bisection names a regression, the fix lands with a pinning test, and
   the IT is green at 2,500 ms; (c) it is over and stays over after the bisection
   and any fix: the IT is OBSERVED-NOT green on M9's rig, with the numbers, the
   variants measured, and the remainder carried as an open row. The bound is not
   changed in any outcome.
2. **The three files are split and stay split** (M11.24, M11.24a–c):
   `DefaultIngest.java`, `Assembly.java` and `BulkService.java` are each below 600
   lines; the full `./gradlew test` is green after each split; the gate refuses
   each of the three at 600 lines, pinned by a harness case per file.
3. **No production constructor makes a ledger nothing reads** (M12.1): the four
   classes have no constructor calling `new IndexCostLedger()`, and a
   repository-gate predicate refuses `new IndexCostLedger()` in any production
   source outside the one composition root that owns the pod's ledger, pinned by a
   harness case; `Assembly` and every production caller pass the pod's ledger.
4. **`Ingest` has no silent defaults** (M12.2): its `buffered` overloads and
   `concreteIndex` are abstract, pinned by `IngestHasNoDefaultsTest` (T0,
   reflection: none of the three is a `default` method); each implementation
   states them; the test doubles that relied on the default still pass.
5. **The unflushed ceiling holds under overlapping flushes** (M12.3), T1: with
   flush A completing after flush B has been enqueued, the bytes buffered plus in
   flight never exceed the ceiling by more than one append, and B's in-flight bytes
   are still counted after A completes; both sides of the "a flush will come" guard
   are pinned: an append waits when a flush is queued, and proceeds when none is
   queued and nobody waits.
6. **Quota buckets are bounded** (M12.4, H5): no bucket is created for an index
   name the catalog does not know; an idle bucket is expired after its configured
   idle time, measured on an injected clock.
7. **Refusals are exported pod-level** (M12.5, H6): each lane-admission `429` and
   each quota `429` increments its own unlabelled counter; the top-K log line
   names indices refused in its interval; the metric-cardinality gate stays green.
8. **`/admin/cost` is opt-in** (M12.6, H7): unconfigured, the route answers `404`
   (not registered); enabled, it answers as before.
9. **One `429` emitter, enforced** (M12.7, H8): a gate predicate refuses any
   `429` status set outside `BulkService.tooManyRequests`, pinned by a case.
10. **`/admin/cost` survives a malformed registration** (M12.8, H9): with one
    registration that cannot be named, the route answers `200` and reports the
    others, the bad one by id.
11. **`cost.top-k-interval` is bounded and its line names the real window**
    (M12.9, H10): values outside the configured bounds refuse at startup; each
    line states the interval it actually covers.
12. **The explicit-partition wait is bounded per index** (M12.10, H11): K
    concurrent explicit-partition requests to one index waiting for its
    registration, K above the per-index cap, hold at most the cap in the wait;
    the rest are refused `429` with `Retry-After` through the one emitter.
13. **The node failure hold is jittered and pinned** (M12.11, H12): `holdMillis`
    at 55 and 65 failures pinned; two nodes with the same failure count hold for
    different durations under different seeds.
14. **Live and catch-up retry state** (M12.12, H13): separated, or a test that
    fails if sharing them lets a catch-up failure delay a live fetch (the row
    decides which).
15. **Quota overrides by alias** (M12.13, H14): an override named by an alias
    applies to its concrete index.
16. **`checkMilestoneVerified` does not fail open** (M12.14, H15): a VERIFIED.md
    with no criteria section, or with a duplicate criterion number, is refused.
17. **H18–H20, H22–H25** (M12.15–M12.17, M12.19–M12.22): every source finding
    each row lists is, in that row's `done` state, either closed by a named test
    (with its red record) or a named file edit, or dropped with a reason. At
    close, VERIFIED.md enumerates each source finding ID once with its disposition,
    so a finding with none is visible.
18. **One assembled-pod read workload exercises all three GET kinds** (M12.18,
    H21): held, streamed past the hold, and catch-up, on one assembled pod, Σ
    index GET shares + unattributed = counted data-segment GETs × 10^6;
    `AdminCostAssemblyTest` covers `top` and the price wiring with two indices.
19. **The `DefaultIngestTest` timeout is diagnosable** (M11.25, H17): a class
    timeout prints a thread dump; the ForkJoin-starvation candidate is tested, and
    either confirmed and fixed or refuted with the run that refutes it.

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1 | T3 | `PartitionVisibilityIT` (measured, not red-first: a measurement, not a behaviour) | — |
| 2 | harness | `FileSizeCeilingTest` (the gate's case per file) | the gate reading the wrong limit, or skipping a named file |
| 3 | harness | the gate predicate's case (`LedgerlessConstructorGateTest`) | a constructor restored with `new IndexCostLedger()` |
| 4 | T0 | `IngestHasNoDefaultsTest` | a default body restored |
| 5 | T1 | `UnflushedCeilingOverlapTest` | `inFlightBytes = 0` on any completion |
| 6 | T0 | `IndexQuotasBoundTest` | a bucket for any name; expiry never run |
| 7 | T1 | `RefusalCountersTest` | one counter for both; a counter with an `index` label |
| 8 | T1 | `AdminCostOptInTest` | the route registered unconditionally |
| 9 | harness | `SingleTooManyRequestsGateTest` | the predicate skipping a file |
| 10 | T1 | `AdminCostMalformedRegistrationTest` | one bad name failing the report |
| 11 | T0 | `CostTopKIntervalBoundsTest` | an unbounded interval accepted |
| 12 | T1 | `ExplicitPartitionWaitCapTest` | the wait unbounded per index |
| 13 | T0 | `NodeFailureHoldJitterTest` | jitter zero; the 55/65 boundary moved |
| 14 | T1 | named in M12.12's row | named there |
| 15 | T0 | `QuotaPropertiesAliasTest` | an alias override ignored |
| 16 | harness | `MilestoneEvidenceTest` new cases | fail-open on a missing section; duplicate number accepted |
| 17 | per row | named in each row | named in each row |
| 18 | T2 | `AssemblyReadWorkloadAttributionTest` | a GET kind dropped from the sum |
| 19 | T1 | `DefaultIngestTest` under the dump extension | — (diagnostic) |

Suites extended: the cost assertions (criteria 3 and 18), the harness gate tests
(2, 3, 9, 16). The simulation and store-conformance suites are untouched.

## Risks

- **M11.23 may find the gap was the container, not the code.** Outcome (a) of
  criterion 1: recorded with both machines' numbers. Or it may find a regression
  it cannot fully remove: outcome (c), OBSERVED-NOT with the numbers. Either is
  acceptable; a quietly widened bound is not.
- **A split that changes behaviour.** Each split lands alone with the full suite
  green and no test changed.
- **Removing default constructors widens many test diffs.** Accepted: each test
  that constructed one now states its ledger, which is the point.
- **The harvest's bundles (H22–H24) are large.** Any one that cannot land in one
  reviewed commit is split at its first review round, as M10.28 was.

## Decisions

No ADR is needed for the re-plan: it moves milestones, not a decision recorded
in an ADR, and is recorded in the roadmap. ADR-0080 is unchanged; only its
implementation milestone moves. M12.2 and M12.1 remove API rather than change a
format or the store SPI.

## Tasks

In the re-plan's order: M11.23, the splits, H2–H4, then the rest. M11.25 may
land anywhere.

| ID | Task | Serves |
|---|---|---|
| M12.0 | This spec, the roadmap's M12 row, fast mode re-homed to M13 and ADR-0080's wire change to M13 or later | — (planning) |
| M11.23 | The drain overrun: rig recorded, measured, trigger `202` timed apart, RustFS memory across runs, bisected (carried) | NFR-7, FR-4 |
| M11.24a | Split `DefaultIngest` below 600 lines, no behaviour change | — (structure) |
| M11.24b | Split `Assembly` below 600 lines, no behaviour change | — (structure) |
| M11.24c | Split `BulkService` below 600 lines, no behaviour change | — (structure) |
| M11.24 | The gate keeping the three below 600 lines (carried) | — (structure) |
| M12.1 | H2: no ledger-less production constructor | FR-21 |
| M12.2 | H3: `Ingest`'s `buffered` overloads and `concreteIndex` abstract | FR-15, FR-19 |
| M12.3 | H4: the unflushed-bytes ceiling's reset race, and its guard pinned | NFR-6 |
| M12.4 | H5: quota buckets bounded | FR-15, FR-21 |
| M12.5 | H6: pod-level refusal counters; refused indices in the top-K line | FR-21 |
| M12.6 | H7: `/admin/cost` opt-in | FR-21 |
| M12.7 | H8: one `429` emitter, a gate predicate | FR-21 |
| M12.8 | H9: `/admin/cost` survives a malformed registration | FR-21 |
| M12.9 | H10: `cost.top-k-interval` bounded, the real window logged | FR-21 |
| M12.10 | H11: the explicit-partition wait bounded per index | FR-13, FR-15 |
| M12.11 | H12: the node failure hold jittered, `holdMillis` pinned at 55 and 65 | FR-6, NFR-4 |
| M12.12 | H13: live and catch-up retry state separated, or sharing justified by test | FR-6 |
| M12.13 | H14: quota overrides by alias | FR-15 |
| M12.14 | H15: `checkMilestoneVerified` fail-open and duplicate numbers | — (harness) |
| M12.15 | H18: `PushQueue` drain bound injectable, abandoned pushes counted | FR-5 |
| M12.16 | H19: `GovernorMetrics` bound per registry | FR-21 |
| M12.17 | H20: `expiredOr` typed exception; the offset restore under its lock | FR-10, FR-6 |
| M12.18 | H21: one assembled-pod read workload with all three GET kinds; `AdminCostAssemblyTest` with `top` and prices | FR-21 |
| M12.19 | H22: the test-pin bundle | — (quality) |
| M12.20 | H23: the doc and ADR text bundle | — (docs) |
| M12.21 | H24: the flake and suite-time bundle | — (quality) |
| M12.22 | H25: JVM gates — the ADR short form and io-seam exact-path anchoring | — (harness) |
| M11.25 | The `DefaultIngestTest` class timeout, with H17's thread dump (carried) | — (quality) |
| M12.23 | Close M12: VERIFIED.md, the roadmap row, the milestone gate's default moved to M12, the milestone review | — (evidence) |
