<!-- SPDX-License-Identifier: Apache-2.0 -->
# M8 — Resilience: the system assembled, and then broken

## Completion condition

From [roadmap.md](../../roadmap.md): *"Chaos matrix passes, including `SIGSTOP`
gray failure and AZ partition; RPO 0 demonstrated"*, plus the `EndpointSlice`
watch, the early-challenge path and therefore **NFR-9**, and the degraded
`ctl/inbox/` path — both reassigned to M8 and both previously owned by nothing.

⚠️ **AND THE THING BEING BROKEN MUST EXIST FIRST**
([ADR-0052](../../decisions/0052-m8-owns-assembly-and-the-first-real-backend-because-its-evidence-is-unbuyable-without-them.md)).
There is no production `main()` in the tree and no real object-store backend.
The mechanisms listed in § *The unwired set* below are constructed only by their own tests. A chaos matrix over a
system nothing assembles kills processes that run none of the mechanisms under
test, and `MemoryBinStore` is a `ConcurrentHashMap` in the killed process's own
heap, so its RPO is 0 for survivors and ∞ for the killed pod **independent of
any code this project wrote**. So M8 owns assembly and the first real backend,
as its first tasks, before any chaos task. The completion condition is unchanged.

## The unwired set

⚠️ **ONE LIST, DEFINED HERE, REFERRED TO EVERYWHERE ELSE IN THIS DOCUMENT.**
Earlier drafts gave it three different sizes in four places. It is **not**
counted, because every count this project has written for a set like this has
been short.

⚠️ **THE THIRD COLUMN IS `check-wired.sh`'s INPUT, NOT A DESCRIPTION OF IT.**
Its grammar is in `scripts/wired_scan.py`'s docstring (M8.25): each backticked
span is one predicate and every one must hold, so a cell carries no other
backticks. The fourth column names the open row that owns an entry still
unwired; a `done` owner, or none, fails the gate.
The script READS this table and turns each cell into its search; it does not
carry its own copy, because a copy in a file no spec reviewer opens is where
this set has already gone stale three times. If a cell cannot be mechanised as
written, the cell is wrong and is fixed HERE.

| Entry | What is unwired | What `check-wired.sh` accepts as wired | Else owned by |
|---|---|---|---|
| M5.6e | the production `SequencerTransport` — `InProcessTransport` in `testFixtures` is the only implementation | `new-impl SequencerTransport` (a construction of an implementation) | — |
| M5.91a | no production `main()` (closed by M8.4) | `main` | — |
| M5.91b | `SegmentPrefetcher`, `NodeSegmentSource`, `NodeSubscriptions` — built only by tests (closed by M8.31 for the last two, and by M8.56 for `SegmentPrefetcher`) | `new SegmentPrefetcher`; `new NodeSegmentSource`; `new NodeSubscriptions` | M8.56 |
| M5.91c | `FallbackLadder` — built only by tests, and never EXECUTED (closed by M8.28, criterion 20) | `new FallbackLadder`; `call tierFor` (constructed AND run) | M8.28 |
| M6.15 | `IndexRegistrar.onReconnect()`, called by nothing because no production `SubscriptionTransport` exists | `call onReconnect` | — |
| M6.19 | `RoutedIngest` — the routed path, in no deployable server | `new RoutedIngest` | — |
| M7.17 | `ProgressReporter.Positions` — no production source (M8.43, split out of M8.6) | `implements Positions` | — |
| M7.18 | the retained-floor frame from GC to the consumer (closed by M8.6, ADR-0056) | `call retainedFrom` | — |
| M7.21n | `RetentionPass`, `LeasedGc`, `RetentionObservable` — constructed only by their own tests (closed by M8.5) | `new RetentionPass`; `new LeasedGc`; `new RetentionObservable` | — |
| M7.24 | `ChainGc`, constructed only by its own test (M8.39; split out of M8.5 because every checkpoint source is a read per pass) | `new ChainGc` | — |
| M7.25 | the commit chain — **no `src/main` method produces a `List<CommitDelta>`** | `new ChainMemory` (closed by M8.3: the chain is a Snapshot record component, which a method-return predicate could not see) | — |
| M7.26 | the delete batch size — no named default in the tree | `constant DEFAULT_DELETE_BATCH in server` (named, and read by the root) | — |

⚠️ **EACH ENTRY CARRIES ITS OWN PREDICATE, BECAUSE A SINGLE ONE DOES NOT FIT.**
Round 2 tightened the predicate from "referenced" to "constructed" — correctly,
since "referenced" was MEASURED green on today's tree for five mechanisms, every
hit a javadoc sentence saying the mechanism is NOT wired — and round 3 found
that "constructed" is unsatisfiable for the four entries that name no type: a
`main()`, a chain, a constant. So the predicate is per entry, written above, and
`check-wired.sh` reads this table rather than embedding a copy of it.

⚠️ **AND TWO IDs HERE ARE NOT THE ONES EARLIER DRAFTS USED.** M5.91 is split
into its two halves because they have different predicates. **M7.19 is NOT in
this list**: that row is a flaky `RoutedIngestTest` deadline and names no
mechanism — an earlier draft of this table called it "the GC lease's production
role registration", which it never was. The three mechanisms that entry was
reaching for are `RetentionPass`, `LeasedGc` and `RetentionObservable`, which
ADR-0052 names and which no backlog row owned; they are **M7.21n** here and get
a backlog row of their own.

Criterion 16 and `scripts/check-wired.sh` (M8.25) both take exactly this list.

## Requirements

| ID | What M8 owes it |
|---|---|
| **NFR-8** | RPO 0 for acked writes at `ack_mode=durable`, demonstrated by killing a process mid-flush and reading every acked record back |
| **NFR-9** | RTO < 5 s for offset visibility after sequencer loss — the `EndpointSlice` watch and the early challenge, which M5 could not serve because static membership never removes a member |
| **NFR-11** | Offset stability across sequencer failover, absolute |
| **FR-11** | Sequencer leadership by CAS lease with epoch fencing, under partition and gray failure rather than under a simulation |
| **FR-12** | Commit forwarding to the leaseholder — ⚠️ **its FLEET half**, which M6's SPEC records as reachable only once a production transport exists |
| **FR-13, FR-16** | The routed path and the registration push, reached over a PRODUCTION transport rather than an in-process fake — M5.6e, M6.15 and M6.19, which have been owed to M8 since M5 and which nothing else can close |
| **FR-1** | The bulk write path, served by a real process against a real store rather than by a test harness |
| **FR-9, NFR-13** | Retention and the consumer watermark, RUNNING — M7 built them and wired none of them |
| **FR-10** | The consumer's fetch modes and its gap recovery — ⚠️ **the fallback ladder EXECUTING**, which `DeliveryGapException`'s javadoc assigns here and which nothing has ever run |
| **NFR-10** | The presign obligations that were waiting for a backend that can sign (M5.37, M5.42, ADR-0041) |
| **NFR-4** | The READ request rate scales with segments, AZs and nodes — never with shards, partitions or indices. ⚠️ **M8 IS WHERE IT BECOMES ASSERTABLE**, because the consumer-side fetch path is wired here (M8.31) and a per-subscription cache is one GET per shard per segment — the one defect this project exists to prevent, landing with every other gate green |
| **NFR-3** | Zero LIST on hot paths — re-asserted **in the assembled process**, which is where M7's GC budget is currently unproven (M7.25) |
| **NFR-2** | Zero idle requests — re-asserted in the assembled process for the same reason |
| **Measurement M1** | ⚠️ **The lease TTL and challenge policy under realistic GC pauses**, which `50-open-questions.md` § 3 assigns to "M8 chaos suite" and which `LeaseConfig`'s javadoc says in as many words that M4 must not hardcode because M8 will measure it |

⚠️ **NFR-14 (`ack_mode=wal` RPO) IS NOT OWED BY M8 AND AN EARLIER DRAFT OF THIS
TABLE CLAIMED IT.** Its mechanism is FR-17 fast mode, which the roadmap defers to
**M11** — "a second write path with its own failure modes should not be built
alongside the first" — and there is no WAL or quorum code in `src/main`. Citing
it here would have been M5.21's defect exactly: a requirement claimed by a
milestone with no criterion behind it.

⚠️ **AND MEASUREMENT M1 CHANGES THE SHAPE OF THIS MILESTONE'S TIME BOUNDS.**
The lease TTL is an INPUT to criteria 9 and 10 and an OUTPUT of criterion 19.
Earlier drafts wrote "~10 s" into both, which is a constant this milestone is
supposed to measure — and retuning a bound after measuring it is moving a
threshold to make a check pass (non-negotiable 2). So the bounds are stated
**relative to the configured TTL**, the measurement is taken first, and the
absolute numbers are REPORTED rather than asserted.

## Scope

1. **A composition root and a production `main()`.** A new `server` module that
   constructs writer, reader, sequencer, subscription hub, retention loop and GC
   lease from configuration and runs as a process.
2. **The production peer transport** — `SequencerTransport` and
   `SubscriptionTransport` (M5.6e), which have existed only as `testFixtures`
   fakes since M5. ⚠️ **WITHOUT IT THERE IS NO FLEET TO BREAK**: every append to
   a non-leaseholder pod fails, `new FleetSequencer(store, config, ???, election)`
   cannot be written by a service, and criterion 1's consumer has nothing to
   subscribe over. It also carries M6.15's reconnect signal and M6.19's routed
   path, both of which name it as their blocker.
3. **One real object-store backend** — S3-compatible, exercised against MinIO in
   the test path. Closes the roadmap's unassigned "first backend that can
   presign" row and ADR-0041's deferred presign obligations (M5.37, M5.42).
4. **The chain's in-memory source**, named before any GC loop is wired (M7.25).
5. **Graceful shutdown** in the order research 08 §7 gives, which that section
   says "gets more of the value than any failover code".
6. **The `EndpointSlice` watch and the early-challenge path** (NFR-9).
7. **The degraded `ctl/inbox/` commit-intent path** under AZ partition.
8. **The chaos harness and ALL NINE of research 08 §9's rows.** ⚠️ An earlier
   draft of this spec decomposed eight and silently dropped *"kill an OpenSearch
   node mid-backlog"* — the only row that exercises the CONSUMER resume path,
   and therefore the only one that could catch a catch-up burst starving the
   live tail.
9. **Measurement M1: the lease TTL and the challenge policy under realistic GC
   pauses.** `50-open-questions.md` § 3 assigns it here and `LeaseConfig`'s
   javadoc says M4 must not hardcode what M8 will measure. ⚠️ The chaos harness
   is the instrument: a `SIGSTOP` of a chosen length IS a GC pause, which is why
   this constant could not be measured before M8.8 existed.
10. **Executing the fallback ladder** — `FallbackLadder` is a policy nothing
   runs, `DeliveryGapException`'s javadoc says "M8 owns executing" it, and its
   `TENS_OF_GETS` cost estimate is MODELLED with M8's chaos matrix named as its
   falsifier. A consumer that meets a gap during a chaos row is where the ladder
   either works or is revealed as unexecuted.

### Not in scope

- **Multi-AZ or multi-node deployment reality.** The harness partitions
  processes on one machine. ⚠️ The `EndpointSlice` watch's behaviour under a real
  Kubernetes control plane stays unmeasured, and `VERIFIED.md` must say so
  rather than implying otherwise.
- **GCS and Azure backends** — the roadmap defers them behind the conformance
  suite, which exists.
- **The cost and latency curve** — M9, measured on the system M8 assembles.
- **The governor (NFR-16)** — M10.
- **Region loss** — a stated non-goal (research 08 F10).

## Design

**The composition root is a module, not a class in `http`.** `http` is the thin
adapter (architecture.md rule 4, ADR-0019) and the in-process API is the primary
one; a `main()` inside it would make the HTTP surface the assembly point and
every in-process embedder would inherit a web server. The new module depends on
everything and **nothing depends on it**, so the dependency surface
`check-module.sh` enforces gains a leaf rather than a cycle.

**Configuration is data, parsed once, at the root.** Every seam this project
spent eight milestones injecting is constructed here and nowhere else. The
`check-io-seam.sh` exemption list grows by exactly one module, and that is the
point of a composition root: the I/O and the clock are real in exactly one place.

**The chaos harness drives real processes.** A test that calls `close()` on an
object is not a kill; `SIGKILL`, `SIGSTOP` and a partition are properties of a
process and a socket. The harness starts the assembled jar, kills or stops or
isolates it, and asserts at the store and at a surviving consumer.

**Rejected: a chaos harness built on in-process fault injection.** The tree
already has `FaultInjectingBinStore` and it is the right tool for store errors —
but it cannot produce a gray failure, because a stopped process renews no lease
while an injected error path is still scheduling. Research 08 §9 says the
`SIGSTOP` and clock-skew rows "are the ones that find real bugs"; both are
untestable in-process.

**Rejected: deferring the real backend to M9 and running chaos on local-FS.**
ADR-0052 § Alternatives, with its number.

## Cost impact

| Rule | Effect |
|---|---|
| **R2 / NFR-3** (no LIST on the hot path) | ⚠️ **M8 is where M7's GC budget is finally provable.** `SegmentGc` asserts 0 LIST around itself; the assembled loop must obtain its chain without a LIST per pass, which is M8.3 |
| **R3 / NFR-2** (zero idle requests) | Re-asserted against the assembled process: an idle pod must issue **zero** store requests over ≥5 minutes of wall clock, not of an injected clock |
| **R6** (the commit/metadata write rate is independent of the INDEX COUNT — cost.md rule 6, which an earlier draft of this cell paraphrased as "one CAS per commit") | Unchanged by assembly. ⚠️ **THE INBOX PATH IS WHERE IT CAN BREAK**: research 03 §10's degraded route is an extra PUT per pod, per slot, per flush, so a partition that lasts converts a shared CAS into a per-pod write. Criterion 17 bounds it and asserts it returns to the normal rate when the partition heals |
| **R15** (LIST ceiling ~1/s) | The orphan sweep's period is now a real timer rather than a parameter; the assembled process must hold the ceiling with the sweep actually running |
| **New cost** | The first real backend introduces real request billing in the test path. MinIO is local and free; what it buys is that the counts are of real requests |

## Acceptance criteria

1. **The assembled process accepts a write and a consumer reads it back,
   against a real S3-compatible endpoint.** `main()` starts from a config file,
   `POST /{index}/_bulk` returns 202, and a consumer subscribing through the
   client reads every record — with the segment fetched from MinIO, not from a
   fixture. ⚠️ **Asserted at the STORE**: the object exists in the bucket and its
   key matches the grammar, so a process that acked from memory is red.
2. **Nothing constructs a store, a clock or a socket outside the composition
   root**, enforced at the MODULE boundary: **`check-module.sh` is EXTENDED
   (M8.29) to refuse a `src/main` dependency on `binstore-backends` from any
   module other than the root**, so a backend constructed inside `ingest` behind
   a factory method fails the gate. ⚠️ **THE GRAPH ASSERTION AN EARLIER DRAFT
   PROPOSED IS WEAKER THAN THE GREP IT REPLACED** — a backend built inside
   `ingest` is never in the root's graph, so walking the root's graph cannot see
   it. ⚠️ **ENFORCED SINCE M8.29, AND AT RUNG 3**: before it, `check-module.sh`
   guarded the rule with `[ "$m" = "plugin" ]`, so `ingest` was excluded — a
   second draft of this criterion asserted the rule in the present tense and
   called it "does not compile", which is rung 3 sold as rung 1. Evidence:
   `ModuleGateTest.onlyTheCompositionRootMayDependOnABackend`.
   `check-io-seam.sh` then passes with exactly one module exempt beyond
   `binstore-backends`.
3. **An idle assembled pod issues, over ≥5 minutes of WALL CLOCK, EXACTLY the
   requests its held roles owe and NOT ONE MORE.** On a pod **holding ≥1
   sequencer lease, serving ≥1,000 subscribers and holding a non-empty
   accumulator buffer**, and ⚠️ **NOT holding the GC lease** — the sweep's own
   LIST rate is criterion 17's and R15's, asserted where the sweep runs, and
   folding a running sweep into this fixture would force exactly the
   `requests <= N` bound this criterion refuses — counted at a real backend: the
   lease renewals owed
   (`elapsed / renewInterval`, ±1) plus the flushes owed by the buffer, and
   **zero** requests of any other kind. ⚠️ **"ZERO REQUESTS" IS THE WRONG BOUND
   FOR THIS FIXTURE AND AN EARLIER DRAFT WROTE IT**: a leaseholder MUST
   `putIfMatch` on `renewInterval` and a non-empty buffer MUST flush, so no
   correct implementation could pass and the only way to green it would be to
   relax the bound to `requests <= N` with N read off the implementation — which
   is a heartbeat regression's hiding place. ⚠️ **AND THE 1,000 SUBSCRIBERS ARE
   WHAT NFR-2 IS ACTUALLY ABOUT**: the owed counts are a function of the lease
   and the buffer and must not move when the subscriber count does, so the same
   fixture is run at 1 subscriber and at 1,000 and the two counts must be
   **equal**. That is the assertion a heartbeat, a metrics push or a per-
   subscriber poll cannot survive, and it is checkable where a bare zero is not. ⚠️ **Wall clock, not injected**: M5's and
   M7's assertions drive a `Clock` seam, and the thing this criterion adds is
   that no real timer in the assembled process fires a request.
4. **The GC loop runs in the assembled process and issues zero LIST per pass.**
   Over ≥10 passes with nothing expired: 0 LIST, 0 GET, 0 DELETE. ⚠️ **THIS IS
   THE CRITERION M7 COULD NOT WRITE** (M7.25) — the pass must obtain its commit
   chain from memory, and a wiring that re-reads it costs one GET per delta.
5. **Graceful shutdown happens in research 08 §7's order, observably.** On
   `SIGTERM`: readiness fails, subscribers are told to reconnect, in-flight
   requests finish, buffers flush and commit, leases are released, then exit —
   asserted as the **observed order of events**, not as a clean exit code. ⚠️ And
   **every record acked before the `SIGTERM` is readable after the process has
   exited** — ⚠️ **THE PROPERTY, WITHOUT WHICH THE ORDER IS DECORATION**: a
   drain that skips the flush satisfies both the order and the 30 s bound by
   being instant, and loses the buffer on every rolling deploy while criteria 5,
   6, 12 and 13 stay green. And the whole sequence completes **within 30 s**
   with a pod holding ≥1 lease,
   ≥1 subscriber and a non-empty buffer — research 08 §7 says to "budget ~30 s
   and measure it", so 30 s is the bound and **the measured number is reported**
   for an operator to set `terminationGracePeriodSeconds` from. ⚠️ **AND THE
   MEASUREMENT IS OF A REAL DRAIN, NOT OF A RECORDING SEAM** — a T0 order test
   cannot measure a flush, so the number comes from the assembled process.
6. **RPO 0, NFR-8: killing a pod mid-flush loses no acked record.** `SIGKILL`
   with ≥1,000 acked records outstanding; every acked record is readable
   afterwards. ⚠️ **THE KILL IS SYNCHRONISED TO THE ACK→PUT WINDOW, NOT TIMED**:
   an unsynchronised `SIGKILL` lands outside that window almost always, and the
   mutation — acking before the PUT completes — then survives every run. The
   harness kills on an observable signal (the ack observed at the producer, the
   PUT not yet observed at the store) and the test **ASSERTS that at least one run landed
   inside the window** and reports how many. ⚠️ **REPORTING IT IS NOT ENOUGH**:
   ack-before-PUT plus a probe that never fires yields "in-window runs: 0" and a
   green suite, so NFR-8 would be demonstrated by runs in which the kill never
   happened at the moment the property is about. ⚠️ **Duplicates are permitted and asserted as such** —
   at-least-once with `_id` dedup is the contract, and a criterion that forbids
   them is testing the wrong thing.
7. **Kill after PUT, before commit: the orphan is swept, the records are
   re-ingested, and no offset gap appears.** The segment PUT but never committed
   is deleted **by the assembled sweep**, the producer's retry lands the records
   in a new segment, and the consumer sees a contiguous offset sequence.
   ⚠️ **THE DELETION's TIME IS ASSERTED, NOT ONLY ITS FACT**: the orphan must
   still be present at a sweep run BEFORE `orphanGrace` elapses and absent
   after. A sweep that deletes on its first pass takes the retry's own
   uncommitted segment with it whenever the retry is slower than one period —
   and with the grace unasserted, all three of this criterion's other clauses
   still pass.
8. **Kill the sequencer mid-commit: I1–I5 hold, the seal succeeds, offsets are
   never reassigned.** ⚠️ **I1–I5, not I1–I4** — architecture.md defines five,
   and M4's VERIFIED.md records I5 as the one a plausible implementation breaks.
9. **`SIGSTOP` on a sequencer (gray failure): a fence or a challenge fires and
   the lag is bounded.** ⚠️ **THE ROW RESEARCH 08 §9 SAYS FINDS REAL BUGS.** A
   stopped process renews no lease and answers no commit, so the assertion is
   that visibility resumes **within `leaseTtl` + 5 s** — stated relative to the
   CONFIGURED TTL rather than to a constant, because the TTL is what
   criterion 19 measures and writing today's value in here would make retuning
   it look like moving a threshold to pass a check —
   and ⚠️ **deliberately NOT criterion 10's 5 s**, because a stopped process is
   the case the `EndpointSlice` watch cannot see: the pod's endpoint stays
   Ready, so this path is TTL-bound by construction and a 5 s bound here would
   be a target no mechanism serves — with the measured number reported — and
   that the resumed epoch is strictly greater — and that the stopped process, on `SIGCONT`, commits
   **nothing**, which is epoch fencing under the one failure that produces a
   live stale leader.
10. **NFR-9: offset visibility resumes in < 5 s after sequencer loss**, measured
    end to end from the kill to the first newly-visible offset, with the
    `EndpointSlice` watch driving the early challenge. ⚠️ **Asserted against the
    WATCH, not against the TTL**: with the watch disabled the same test must
    take **at least `leaseTtl`** — stated relative to the configured value for
    the reason criterion 9 gives, since an earlier draft wrote `~10 s` here
    while criterion 19 owes measuring it. ⚠️ **AND THE ELAPSED TIME IS NOT THE
    ONLY DISCRIMINATOR, BECAUSE IT STOPS DISCRIMINATING IF THE MEASURED TTL IS
    ITSELF ≤ 5 s**: the test also asserts that the **challenge was raised BY THE
    WATCH** — the endpoint-removal event observed, the challenge issued before
    the TTL could have expired — so "the watch is not wired" fails on the
    mechanism and not only on the clock. Without that half, a short TTL would
    let NFR-9 read as met by expiry, which is exactly what M5.21 moved this
    requirement to M8 to escape.
11. **An AZ partition engages the `ctl/inbox/` path, and the seal resolves dual
    leaders with no divergence.** Pods partitioned from their sequencer but not
    from the store fall back to the commit-intent inbox; when the partition
    heals, one epoch's writes are sealed and **no offset is assigned twice**.
    ⚠️ The degraded path had no owning milestone before this one.
12. **A pod partitioned from the store only fails readiness and stops
    accepting.** It must not buffer indefinitely and return 202: the assertion is
    that the acks STOP, counted, rather than that an exception is logged.
13. **A rolling restart of all pods leaves no visibility gap > 1 s and no
    thundering herd.** With **≥200 subscribers**, the reconnects are spread so
    that **no 100 ms window carries more than 20%** of them, asserted as the
    distribution rather than as "they reconnected". ⚠️ **A THRESHOLD IS WHAT
    MAKES THIS FALSIFIABLE**: if a pod simply closes, every subscriber waits out
    the same timeout and reconnects in one window (research 08 §7 step 2), and
    that is 100% in one bucket — a shape no assertion about connection counts
    can see.
14. **Clock skew of ±5 minutes on one pod leaves safety unaffected.** The epoch
    is a counter, so I1–I5 must hold with a skewed clock; only liveness degrades,
    and the test asserts which. ⚠️ **THE SKEW IS APPLIED TO THE PROCESS, NOT
    THROUGH THE `Clock` SEAM** — every pod in the tree takes a `Clock`, so
    injecting a skewed one tests the seam and leaves a lease comparison that
    reads the wall clock directly untouched, which is precisely the mutation
    this row exists to catch. The harness starts the JVM under a shifted
    system clock (`faketime` or an equivalent), so that `System.currentTimeMillis`
    and every injected `Clock` move together. ⚠️ **AN EARLIER DRAFT OFFERED A
    `-D` THE ROOT READS INTO THE INJECTED `Clock` AS AN ALTERNATIVE, WHICH IS
    THE MUTATION THIS CRITERION EXISTS TO CATCH** — under it a direct wall-clock
    read stays unskewed and the test passes with the defect present.
15. **The S3 backend passes store conformance, and the presign obligations that
    were waiting for it land.** `PresignConformance`'s capable branch RUNS
    (M5.37) and M5.42's case asserts a signing failure's message carries neither
    a signed URL nor a credential. ⚠️ **ASSERTED ON THE BACKEND's OWN
    EXCEPTION, NOT ON A WRAPPER's**: `GrantIssuer` chains the cause without
    quoting it, so asserting the wrapper's message reproduces exactly the
    weaker check M5.42's row records as measured-insufficient — the residual
    exposure is the backend's own message reaching a stack trace.
16. **Every mechanism recorded as unwired is wired, or is re-deferred with its
    reason in `VERIFIED.md`.** ⚠️ **THE LIST IS § *THE UNWIRED SET* ABOVE AND IS
    NOT RESTATED HERE** — an earlier draft restated it and the restatement went
    stale the moment the table was corrected, naming M7.19 (which names no
    mechanism), omitting M5.6e and M7.21n, and leaving M5.91 unsplit. One list,
    one place. ⚠️ **A
    milestone that assembles the system and quietly leaves one unwired is the
    failure ADR-0052 exists to end**, so this criterion is an enumeration, and
    the evidence document carries one line per entry.
    ⚠️ **AND THE "STILL NOT WIRED, HERE IS WHY" BRANCH IS CHECKED BY A SCRIPT,
    NOT BY A READER** (M7.31 measured what the reader costs): `scripts/check-wired.sh`
    READS that table — both its rows and its
    third column — and for each entry requires either that **the predicate in
    the table's own "what counts as wired" column** holds, or a line in
    `VERIFIED.md` naming an EXISTING backlog row that carries it forward.
    ⚠️ **THE PREDICATE IS PER ENTRY AND IS NOT RESTATED HERE EITHER**: a second
    draft of this paragraph stated a single `CONSTRUCTED` rule, which the table
    records as unsatisfiable for the four entries naming no type, so M8.25 had
    two contradictory specifications of one gate. ⚠️ **AND "REFERENCED" IS NOT
    "CONSTRUCTED", WHICH IS WHY THE COLUMN SAYS CONSTRUCTED WHERE IT CAN**: review MEASURED that predicate GREEN on
    today's tree for `SequencerTransport` (7 `src/main` files), `LeasedGc` (2),
    `RetentionPass`, `ChainGc` and `RoutedIngest` — every hit a javadoc sentence
    saying the mechanism is NOT wired. The null mutation passed the gate. Both halves are predicates over files, which is `gate-design`
    rung 3, and the failing mutation is nine evidence lines that each name a
    real test while three mechanisms are still constructed only by tests.

17. **The inbox path's write rate is bounded, and it returns to normal when the
    partition heals.** During the partition of criterion 11, the commit-intent
    writes are counted at the store and are **≤1 PUT per pod per SLOT per
    flush** — not per record and not per index — over a fixture of **≥3 indices,
    ≥2 slots and ≥2 pods**, and after the heal the rate returns to the
    unpartitioned one. ⚠️ **A DEGRADED PATH THAT COSTS PER INDEX IS HOW A
    PARTITION BECOMES A BILL**, and cost.md rule 6 — the commit rate is
    independent of the index count — is the rule it would break. ⚠️ **THE ONE-
    INDEX FIXTURE EVERY OTHER TEST USES CANNOT SEE IT**, which is why the counts
    are named here. ⚠️ **AND THE DRAIN IS THE OTHER HALF**: the leaseholder
    reading the inbox back must not LIST it per slot per interval — the drain's
    LIST rate is counted and held under R15's ~1/s, or the degraded path breaks
    the ceiling that the normal path was designed around.
18. **Killing an OpenSearch node mid-backlog resumes from `batch_start`, and the
    catch-up does not starve the live tail.** With a consumer behind by ≥1 hour
    of segments, a node is killed and its shards reallocate; the resumed shard
    reads from its committed pointer with no gap, and ⚠️ **a live record written
    AFTER the kill becomes visible within the same flush-window bound as it
    would with no backlog at all** — the starvation half, which is the only
    reason this row is in research 08 §9 and which an assertion about the
    backlog alone cannot see. ⚠️ **AND "AS IT WOULD WITH NO BACKLOG" IS A
    CONTROL RUN THAT IS MEASURED**, in the same test, with no backlog present;
    comparing against a remembered number from another suite is how a bound
    quietly becomes whatever the implementation does.

19. **Measurement M1: the lease TTL and challenge policy are MEASURED under
    realistic pauses, and the configured defaults are shown to survive them.**
    `SIGSTOP` a leaseholder for a series of durations spanning a realistic GC
    pause; report the longest pause the configured TTL tolerates without a
    spurious takeover, and the takeover latency once it does fire. ⚠️ **THIS
    CRITERION REPORTS NUMBERS AND ASSERTS ONLY A DIRECTION**: that a pause
    shorter than the TTL causes NO takeover and one longer causes exactly one.
    Asserting a specific pause tolerance would pin the JVM garbage collection behaviour, which is
    not ours to pin — the shape M7.14 used for measurement M5/M6. ⚠️ **AND IF
    THE MEASUREMENT SHOWS THE DEFAULT TTL IS WRONG, CHANGING IT IS THE POINT OF
    THE MEASUREMENT** and not a weakened threshold; the change lands with the
    number that caused it.
20. **The fallback ladder EXECUTES when a consumer meets a gap.** During a chaos
    row that produces a real `DeliveryGapException`, the ladder's tiers are
    observed to run in order and the consumer recovers — and the GETs each tier
    costs are COUNTED, so `FallbackLadder`'s modelled `TENS_OF_GETS` is either
    confirmed or corrected with the measured number. ⚠️ **A POLICY NOTHING
    EXECUTES IS A POLICY THAT HAS NEVER BEEN WRONG**, and M6's SPEC assigned
    executing it here.
21. **Configuration is parsed once, and a bad configuration fails the process at
    startup rather than at first use.** A missing endpoint, an unparseable
    duration and a negative size each refuse with a message naming the key —
    and the process EXITS NON-ZERO rather than starting degraded — ⚠️ **the
    exit code is asserted against a REAL PROCESS**, because a T0 test of the
    parser passes against a root that catches the refusal and starts anyway.
    Criterion 5's shape: a T0 test for the message and a T3 test for the code. ⚠️ **THE
    FAILURE PATH IS THE CRITERION**: a root that defaults a missing store
    endpoint to a local path starts happily and writes nowhere anyone expects.

22. **A consumer that meets a position GC has collected is refused, from a real
    GC pass rather than from a hand-fed floor.** M7 asserted the two halves
    separately because `ingest` does not depend on `client` — the boundary was
    REPORTED in one test and the refusal asserted against a floor set by hand in
    another — and M7.23 records that the joining test "belongs wherever the
    assembly lands". ⚠️ **THE ASSEMBLY IS THIS MILESTONE**, so the join is owed
    here: GC deletes, the retained floor travels to the consumer over the
    production transport, and the consumer's next read of a collected position
    raises `PositionCollectedException` carrying the lost range. This is also
    the T4 `RetentionRefusalIT` M7's test plan named and M7 did not write.

23. **The assembled consumer's fetch path costs GETs that scale with SEGMENTS,
    not with shards or subscriptions (NFR-4).** One node hosting **≥8 shards
    across ≥2 indices**, all subscribed, all reading the **same ≥4 segments**:
    the GETs counted at the store are a function of the segment count and **do
    not move when the shard or subscription count doubles**. ⚠️ **A PER-
    SUBSCRIPTION CACHE IS THE MUTATION**, and it is invisible to every other
    criterion here — criterion 3 is idle and fetches nothing, criterion 1 reads
    one record through one consumer, criterion 4 counts a GC pass, criterion 20
    counts the gap path. One GET per shard per segment is correct, fast, green
    on every read-back assertion, and exactly the request-rate shape
    non-negotiable 6 forbids. ⚠️ **AND M9 WOULD PUBLISH THE MUTANT's CURVE**,
    because the cost numbers M9 measures are of whatever M8 assembles.
24. **The routed path serves a write through the assembled server (M6.19,
    FR-13).** `POST /{index}/_bulk` with `os_routing` on a SPLIT index (a
    routing factor above 1) lands in the partition OpenSearch's own placement
    would choose, a write naming a partition outside the index's range is
    REFUSED with FR-13's status rather than placed, and a routed write to an
    index whose `routing_partition_size` > 1 is refused, because ADR-0006 puts
    that shard out of the ingester's reach (corrected by M8.32: an earlier
    draft asked for that mode to LAND). ⚠️ **`RoutedIngest`
    HOLDS ALL OF THAT AND ITS SIX CONSTRUCTION SITES ARE ALL TESTS** (M6.19), so
    without this criterion the spec would claim FR-13 with nothing behind it —
    the defect it refuses NFR-14 for. ⚠️ **AND CONSTRUCTING IT IS NOT CALLING
    IT**: criterion 16's predicate for M6.19 is a `src/main` construction, which
    `new RoutedIngest(...)` assigned and never called satisfies, so the routed
    WRITE is what this criterion asserts.
25. **Commit forwarding works across the fleet (FR-12).** A write arriving at a
    pod that does not hold the lease for its slot is forwarded to the pod that
    does, over the production transport, and is acked exactly once — with the
    forwarding counted, so a pod that silently took the commit itself is red.
    ⚠️ **M4 BUILT THIS AND ASSERTED IT OVER AN IN-PROCESS FAKE**; the fleet half
    has been owed since M5 and FR-12 sat in this table for six review rounds
    cited by no task and asserted by no criterion.

## Test plan

| Tier | What | Why not lower |
|---|---|---|
| **T0** | Config parsing, the shutdown ORDER against a recording seam, the composition graph | the order is a property of a sequence, not of a process |
| **T2** | The S3 backend against MinIO: conformance, presign, the signing-failure message | a real endpoint is the point |
| **T3** | The assembled process end to end: write, read, idle cost, GC cost | needs a process and a store |
| **T4** | The chaos matrix: kill, `SIGSTOP`, partition, skew, rolling restart | ⚠️ **the one place T4 is not avoidable** — `SIGSTOP` and a partition are properties of a process and a socket, and research 08 §9 names them as the rows that find real bugs |

**The test that must fail first, per criterion**, and the mutation each is meant
to catch:

| Criterion | Test | The mutation it must red |
|---|---|---|
| 1 | `AssembledWriteReadIT` | acking from the accumulator without a PUT — green on the 202 and on the read-back if the read is served from the same process's cache |
| 2 | ⚠️ **NO UNIT TEST — the EXTENDED `check-module.sh` (M8.29)**, whose own red is observed by adding a `binstore-backends` dependency to `ingest` and watching the gate go from green (today) to refusing it | the gate as it stands: review MEASURED that adding that dependency leaves `check-module.sh` GREEN, because the rule is guarded by `[ "$m" = "plugin" ]`. A `tdd-red.sh` record is not available for a shell gate, so the red is recorded by running the script before and after, both outputs in the commit body |
| 3 | `IdleAssembledCostIT` | a heartbeat, a metrics push, a sweep on a real timer, or any per-subscriber request — red by the 1-versus-1,000 EQUALITY, which a bound of the form `requests <= N` cannot see |
| 4 | `AssembledGcCostIT` | obtaining the chain with `recover()` per pass — one GET per delta, which is M7.25's exact defect |
| 5 | `ShutdownOrderTest` (T0, the ORDER) **and `ShutdownDrainIT` (T3, the 30 s NUMBER and the READ-BACK)** | releasing the lease BEFORE telling subscribers, which passes a "clean exit" assertion and produces the stall §7 describes — and, for the IT, a drain that skips the flush: it is FASTER than the 30 s bound, so only the read-back of a record acked before the signal can red it |
| 6 | `KillMidFlushIT` | acking before the PUT completes |
| 7 | `KillBeforeCommitIT` | a sweep that deletes the orphan before the grace period, taking the retry's records with it |
| 8 | `KillSequencerMidCommitIT` | a seal that reassigns an offset |
| 9 | `StoppedSequencerIT` | a resumed process that commits under its old epoch |
| 10 | `EarlyChallengeIT` | the watch not wired, so the number is the TTL |
| 11 | `AzPartitionIT` | an inbox entry applied twice, or an epoch's writes neither sealed nor applied |
| 12 | `StorePartitionIT` | buffering and continuing to ack |
| 13 | `RollingRestartIT` | unstaggered reconnects — red by the >20%-in-one-100 ms-window threshold, which a count-only assertion cannot see |
| 14 | `ClockSkewIT` | a lease comparison by wall clock rather than by epoch |
| 15 | `S3ConformanceIT`, `PresignFailureMessageTest` | a signing failure whose message includes the URL; and, for conformance, a conditional write that reports success on a PRECONDITION FAILURE — the `If-None-Match` shape ADR-0011's CAS turns on, which both backends in the tree today answer by never failing |
| 16 | `check-wired.sh` + `VERIFIED.md` enumeration | an evidence line for EVERY row of § *The unwired set*, each naming a real test, while some of those mechanisms are still constructed only by tests — the gate checks that a line EXISTS, never what it claims (M7.31, measured). ⚠️ **NO COUNT IS WRITTEN HERE**: an earlier draft said "nine lines, three mechanisms" against the table, which is the third inconsistent count of one set in this commit and the reason the set is now defined once |
| 17 | `InboxWriteRateIT` (≥3 indices, ≥2 slots, ≥2 pods) | a commit-intent write per INDEX rather than per pod per slot per flush — invisible at the one-index fixture every other test uses — and a drain that LISTs per slot per interval, which breaks R15 while every correctness assertion stays green |
| 18 | `OpenSearchNodeKillIT` | catch-up served ahead of the live tail: the backlog assertion passes and a record written after the kill waits for the whole backlog |
| 19 | `LeaseTtlMeasurementIT` | a takeover that fires on a pause SHORTER than the TTL (liveness lost for a healthy leader) or none on a pause longer than it (safety resting on nothing) |
| 20 | `FallbackLadderExecutionIT` | a ladder whose tiers are constructed and never invoked — today's state, and green on any assertion about recovery alone if the consumer also has a direct path |
| 21 | `ConfigRefusalTest` (T0, the MESSAGE) **and `ConfigExitCodeIT` (T3, the non-zero EXIT)** | a missing store endpoint defaulted to a local path: the process starts, writes nowhere anyone expects, and every in-process test passes |
| 22 | `RetentionRefusalTest` (T1: the join needs no container) | the floor reported by GC never reaching the consumer: both halves stay green in their own modules, which is exactly the state M7 shipped |
| 23 | `ProductionFetchPathTest` (≥8 shards, ≥2 indices, ≥4 shared segments, counts doubled; GETs counted at an HTTP server, since a presigned GET bypasses every `CountingBinStore`) | a per-subscription `NodeSegmentSource` cache: one GET per shard per segment — correct, fast, green on every read-back assertion in this plan, and the exact shape non-negotiable 6 forbids |
| 24 | `RoutedWriteTest` | the assembled server constructing plain `DefaultIngest`: every un-routed write passes and `os_routing` is silently ignored. ⚠️ Also reds `new RoutedIngest(...)` constructed and never called, which criterion 16's predicate alone accepts |
| 25 | `CommitForwardingIT` | a pod that takes the commit itself instead of forwarding — green on the ack, and it is two leaders writing one slot |
| — | `PeerTransportIT` (M8.20, M8.21) — its mutation: a transport that loops back in-process, which satisfies every "the transport works" assertion and no fleet one — red by criteria 11, 16, 17 and 25, which need two pods; ⚠️ **criteria 1 and 20 CANNOT red it** and an earlier draft listed them | ⚠️ **NOT A CRITERION OF ITS OWN AND THAT IS DELIBERATE**: the production transport is load-bearing for criteria 1, 11, 13, 16, 17 and 20, and a criterion asserting "the transport works" would be satisfied by a loopback. It is asserted by the fleet criteria that cannot pass without it |

⚠️ **`check-tdd` cannot see an IT annotated with a composed annotation** (M0.17)
and cannot record a red whose failure mode is a process death. Where that bites,
the red is observed by hand and the commit says so — the discipline M7.14 used.

**Suites this milestone extends**: store conformance (a third backend), the
commit-protocol simulation (I1–I5 under real kills), the cost assertions (idle
and GC in the assembled process).

⚠️ **AND THE SUITE-TIME BUDGET IS PART OF THE PLAN, NOT AN AFTERTHOUGHT**
(M6.20's precedent, and build.md's memory caps). Criterion 3 is ≥5 minutes of
WALL clock and criterion 18 needs an hour of segments — so both live in a
separate long suite, not in `./gradlew test`, the long suite declares its budget,
and the segments for criterion 18 are WRITTEN with old timestamps rather than
waited for. ⚠️ **A chaos suite that takes an hour is a chaos suite nobody runs**,
and the honest failure here is not flakiness but abandonment.

## Risks

| Risk | What would reveal it | Mitigation |
|---|---|---|
| **Assembly discovers that a seam does not compose** — the mechanisms enumerated in ADR-0052 have never been in one object graph | The composition root does not type-check, or needs a mechanism to change | Land the root EARLY and incomplete (M8.1), then wire mechanisms one commit each. A root that arrives last discovers everything at once |
| **The chaos harness is flaky and its flakiness is read as a finding** | A row that passes on re-run | Each chaos row asserts an INVARIANT, never a timing, except the criteria that are explicitly about time (5, 9, 10, 13, 18, 19) — and those state a bound with headroom and report the measured number |
| **`SIGSTOP` on the wrong process proves nothing** | The stopped process was not the leaseholder | The test asserts leaseholder identity BEFORE stopping, and the assertion after `SIGCONT` is about that same identity |
| **MinIO's S3 behaviour differs from S3's** where the protocol is ambiguous (conditional writes, listing order) | Conformance passes on MinIO and the code is wrong for S3 | ⚠️ Stated rather than solved: this is a real limit of the evidence, and `VERIFIED.md` says which criteria rest on MinIO alone. ADR-0011's CAS decision already turns on `If-None-Match`, which MinIO and S3 both support but with different error shapes — so the conformance suite asserts the SHAPE we depend on |
| **The milestone is too large to land as one** | Review rounds on a task that spans assembly and chaos | The decomposition below is strictly ordered: root, backend, chain source, wiring, shutdown, then one chaos row per commit |
| **RPO 0 is demonstrated against a store that acks before durability** | A kill loses an acked record and the store is blamed | MinIO is durable on `PUT` return; the criterion asserts read-back after a process death, which is the property, and the store's own durability is assumed and stated |

## Tasks

⚠️ **Strictly ordered, and the order is a dependency order**: root, transport,
backend, chain source, wiring, shutdown, harness, then one chaos row per commit.
Every row cites the requirement it serves — a task serving no FR/NFR is
unjustified work (sdd.md rule 2), and an earlier draft of this table left nine
rows uncited.

| ID | Task | Serves |
|---|---|---|
| M8.0 | This spec, ADR-0052 and the decomposition | — (planning) |
| M8.26 | Config parsing and its refusals: a bad key fails the process at startup, non-zero | FR-1 |
| M8.1 | The `server` module and the composition root: config in, object graph out, nothing wired to a socket yet | NFR-3, NFR-2 (the seam the budgets are asserted at) |
| M8.33 | `CommitRequestFrame` in `format`, ADR-0053 and its golden files — a forwarded commit is a format type, not a JSON body | FR-11, FR-12 |
| M8.34 | `check-wire-parity`: `CommitRequest` and `CommitRequestFrame` hold the same fields and nothing keeps them in step | FR-12 |
| M8.20 | **The production `SequencerTransport`** (M5.6e): the pod-to-pod forwarding hop outside a test, and commit forwarding asserted across the fleet | FR-11, FR-12 |
| M8.21 | **The production `SubscriptionTransport`**, and `IndexRegistrar.onReconnect()` wired to its reconnect (M6.15) | FR-16, FR-13 |
| M8.2 | The S3-compatible backend against MinIO: PUT, GET, conditional write, LIST, DELETE | NFR-8 |
| M8.22 | That backend through the store conformance suite, including the branches only a real one reaches | NFR-8 |
| M8.3 | The commit chain's in-memory source, so a GC pass costs no GET (M7.25) | NFR-3 |
| M8.4 | `main()`: the process starts, serves `_bulk`, and a consumer reads back from the real store | FR-1, FR-13 |
| M8.5 | The retention loop, the GC lease, the orphan sweep and the alarms wired (M7.21n, M7.26), with their cost budgets asserted in-process. ⚠️ M7.24 (`ChainGc`) split to M8.39 and criterion 3 to M8.40 | NFR-3, NFR-2, FR-9 |
| M8.6 | `ProgressReporter`'s production position source (M7.17) and the retained-floor frame (M7.18) — ⚠️ **M7.18 IS THIS ROW's, NOT M8.5's**, which an earlier draft had claiming it too | FR-9, NFR-13 |
| M8.7 | Graceful shutdown in §7's order, with the grace budget measured | NFR-9 |
| M8.8 | The chaos harness: start a real process, kill it, stop it, assert at the store | NFR-8 |
| M8.23 | The harness's network half: partition a process from a peer and from the store, and skew its clock | FR-11 |
| M8.9 | Kill mid-flush: RPO 0 | NFR-8 |
| M8.10 | Kill after PUT before commit: orphan swept on time, records re-ingested, no gap | NFR-8, FR-9 |
| M8.11 | Kill the sequencer mid-commit: I1–I5, seal, no reassignment | FR-11, NFR-11 |
| M8.12 | `SIGSTOP` a sequencer: the gray-failure row | FR-11, NFR-11 |
| M8.13 | The `EndpointSlice` watch and the early challenge: NFR-9 under 5 s | NFR-9 |
| M8.14 | The degraded `ctl/inbox/` path under AZ partition, and its write rate bounded | FR-11, NFR-3 |
| M8.15 | Partitioned from the store: readiness fails, acks stop | NFR-8 |
| M8.16 | Rolling restart: no gap > 1 s, staggered reconnects | NFR-9, FR-16 |
| M8.17 | Clock skew ±5 min applied to the PROCESS: safety by epoch, not by clock | FR-11 |
| M8.24 | Kill an OpenSearch node mid-backlog: resume from `batch_start`, and the catch-up does not starve the live tail | FR-9, NFR-13 |
| M8.18 | The presign obligations that were waiting for a real backend (M5.37, M5.42) | NFR-10 |
| M8.27 | Measurement M1: the lease TTL and challenge policy under `SIGSTOP` pauses, reported as numbers | FR-11 |
| M8.28 | The fallback ladder EXECUTES on a real gap, and its GET cost is counted rather than modelled | FR-10 |
| M8.32 | The routed path through the assembled server (M6.19): `os_routing` placement and FR-13's refusal | FR-13 |
| M8.31 | Wire the consumer-side fetch path: `NodeSegmentSource`, `NodeSubscriptions` and the first production `SegmentSource` (M5.91b) | FR-10, NFR-4 |
| M8.56 | Wire `SegmentPrefetcher` into the assembled ingester (split from M8.31) | FR-10, NFR-4 |
| M8.30 | Join GC's deletions to the consumer's refusal over the production transport (M7.23) | FR-9, FR-10 |
| M8.29 | Extend `check-module.sh`: only the root may depend on `binstore-backends` in `src/main` | — (gate) |
| M8.25 | `scripts/check-wired.sh`: the fixed list of unwired mechanisms, each either constructed in `src/main` or naming an existing backlog row | — (gate) |
| M8.19 | M8's `VERIFIED.md`, the unwired-mechanism enumeration, and the milestone review | — (evidence) |

⚠️ **THE IDs ARE NOT IN NUMERIC ORDER AND THAT IS CORRECT**: M8.20–M8.25 were
added by this spec's own review, which found the production transport owed to M8
since M5 and absent from the decomposition, one of research 08 §9's nine chaos
rows silently dropped, and criterion 16's re-deferral branch checkable by nobody.
IDs are stable identifiers, never an ordering mandate (git.md rule 1).
M8.26–M8.28 were added by round 2 of the same review, which found measurement M1
(assigned to "M8 chaos suite" by `50-open-questions.md` § 3, and named in
`LeaseConfig`'s javadoc as the thing M4 must not hardcode) owed to M8 and closed
by nothing, the fallback ladder's execution owed by `DeliveryGapException`'s
javadoc and absent, and configuration parsing with no criterion, test or failure
path at all.
