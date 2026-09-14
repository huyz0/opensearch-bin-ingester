// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M4's completion condition: I1-I5 hold across 1,000 deterministic seeds.
 *
 * <p>T1 -- {@code MemoryBinStore}, an injectable clock, a fault-injecting store
 * -- so it runs on every commit. ⚠️ ON `MemoryBinStore`, NOT MinIO, which
 * ADR-0008's addendum and testing.md both require: MinIO's conditional writes
 * are not stable enough to test the commit protocol against.
 *
 * <p>⚠️ WHAT IT ACTUALLY ASSERTS, because "I1-I5" is a claim worth taking apart:
 * <ul>
 *   <li><b>I1, I2, I5's structural clause</b> plus {@code chain}, {@code gap},
 *       {@code link} and {@code order} -- {@code Invariants.checkChain}, once
 *       per epoch.</li>
 *   <li><b>I3 and I4's DROP clause</b> -- {@code ReaderInvariants.checkReader},
 *       once per READER at its own epoch, against production's own reader.</li>
 *   <li><b>I5's ACK-ORDERING clause</b> -- {@code AckOrderInvariants}, over a
 *       trace the driver emits, because that order leaves no trace in the
 *       bytes.</li>
 * </ul>
 *
 * <p>⚠️ {@code checkAckOrder} CANNOT FAIL, AND NOT BECAUSE {@code CommitLog} IS
 * SERIAL -- that is what an earlier draft of this javadoc said and review
 * refuted it. The DRIVER synthesises both events from one {@code commit()}
 * return, adjacently and single-threaded, so the trace is ordered by the
 * driver's construction rather than the writer's. A {@code BatchingSequencer}
 * acking window N+1 before window N confirms would leave this GREEN. Making it
 * a real tripwire needs the CONFIRMED event to come from the store, which
 * already sees every PUT, or from a production callback: **M4.50**.
 *
 * <p>⚠️ AND THE TRACE CARRIES ONLY THE LEADER'S ACKNOWLEDGEMENTS. A fenced
 * writer's are absent, which matters because I5 is about what a FENCED writer
 * manages to acknowledge -- the population the simulation's own javadoc calls
 * "the ONLY thing that makes I5 reachable". Zombie commits now emit events too,
 * so the trace covers them -- though MEASURED at 0 zombie writes over 200 ROUGH
 * seeds, because M4.47's seal-the-run fences a zombie before it commits, so that
 * arm is a guard rather than exercised coverage. What the trace still cannot
 * cover is a write the zombie never learned had landed, which is M4.50's other
 * half.
 *
 * <p>⚠️ {@code checkReader} IS LIVE, which is a correction in the other
 * direction: review measured it catching a {@code ChainReplay} mutation no other
 * test caught -- "reader applied stream ... up to 0, dropping records the chain
 * committed up to 37". It is not decoration.
 *
 * <p>⚠️ AND I4's REORDER CLAUSE IS NOT ASSERTED AT ALL -- M4.13f settled that it
 * has no arm of its own below the granularity the chain carries, and the residue
 * is segment bytes no checker here reads. A sweep is only as strong as its
 * checkers, so the list above is the honest scope of "I1-I5" today.
 *
 * <p>⚠️ THE FAULT CLASSES ARE NOT ALL PRESENT EITHER. M4.13b (delayed writes,
 * reordered completions), M4.13d (per-class outcome evidence) and M4.13e
 * (partitioned leaders) are open, and acceptance criterion 1 names them. This
 * row owns the SWEEP and its budget; those own what it injects.
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitProtocolSweepTest {

    /**
     * The profile every seed runs under, all six classes live.
     *
     * <p>⚠️ EVERY NUMBER HERE WAS RE-MEASURED ON THIS TREE, and the previous
     * version of this comment is why that matters: it justified its rate by
     * citing "M4.13c measured that at 0.05 the seeds committing nothing reaches
     * 3". Review re-ran it: the true figure for {@code (0.05, 0.05, 0.1, 0.05,
     * 0.05, 0.05)} is <b>137 of 1,000</b>, 45x the quoted bound. A seed that
     * commits nothing holds I1-I5 VACUOUSLY, so 13.7% of the seeds M4's
     * completion condition is claimed over were proving nothing.
     *
     * <p>⚠️ AND THE OLD PROFILE WAS NOT INNOCENT EITHER: {@code (0.05, 0.05,
     * 0.1, 0)} produced <b>47</b> zero-commit seeds, and three of its six
     * classes were silently 0 because it used the four-argument convenience
     * constructor. This profile is strictly better than that one on every axis
     * measured over 1,000 seeds at 120 rounds and 3 pods:
     *
     * <pre>
     *   profile                              zero-commit  commits  readers  takeovers
     *   (0.05,0.05,0.1,0) -- three off               47     7.33    74.17     2.87
     *   (0.05,0.05,0.1,0.05,0.05,0.05)              137     4.22    47.34     2.61
     *   (0.03,0.03,0.1,0.02,0.02,0.02) -- this       12    11.11    85.46     4.81
     * </pre>
     *
     * <p>⚠️ THE BOUND IS ASSERTED, NOT TRUSTED. `zeroCommitSeeds` below fails
     * the sweep if vacuous seeds rise, because a rate is easy to raise and its
     * cost is invisible in a green run.
     */
    static final FaultInjectingStore.Faults SWEEP_FAULTS =
            new FaultInjectingStore.Faults(0.03, 0.03, 0.1, 0.02, 0.02, 0.02);

    /**
     * The number M4's completion condition names. ⚠️ THIS IS ONLY THE FALLBACK,
     * so asserting it holds nothing: {@code SEEDS} is what the loop runs and
     * what every floor is scaled by. Review MEASURED a one-token rewrite of the
     * line below -- {@code Integer.getInteger("sweep.seeds", 10)} -- running the
     * whole sweep at 10 seeds with an assertion on this constant still green.
     */
    private static final int DEFAULT_SEEDS = 1000;

    /**
     * ⚠️ CONFIGURABLE, defaulting to the completion condition's own number. A
     * lower count is for bisecting a failure, never for making a red run green:
     * the seeds are deterministic, so a seed that fails at 1,000 fails at 1,000
     * whatever this is set to. ⚠️ SETTING THE PROPERTY SKIPS THE WORKLOAD
     * ASSERTION BELOW, so a run that sets it is not a run of M4's completion
     * condition and must not be reported as one. ⚠️ AND IT NEED NOT BE SET AT
     * INVOCATION: review MEASURED one line of {@code systemProp.sweep.seeds=3}
     * in the TRACKED {@code gradle.properties} shrinking the sweep silently.
     * That half is a grep over tracked files and is <b>M0.89</b>'s, because no
     * assertion inside this file can see it.
     */
    private static final int SEEDS = Integer.getInteger("sweep.seeds", DEFAULT_SEEDS);

    /**
     * The stated budget. ⚠️ THIS JAVADOC IS THE ONE PLACE THE RUN TIME LIVES;
     * everything else points here rather than restating it. Five copies in
     * four files had already drifted to four different values -- 14.6s, 15.2s,
     * 16.4s, 17.1s -- for one number.
     *
     * <p>MEASURED for 1,000 seeds, each figure from a named run: 14.6s on this
     * harness; 15.1s, 15.2s (20 cores), 15.4s and 15.7s in review trees on the
     * same machine. ⚠️ THAT SPREAD IS THE ARGUMENT for reporting rather
     * than asserting the time, and the run prints the figure so a regression is
     * read off that line and not off this comment.
     */
    private static final long BUDGET_MILLIS = 60_000;

    private static final int ROUNDS = 120;
    private static final int PODS = 3;

    /**
     * ⚠️ THE SWEEP'S OWN PLUMBING, pinned THROUGH the loop rather than beside
     * it. An earlier version of this test built its own store and called
     * {@code checkReader} directly; review measured that it shared no line with
     * the checking loop, so dropping the loop's {@code addAll} -- computing the
     * verdict and discarding it -- left the whole suite green, and five existing
     * tests already covered everything it asserted.
     *
     * <p>⚠️ WHAT IS AT RISK IS HALF THE COMPLETION CONDITION. {@code checkReader}
     * carries I3 and I4's drop clause, and it is the O(E-squared) loop anyone
     * optimising against the 60s budget reaches for first. This drives the real
     * simulation with a deliberately broken view, so only the {@code addAll}
     * can make the assertion pass.
     */
    @Test
    void aBrokenReaderViewREACHESTheSweepsVerdict() throws Exception {
        var run = CommitProtocolSimulation.run(0, 40, 3,
                new FaultInjectingStore.Faults(0, 0, 0, 0),
                new binjava.binstore.backend.MemoryBinStore(),
                reader -> new ReaderInvariants.ReaderView(reader.epoch(), java.util.Map.of()));

        assertThat(run.commits()).as("the fixture must have committed something").isPositive();
        assertThat(run.violations())
                .as("a reader that dropped everything the chain committed is I4, and the "
                        + "loop's verdict must reach the Result -- not be computed and binned")
                .extracting(Invariants.Violation::invariant)
                .contains("I4");
    }

    @Test
    void theInvariantsHoldAcrossEverySeed() throws Exception {
        // ⚠️ THE WORKLOAD IS ASSERTED, NOT MERELY DECLARED, and it lives inside
        // this test rather than beside it for a reason worth recording: a test
        // asserting only its own file's constants can never be observed failing
        // without editing that file, so testing.md rule 2 cannot be satisfied
        // for it. Here it rides the sweep's own red.
        // ⚠️ SEVEN OF THE EIGHT FLOORS BELOW ARE `SEEDS * k`, so they measure
        // work PER SEED and are invariant under the seed count. ⚠️ THE EIGHTH
        // IS NOT, and it is called out here rather than only where it sits:
        // the refusals-seen floor is `SEEDS / 50`, which INTEGER-DIVIDES TO
        // ZERO below 50 seeds and is vacuous there. It is scaled that way
        // because its signal is ~0.04 per seed; see its own comment. Review MEASURED `SEEDS` 1000
        // to 10 together with `ROUNDS` 120 to 60 passing green at 1/200th of
        // the stated workload. Nothing else in the tree asserts the number M4's
        // completion condition names.
        // ⚠️ ON `SEEDS`, THE RESOLVED VALUE, AND AGAINST A LITERAL. Asserting
        // `DEFAULT_SEEDS` leaves the count the loop actually runs free -- review
        // MEASURED a one-token rewrite of line 85 to
        // `Integer.getInteger("sweep.seeds", 10)` running the whole sweep at 10
        // seeds with that assertion green -- and asserting `isEqualTo(
        // DEFAULT_SEEDS)` would be satisfied by lowering the constant itself.
        // The property-unset guard is what keeps `-Dsweep.seeds` usable for
        // bisecting; a run that sets it does not run the completion condition.
        if (System.getProperty("sweep.seeds") == null) {
            assertThat(SEEDS)
                    .as("M4's completion condition is \"I1-I5 hold across 1,000 deterministic "
                            + "simulation seeds\"")
                    .isEqualTo(1000);
        }
        assertThat(ROUNDS).as("act-or-fail steps per seed -- the depth a shallow run collapses "
                + "first, and the dimension the floors alone do not hold").isEqualTo(120);
        assertThat(PODS).as("logical pods contending for the lease").isEqualTo(3);

        FaultInjectingStore.Faults faults = SWEEP_FAULTS;

        List<String> failing = new ArrayList<>();
        long commits = 0;
        long takeovers = 0;
        long epochsBurned = 0;
        int faultsFired = 0;
        long readersChecked = 0;
        boolean ackFloorAlwaysZero = true;
        long ackEvents = 0;
        long ackKindEvents = 0;
        int zeroCommitSeeds = 0;
        // ⚠️ ONE ACCUMULATOR, NOT FOUR LONGS (M5.51). Four `+=` lines naming
        // four components are four transposable sites, and review measured that
        // closing the CONSTRUCTION site alone left them: `releasesDespiteOwn
        // Partition += run.releases().refusedForAnotherPod()` compiles and
        // CANNOT FAIL, because the sweep asserts both of those isZero and
        // swapping two honestly-zero expressions changes no assertion, no
        // floor, not even the printf. `plus` collapses them into one
        // constructor that `GracefulReleaseMeterTest` pins.
        GracefulReleaseMeter.Counts releases = GracefulReleaseMeter.Counts.none();
        long start = System.nanoTime();
        for (long seed = 0; seed < SEEDS; seed++) {
            var run = CommitProtocolSimulation.run(seed, ROUNDS, PODS, faults);
            if (run.commits() == 0) {
                zeroCommitSeeds++;
            }
            commits += run.commits();
            takeovers += run.takeovers();
            epochsBurned += run.highestEpoch();
            faultsFired += run.faults().size();
            readersChecked += run.readersChecked();
            releases = releases.plus(run.releases());
            ackEvents += run.acks().size();
            ackKindEvents += run.acks().stream()
                    .filter(AckOrderInvariants.AckEvent::isAck)
                    .count();
            for (long epoch = 1; epoch <= run.highestEpoch(); epoch++) {
                long floor = run.lowestAckedSequenceIn(epoch);
                if (floor > 0) {
                    ackFloorAlwaysZero = false;
                }
            }
            if (!run.violations().isEmpty()) {
                // ⚠️ ONE LINE PER SEED, NOT ONE PER VIOLATION, and the count of
                // the rest. `checkChain` walks once per epoch over `pods x
                // rounds`, so ONE chain-level defect is re-reported at every
                // epoch whose walk covers it: review MEASURED a single
                // production mutation producing 2,000 identical lines from 13
                // seeds and 353 KB of test XML at just 20 seeds -- ~17 MB at
                // 1,000, with the seed, which is the whole point, buried.
                var first = run.violations().get(0);
                failing.add("seed " + seed + ": " + first.invariant() + " -- " + first.detail()
                        + (run.violations().size() > 1
                                ? " (+" + (run.violations().size() - 1) + " more on this seed)"
                                : ""));
            }
        }
        System.out.printf("sweep: graceful releases %d, refused for another pod %d, refusals seen %d, "
                        + "despite own partition %d, zero-commit seeds %d%n",
                releases.released(), releases.refusedForAnotherPod(), releases.refusalsSeen(),
                releases.releasedDespiteOwnPartition(), zeroCommitSeeds);
        long elapsed = (System.nanoTime() - start) / 1_000_000;

        // ⚠️ ANTI-VACUITY FIRST, and it comes before the invariant assertion on
        // purpose: a sweep over a cluster that never committed, never failed
        // over and was never faulted reports CLEAN and proves nothing. This is
        // the same guard `CommitProtocolSimulationTest` puts first.
        // ⚠️ FLOORS PROPORTIONAL TO THE SEED COUNT, not `isPositive()`. Review
        // MEASURED that `isPositive()` does not hold the WORKLOAD: cutting
        // `ROUNDS` from 120 to 3 leaves a sweep at 1/40th the work green, with
        // all three floors satisfied. Per seed the means are 7.4 commits, 2.9
        // takeovers and 80.2 epochs at 120 rounds, against 0.9, 0.9 and 1.3 at
        // 3 -- so these separate the two by a wide margin in both directions.
        assertThat(commits)
                .as("mean commits per seed must stay near the re-measured 11.11, not collapse "
                        + "to the 0.9 a shallow run gives")
                .isGreaterThan(SEEDS * 3L);
        assertThat(takeovers)
                .as("and mean takeovers near the re-measured 4.81, against 0.97 shallow -- the "
                        + "flattest of these floors, so it is set at 1.5 per seed rather than "
                        + "1: review measured 2,872 against a floor of 1,000, only 3.2% of "
                        + "the separation, with ROUNDS=4 already clearing it")
                .isGreaterThanOrEqualTo(SEEDS * 3L / 2);
        assertThat(epochsBurned)
                .as("and the epoch depth near the re-measured 85.88 per seed, against 1.3 -- "
                        + "the dimension a shorter run collapses first")
                .isGreaterThan(SEEDS * 10L);
        // ⚠️ THE FAULT PROFILE IS A THRESHOLD TOO, and `isPositive()` did not
        // hold it: review MEASURED a profile weakened 494x -- injected faults
        // from 122,096 to 247 -- passing every floor here. Only `Faults.none()`
        // reddened. Measured means are 122 faults per seed at the real profile
        // against 0.25 at the weakened one, so this separates them by 5x in one
        // direction and 80x in the other.
        assertThat((long) faultsFired)
                .as("mean faults per seed must stay near the measured 122, not the 0.25 a "
                        + "weakened profile gives")
                .isGreaterThanOrEqualTo(SEEDS * 20L);

        // ⚠️ THE CHECKERS MUST BE SEEN TO RUN, because both were measured
        // unfalsifiable: deleting the `checkReader` loop left the whole suite
        // green, and so did neutralising every ack event. `checkReader` is the
        // LIVE one -- it carries I3 and I4's drop clause, half of what M4's
        // completion condition claims -- and it is the O(E-squared) loop anyone
        // optimising against the budget deletes first.
        // ⚠️ THE STATED FIGURE WAS STALE (M5.30), and a floor whose headroom
        // is wrong cannot be reasoned about the next time it needs tightening
        // -- which is the only reason these comments carry numbers at all. The
        // old "74.17 per seed" was the SUPERSEDED profile's row from the table
        // at the top of this file, not this profile's.
        // ⚠️ ITS UNITS WERE RIGHT, AND M5.30's FIRST DRAFT SAID OTHERWISE.
        // That draft called "7.3x" a mean quoted where the sentence speaks of
        // cumulative prefixes; review re-ran the superseded profile and
        // measured 74.172 readers per seed, a MEAN clearance of 7.417x and a
        // cumulative-prefix MINIMUM of 7.322x at N=94. 7.3x is the prefix
        // minimum. The old comment's sentence and its number agreed; only the
        // profile had moved.
        // ⚠️ RE-MEASURED on this tree over 1,000 seeds: 85,458 readers, 85.46
        // per seed, mean clearance 8.55x. The number that matters for a
        // SHORTENED run is the cumulative-prefix MINIMUM, which is 8.20x, and
        // it falls at N=1 -- so `-Dsweep.seeds=1` is the TIGHTEST run for this
        // floor rather than a vacuous one, and no interior prefix dips below
        // it.
        assertThat(readersChecked)
                .as("one reader judged per epoch that has a chain -- measured 85.46 per "
                        + "seed, and the TIGHTEST cumulative prefix clears the floor of 10 "
                        + "by 8.2x")
                .isGreaterThanOrEqualTo(SEEDS * 10L);
        // ⚠️ THE TRACE MUST EXIST BEFORE ITS FLOOR MEANS ANYTHING. The floor
        // assertion below is RELATIVE -- "no chain's floor is above 0" -- so an
        // EMPTY trace satisfies it, and review MEASURED exactly that: with every
        // `acks.add` neutralised the whole suite stayed green while this test
        // reported success. That is the slot-0 CONTINUE defect reintroduced one
        // level up, on the trace rather than on its floor.
        // ⚠️ RE-MEASURED (M5.30): 160,478 ack events over 1,000 seeds --
        // 160.48 per seed, not the 17.53 this comment used to claim, so the
        // floor of 5 clears by 32.10x on the mean and by 30.60x on the
        // TIGHTEST cumulative prefix, against a stated 3.09x. Same single
        // error as the readers floor above: a stale figure. 3.09x was already
        // a prefix minimum -- 17.53/5 is 3.506x, so it cannot have been a mean.
        // The prefix minimum is again at N=1.
        //
        // ⚠️ AND 30.60x IS HEADROOM OVER THE WRONG POPULATION, which M5.50
        // CLOSED by adding a second floor over the ACK half alone -- see the
        // block below, which carries the measured figures. ⚠️ THEY ARE NOT
        // RESTATED HERE ON PURPOSE: an earlier version of this comment repeated
        // all five of them twenty lines from the block that owns them, in the
        // file whose recorded failure mode is exactly a stale figure in a floor
        // comment (M5.30 was a whole task spent on that), and the next
        // re-measurement would have had two places to update and updated one.
        // ⚠️ AN EARLIER VERSION ALSO READ "which review MEASURED and M5.50
        // owns", presenting as open the row that closed it.
        assertThat(ackEvents)
                .as("the ack trace must be non-empty before its floor says anything -- "
                        + "`checkAckOrder` over an empty list reports nothing, forever")
                .isGreaterThanOrEqualTo(SEEDS * 5L);
        // ⚠️ A SECOND FLOOR, OVER THE ACK-KIND EVENTS ALONE, because the floor
        // above is over a 93/7 MIXTURE and cannot guard the minority half at
        // any seed-invariant value. Measured at 1,000 seeds: 160,478 events of
        // which only 11,110 are ACK -- 149.37 CONFIRMED per seed against 11.11
        // ACK. `checkAckOrder`'s only two `found.add` sites sit past
        // `if (!e.isAck()) { ...; continue; }`, so a trace with no ACK events
        // reports nothing FOREVER, which is verbatim the vacuity the floor
        // above says it refuses.
        //
        // ⚠️ THE GAP WAS DEMONSTRATED, NOT INFERRED: stripping the three
        // `acks.add(...acked(...))` calls in `CommitProtocolSimulation` takes
        // M4.50's own ack+1 defect from 11,110 violations to ZERO while the
        // mixture floor stays green at 29.83x.
        //
        // ⚠️ `SEEDS * 3`, AND THE ALTERNATIVES ARE MEASURED RATHER THAN
        // ARGUED. At the tightest cumulative prefix, N=8, the trace carries 47
        // ack-kind events: `SEEDS * 5` is 40, which clears by 1.175x -- seven
        // events of slack, one unlucky seed from a false red; `SEEDS * 3` is
        // 24 and clears by 1.958x; `SEEDS * 8` is 64 and FALSE-REDS at 0.734x.
        // ⚠️ AND `SEEDS * 1` FAILS THE WORKLOAD PROPERTY OUTRIGHT, which is
        // sharper than "holds almost nothing": at ROUNDS=3 the ack half totals
        // 1,017, so a floor of 1,000 stays GREEN on the shallow run by seventeen
        // events. `SEEDS * 3` reds that run by 2.95x and clears the real one by
        // 1.96x at its tightest prefix -- the best-separated of the four.
        //
        // ⚠️ AND "TIGHTEST" IS EXHAUSTIVE, NOT SAMPLED: review instrumented
        // every prefix N in 1..1000 and the minimum ratio is 1.9583, at N=8,
        // with no prefix below 3N -- or below 5N either. ⚠️ NOTE N=1 IS 2.667x,
        // so unlike the readers and mixture floors in this method the tightest
        // prefix here is NOT N=1, which is why two sampled points would have
        // been the wrong evidence even though they agreed.
        //
        // ⚠️ AND NOT `isPositive()`, for the reason this file's own workload
        // note gives rather than the one an earlier draft of M5.50 gave: a
        // positive-only floor does not hold the WORKLOAD and stays green at
        // ROUNDS 120 -> 3, one fortieth of the work. ⚠️ THE WORKLOAD PROPERTY
        // IS NOT WHAT DISTINGUISHES THIS FLOOR FROM THE ONE ABOVE -- at
        // ROUNDS=3 the mixture floor reds too, 3,553 against 5,000. What this
        // floor adds is the POPULATION: the ack half alone. (The draft cited the 12
        // zero-ack seeds; that does not transfer, because the earliest is seed
        // 197, seed 0 emits 8, and `-Dsweep.seeds=N` only ever takes the
        // PREFIX from seed 0 -- no prefix fails a cumulative positive floor.)
        assertThat(ackKindEvents)
                .as("the ACK half of the trace must be non-empty and must hold the workload -- "
                        + "`checkAckOrder` skips every non-ack event, so a trace of pure "
                        + "CONFIRMED events reports nothing, forever")
                .isGreaterThanOrEqualTo(SEEDS * 3L);
        assertThat(ackFloorAlwaysZero)
                .as("every chain's ack trace is based at 0, which is what the slot-0 CONTINUE "
                        + "event exists to guarantee -- it was documented in three places and "
                        + "emitted nowhere until review measured the floor sitting at 1")
                .isTrue();

        // ⚠️ A GRACEFUL RELEASE IS JUDGED AGAINST THE LEADER (M5.26). The
        // simulation's `leader.close()` once named no acting pod, so
        // `FaultInjectingStore` judged the release against WHOEVER ACTED LAST
        // -- refusing it for a partition injected at a different pod and
        // under-exercising the polite-failover path by that much.
        // ⚠️ THE FLOOR COMES FIRST, and it is not decoration: the refusal
        // count below is trivially 0 on a sweep where no leader ever closed
        // politely, which is the vacuity every other floor in this method
        // exists to refuse. ⚠️ AND IT COUNTS RELEASES THAT RETURNED, not
        // branch entries: review MEASURED a first version at `SEEDS / 20` over
        // attempts staying green at 1,713 with `leader.close()` DELETED
        // outright, and green again with the polite path made 26x rarer.
        // MEASURED 1,608 completed releases over 1,000 seeds -- 1.6 per seed
        // -- so a floor of one per seed clears it by 1.6x, and the count is
        // deterministic per seed rather than sampled. ⚠️ THE CLEARANCE IS A
        // 1,000-SEED FIGURE: under `-Dsweep.seeds=1` the margin is exactly
        // 1.0x, and the refusal signal below is 5 seeds in 1,000, so a
        // shortened bisect run meets this floor while seeing nothing.
        // ⚠️ AND IT IS A RELEASE, NOT MERELY STORE WORK (M5.29). Until the
        // meter could name the verb, this floor rested on "the window reached
        // the store": review MEASURED `leases.release()` in
        // `LocalSequencer.close` swapped for another verb leaving it GREEN at
        // 1,626, and only DELETING the store call redding it. The meter now
        // credits a `putIfMatch` -- what `LeaseManager.releaseLocked` actually
        // does -- and the same swap gives 0 and reds this line. MEASURED, on
        // this tree, at 1,000 seeds: 1,608 unmutated, 0 with `store.stat` in
        // its place.
        assertThat(releases.released())
                .as("the polite-failover path must COMPLETE before a claim about how it is "
                        + "judged says anything -- a floor over attempts survives deleting "
                        + "the release entirely, and a floor over store CALLS survives "
                        + "replacing it with another verb")
                .isGreaterThanOrEqualTo(SEEDS);
        assertThat(releases.refusedForAnotherPod())
                .as("a leader releasing its lease politely is judged against ITSELF, so no "
                        + "release is refused blaming another pod -- MEASURED at 5 with the "
                        + "defect reinstated at the store call, 0 without")
                .isZero();
        // ⚠️ AND THE OTHER DIRECTION (M5.31). The assertion above catches a
        // release wrongly REFUSED because the store judged it against a
        // partitioned stranger. The same stale actor wrongly ALLOWS one when
        // the stranger is NOT partitioned and the leader is -- and until this
        // line nothing looked for it. Review MEASURED the two sets of seeds
        // being different: the 5 carrying a wrong refusal are 372, 495, 662,
        // 887, 922, while the 4 whose outcomes MOVE are 323, 372, 495, 662 --
        // seed 323 runs 3 releases and 9 commits on a CORRECT tree and 4 and 13 UNDER THE DEFECT, with zero wrongly-blamed refusals either way -- the
        // unseen direction showing up in the totals and in no assertion.
        // ⚠️ STATED AS TWO STATES RATHER THAN "from X to Y", because the row
        // this task came from used that form and review read it backwards.
        // ⚠️ AND THE DEFECT ALONE REDS ONLY THE ASSERTION ABOVE, because
        // AssertJ stops at the first failure. The independent falsifying power
        // of THIS line was measured by blinding the old detector too --
        // `refusedForAnotherPod++` deleted AND `actingAs` deleted -- whereupon
        // the sweep reds solely here, 1 against 0.
        // ⚠️ IT IS THE MORE DAMAGING HALF: a wrongly refused release leaves the
        // lease to expire, which is the ungraceful path the cluster already
        // tolerates; a wrongly allowed one hands the lease back for a leader
        // that is cut off and cannot know, AND is credited by
        // `releases.released()`, so that floor is partly met by releases which
        // should never have completed.
        // ⚠️ THIS `isZero` IS NOT VACUOUS, and the reason is the floor below
        // rather than anything here: `releases.refusalsSeen()` proves
        // release windows DO meet partitions -- 38 over 1,000 seeds. Without
        // that floor, "no release completed despite its own partition" would
        // also be satisfied by a sweep in which no release ever met one.
        assertThat(releases.releasedDespiteOwnPartition())
                .as("a release that completed while the LEADER was cut off is one the "
                        + "leader's own partition should have refused -- and it was credited "
                        + "as graceful while doing so")
                .isZero();
        // ⚠️ AND THE DETECTOR MUST BE ABLE TO DETECT (M5.28). The assertion
        // above is `isZero`, so it can only fail by OVER-counting: review
        // MEASURED that misspelling `"partition"` in the meter's scan
        // predicate leaves the sweep green, and an emptied window or a deleted
        // increment are the same family -- 0 is the fixed tree's answer too.
        // A floor on the refusals the meter SEES kills TWO of the three --
        // MEASURED. Deleting `refusedForAnotherPod++` survives it, because on
        // a correct tree that counter is legitimately 0; `GracefulReleaseMeterTest`
        // is what catches that one, by building the situation.
        // ⚠️ NOT `isPositive()`, and that is the difference from the floors
        // around it: 38 refusals over 1,000 seeds is ~0.04 per seed, so a bare
        // positive floor REDS at `-Dsweep.seeds=1` with no defect present.
        // ⚠️ AND THE CLEARANCE IS STATED AT PREFIX SCOPE, because over the
        // 1,000-seed TOTAL it reads 1.9x and that is the wrong number to
        // reason with. MEASURED per cumulative prefix: 38 refusals fall on 37
        // seeds, `cum(N) >= N/50` holds for every N in 1..1000 so no
        // shortened run false-reds, but the slack is EXACTLY ZERO for N in
        // [50,79] -- only seed 1 carries a refusal before seed 79 -- and for
        // N < 50 the floor is 0 and this assertion is VACUOUS. A re-measured
        // `partitionRate`, or losing that one early window, reds a 50-seed
        // run with no defect present.
        assertThat(releases.refusalsSeen())
                .as("the meter must SEE refusals inside release windows, or asserting it "
                        + "counts none of the wrong kind constrains nothing")
                .isGreaterThanOrEqualTo(SEEDS / 50);
        // ⚠️ AND IT MUST BE THE REFUSAL COUNTER, not whichever `int`
        // sits beside it. M5.51 closed that at the `Result` CONSTRUCTION
        // -- it is now an `incompatible types` error -- but NOT here, at
        // the READ: `releases.refusalsSeen()` -> `releases.released()` on
        // this very line is still green, and unkillably so, because :438
        // already floors `released` at SEEDS while this floor is SEEDS/50.
        // M5.68 owns the read side. ⚠️ THE MARGIN IS MEASURED, NOT STRUCTURAL: 38
        // against 1,608, 42x. A refusal aborting its release proves
        // only that a refusing window is not CREDITED; it does not
        // bound the refusal COUNT below the credit count, and
        // `TWORefusalsInOneWindowAreCountedTWICE` is the standing
        // proof -- one window adds 2 here and 0 there. Review measured
        // zero windows doing both over 1,000 seeds, and this green at
        // every prefix from N=1.
        assertThat(releases.refusalsSeen())
                .as("and it must be the REFUSAL counter, not whichever counter sits "
                        + "beside it at this READ -- the `Result` construction no longer "
                        + "admits the swap, this line still does (M5.68)")
                .isLessThan(releases.released());

        // ⚠️ A SEED THAT COMMITS NOTHING HOLDS I1-I5 VACUOUSLY, and nothing was
        // counting them. Measured on this tree: 47 under the old profile, 137
        // under a first attempt at enabling every class, 12 under the profile
        // above. The completion condition says "across 1,000 seeds"; seeds that
        // never commit are not part of that thousand in any meaningful sense,
        // so their number is bounded here rather than discovered later.
        assertThat(zeroCommitSeeds)
                .as("seeds that commit NOTHING prove nothing -- they hold every invariant "
                        + "vacuously. Measured at 12 for this profile; the bound is where a "
                        + "raised fault rate starts hollowing out the sweep")
                .isLessThanOrEqualTo(SEEDS / 40);

        assertThat(failing)
                .as("every FAILING SEED is named -- one line each, first violation plus a "
                        + "count of the rest -- so a systemic break stays readable and each "
                        + "seed is pinnable as a named regression. The rest of a seed's "
                        + "violations are NOT listed; re-run that seed alone to see them")
                .isEmpty();

        // ⚠️ THE BUDGET IS REPORTED, NOT ASSERTED, and that is performance.md
        // rule 9: "Gate on allocation, trend on time ... a threshold on it
        // becomes noise that people learn to re-run until green -- which is
        // worse than no gate". An earlier draft asserted `< 60s` on wall clock.
        // Against the run times in `BUDGET_MILLIS`'s javadoc -- not its value,
        // which IS the budget -- that is a dead band near 4x, so a 3x
        // `checkChain` regression passes while a 2-core runner reds with no
        // defect at all. `scripts/check-suite-time.sh` is the designated home
        // for the budget, and M0.88 records three Gradle invocations running
        // concurrently on this machine as a routine occurrence.
        System.out.printf("sweep: %d seeds in %d ms, %d readers judged, budget %d ms%n",
                SEEDS, elapsed, readersChecked, BUDGET_MILLIS);
    }
}
