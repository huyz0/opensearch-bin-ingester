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
        // ⚠️ EVERY FLOOR BELOW IS `SEEDS * k`, so all six measure work PER SEED
        // and are invariant under the seed count. Review MEASURED `SEEDS` 1000
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

        FaultInjectingStore.Faults faults =
                new FaultInjectingStore.Faults(0.05, 0.05, 0.1, 0);

        List<String> failing = new ArrayList<>();
        long commits = 0;
        long takeovers = 0;
        long epochsBurned = 0;
        int faultsFired = 0;
        long readersChecked = 0;
        boolean ackFloorAlwaysZero = true;
        long ackEvents = 0;
        long start = System.nanoTime();
        for (long seed = 0; seed < SEEDS; seed++) {
            var run = CommitProtocolSimulation.run(seed, ROUNDS, PODS, faults);
            commits += run.commits();
            takeovers += run.takeovers();
            epochsBurned += run.highestEpoch();
            faultsFired += run.faults().size();
            readersChecked += run.readersChecked();
            ackEvents += run.acks().size();
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
                .as("mean commits per seed must stay near the measured 7.4, not collapse "
                        + "to the 0.9 a shallow run gives")
                .isGreaterThan(SEEDS * 3L);
        assertThat(takeovers)
                .as("and mean takeovers near the measured 2.87, against 0.97 shallow -- the "
                        + "flattest of these floors, so it is set at 1.5 per seed rather than "
                        + "1: review measured 2,872 against a floor of 1,000, only 3.2% of "
                        + "the separation, with ROUNDS=4 already clearing it")
                .isGreaterThanOrEqualTo(SEEDS * 3L / 2);
        assertThat(epochsBurned)
                .as("and the epoch depth near the measured 80.2 per seed, against 1.3 -- "
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
        assertThat(readersChecked)
                .as("one reader judged per epoch that has a chain -- measured 74.17 per "
                        + "seed, so a floor of 10 clears every cumulative prefix by 7.3x")
                .isGreaterThanOrEqualTo(SEEDS * 10L);
        // ⚠️ THE TRACE MUST EXIST BEFORE ITS FLOOR MEANS ANYTHING. The floor
        // assertion below is RELATIVE -- "no chain's floor is above 0" -- so an
        // EMPTY trace satisfies it, and review MEASURED exactly that: with every
        // `acks.add` neutralised the whole suite stayed green while this test
        // reported success. That is the slot-0 CONTINUE defect reintroduced one
        // level up, on the trace rather than on its floor. Measured 17.53 events
        // per seed, so a floor of 5 clears every cumulative prefix by 3.09x.
        assertThat(ackEvents)
                .as("the ack trace must be non-empty before its floor says anything -- "
                        + "`checkAckOrder` over an empty list reports nothing, forever")
                .isGreaterThanOrEqualTo(SEEDS * 5L);
        assertThat(ackFloorAlwaysZero)
                .as("every chain's ack trace is based at 0, which is what the slot-0 CONTINUE "
                        + "event exists to guarantee -- it was documented in three places and "
                        + "emitted nowhere until review measured the floor sitting at 1")
                .isTrue();

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
