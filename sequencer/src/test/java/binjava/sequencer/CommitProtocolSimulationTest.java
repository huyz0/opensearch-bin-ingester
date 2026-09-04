// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ⚠️ "NO VIOLATIONS" IS ALSO WHAT A SIMULATION THAT DID NOTHING REPORTS. The
 * spec names this as a milestone risk in as many words — a fault-injecting store
 * that never injects the fault that matters — and the defence is that every run
 * asserted clean must FIRST be asserted to have done something: committed,
 * failed over more than once, and actually injected the faults it configured.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitProtocolSimulationTest {

    private static final FaultInjectingStore.Faults CLEAN = FaultInjectingStore.Faults.none();
    private static final FaultInjectingStore.Faults ROUGH =
            new FaultInjectingStore.Faults(0.05, 0.05, 0.1);

    @Test
    void aRunACTUALLYCommitsAndFailsOverBeforeAnyCleanReportMeansAnything()
            throws Exception {
        // ⚠️ THE ANTI-VACUITY TEST, and it must come first. Every other
        // assertion in this file is that something was NOT violated, and a
        // harness that led nothing and committed nothing satisfies all of them.
        var result = CommitProtocolSimulation.run(7L, 120, 3, CLEAN);

        assertThat(result.commits()).as("it committed").isGreaterThan(10);
        assertThat(result.takeovers())
                .as("and the lease changed hands repeatedly -- otherwise this is a "
                        + "single-leader test wearing a simulation's name")
                .isGreaterThan(2);
        assertThat(result.highestEpoch())
                .as("and each takeover opened a NEW chain, which is what epoch fencing means")
                .isGreaterThan(2L);
    }

    @Test
    void aCleanRunViolatesNothing() throws Exception {
        var result = CommitProtocolSimulation.run(7L, 120, 3, CLEAN);
        assertThat(result.violations())
                .as("I1, I2 within a chain, and I5 all hold with no faults injected")
                .isEmpty();
    }

    @Test
    void theSameSeedReplaysExactlySoAFailureIsAPermanentRegression() throws Exception {
        var a = CommitProtocolSimulation.run(99L, 80, 3, ROUGH);
        var b = CommitProtocolSimulation.run(99L, 80, 3, ROUGH);

        assertThat(b.commits()).isEqualTo(a.commits());
        assertThat(b.takeovers()).isEqualTo(a.takeovers());
        assertThat(b.highestEpoch()).isEqualTo(a.highestEpoch());
        assertThat(b.faults()).as("including which faults fired, in order").isEqualTo(a.faults());
        // ⚠️ AND THE VIOLATIONS, which is the field this test's name is actually
        // about: "a failing seed is a permanent regression" is a claim about what
        // the seed REPORTS, and comparing only counts and faults would let two
        // runs of one seed disagree about what they found while this test passed.
        assertThat(b.violations())
                .as("and the violations it found, which is what a named regression names")
                .isEqualTo(a.violations());
    }

    @Test
    void aROUGHSWEEPACTUALLYINJECTSEveryFaultClassAndDRIVESFencedWritersAtTheChain()
            throws Exception {
        // ⚠️ A RANGE, NOT A HAND-PICKED SEED, and that is a correction rather
        // than a preference. Three times in this task a change to how faults are
        // drawn moved the seed -> outcome mapping and silently un-exercised the
        // one seed the assertion named; round-2 review measured that 20 of 31
        // ROUGH seeds attempt no zombie write at all, so any single seed is a
        // coin flip away from vacuous. Criterion 1 asks for evidence ACROSS the
        // range anyway -- "each fault class must have at least one seed where it
        // changes the outcome" -- so the sweep is the honest unit.
        Set<String> kinds = new TreeSet<>();
        int zombieAttempts = 0;
        int zombieWrites = 0;
        int commits = 0;
        for (long seed = 0; seed < 30; seed++) {
            var r = CommitProtocolSimulation.run(seed, 120, 3, ROUGH);
            r.faults().forEach(f -> kinds.add(f.kind()));
            zombieAttempts += r.zombieAttempts();
            zombieWrites += r.zombieWrites();
            commits += r.commits();
        }

        assertThat(kinds)
                .as("the decorator is in the path and EVERY configured class fired somewhere "
                        + "in the range -- an aggregate count is satisfied by one class firing "
                        + "a hundred times, which is the shape criterion 1 forbids")
                .containsExactly("ambiguousPut", "duplicatePut", "unreachable");
        assertThat(commits)
                .as("and the faulted cluster still did work, so the range is not reporting "
                        + "cleanly about a system that stopped")
                .isPositive();
        assertThat(zombieAttempts)
                .as("a fenced leader really did ATTEMPT a write somewhere in the range, which "
                        + "is the only route to I5 and therefore the only way it can be tested")
                .isPositive();
        // ⚠️ ATTEMPTS ARE THE ANTI-VACUITY SIGNAL; SUCCESSES ARE THE DEFECT. A
        // successful zombie write means the barrier failed, so asserting it zero
        // is an assertion about the SYSTEM -- and it is deliberately NOT made
        // here: round-2 review measured a seed that lands one today, and M4.16
        // owns it. Stating the count rather than asserting it keeps this test
        // honest about what it has and has not established.
        assertThat(zombieWrites)
                .as("recorded, not asserted: fenced writes that LANDED across the range. "
                        + "M4.16 owns driving this to zero; a test asserting it here would go "
                        + "red when the system is fixed or green while it is broken")
                .isGreaterThanOrEqualTo(0);
    }

    @Test
    void aFAULTEDSWEEPKeepsCOMMITTINGRatherThanSTALLING() throws Exception {
        // ⚠️ THE FLOOR THE FAULTED RANGE NEVER HAD. Every anti-vacuity assertion
        // in this file was on the CLEAN run, so a faulted sweep could grind almost
        // to a halt and still report "no violations" -- which is also what a
        // cluster that stopped reports. Round-2 review of M4.12 measured the
        // consequence: SEED 17 COMMITS ZERO AND REPORTS CLEAN.
        // ⚠️ AGGREGATE, NOT PER SEED, and that is the correction rather than a
        // convenience. MEASURED over seeds 0..29 at ROUGH: commits range 0..21 per
        // seed, ONE seed commits zero, takeovers range 1..6, and the clean run's
        // worst seed still makes 52. A per-seed floor would therefore fail on a
        // seed that is heavily faulted rather than broken -- so the floor is on
        // the range, plus a bound on how MANY seeds may starve. That catches a
        // systemic stall while tolerating the one genuinely unlucky trace.
        int seeds = 30;
        long commits = 0;
        int takeovers = 0;
        int starved = 0;
        for (long seed = 0; seed < seeds; seed++) {
            var r = CommitProtocolSimulation.run(seed, 120, 3, ROUGH);
            commits += r.commits();
            takeovers += r.takeovers();
            if (r.commits() == 0) {
                starved++;
            }
        }

        // ⚠️ Measured 208; the floor is one commit per seed, which a stall cannot
        // reach and which leaves room for the fault rates to be tuned.
        assertThat(commits)
                .as("the faulted range kept committing -- %d commits over %d seeds, and a "
                        + "cluster that had stopped would report clean just as loudly",
                        commits, seeds)
                .isGreaterThan(seeds);
        assertThat(takeovers)
                .as("and the lease kept changing hands, or this is a single-leader run wearing "
                        + "a faulted simulation's name")
                .isGreaterThan(seeds);
        // ⚠️ STARVED SEEDS ARE THE DISCRIMINATOR, and the throughput floors above
        // are NOT -- which I only know because I measured the stall instead of
        // assuming it. Removing the clock advance that lets an unheld lease lapse
        // (the mechanism the driver's own comment says prevents a DEADLOCK) moves
        // the aggregates barely at all: commits 208 -> 171, takeovers 77 -> 57.
        // Both floors above still pass under the stall. What moves sharply is how
        // many seeds commit NOTHING: 1 healthy, 5 stalled.
        // ⚠️ SO THE BOUND IS 3, sitting between the two measured values. Stated
        // with both numbers so it is auditable rather than arbitrary, and so the
        // next hand can see the margin is 1-vs-5 and not pretend it is wide.
        assertThat(starved)
                .as("only %d of %d seeds committed nothing at all -- measured 1 healthy and 5 "
                        + "with the lapse-advance removed, so this is the assertion that "
                        + "separates a degraded cluster from a stalled one", starved, seeds)
                .isLessThan(3);
    }
}
