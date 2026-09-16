// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * I1-I5 across 1,000 seeds of a fleet where most pods FORWARD (M5.7, FR-11).
 *
 * <p>⚠️ A SECOND SWEEP, NOT A CHANGED ONE, and that is the whole reason this is
 * a separate file. M4's completion condition is "I1-I5 hold across 1,000
 * deterministic simulation seeds", and its sweep is the artefact that holds it;
 * turning forwarding on inside that loop would move every seed's draw sequence,
 * so the numbers M4 recorded could never be reproduced from this tree again.
 * The fleet whose PROTOCOL M5 builds is a DIFFERENT one -- the leaseholder is
 * one pod among many -- so it gets its own 1,000 seeds. ⚠️ THE PROTOCOL, NOT
 * THE DEPLOYMENT, and this sentence said "the deployment M5 makes correct"
 * until M5.20's sweep: every test of forwarding in this tree runs over a TEST
 * `SequencerTransport`, the production one is M5.6e (owned by M8), so no
 * deployment is made correct here.
 *
 * <p>⚠️ MEASURED ON THIS TREE, at 1,000 seeds, 120 rounds and 3 pods under
 * {@link CommitProtocolSweepTest#SWEEP_FAULTS}, against the same profile with
 * forwarding off:
 *
 * <pre>
 *   fleet          zero-commit  commits  takeovers  fwd commits  attempts  refusals
 *   leader only              12   11.11       4.81           -         -         -
 *   with forwarding           3   14.82       4.60        4.52     39.54     22.82
 *
 *   seeds that forwarded at least once  956 / 1000
 *   seeds that saw a refusal            996 / 1000
 * </pre>
 *
 * <p>⚠️ MOST FORWARDS DO NOT LAND, and that is the fleet rather than a defect:
 * a follower forwards whether or not anybody is leading, so under this fault
 * profile 22.82 of the 39.54 attempts per seed are REFUSED by an endpoint that
 * answers nothing. ⚠️ THOSE REFUSALS ARE THE POINT. They are the only rounds
 * where the lease and the routing table disagree, and so the only ones that
 * reach {@code RemoteSequencer}'s refusal arm at all -- review MEASURED that
 * arm dead in all 1,000 seeds of an earlier draft, with the transport's
 * refusal replaced by an {@code AssertionError} and the sweep still green.
 *
 * <p>⚠️ THE RE-READ, NOT THE RESEND. MEASURED: 22,824 refusals caught, 22,136
 * lease re-reads, and ZERO resends to a moved holder. A resend needs the lease
 * to MOVE between the send and the re-read, which a single-threaded driver
 * with atomic rounds cannot produce; {@code RemoteSequencerTest} moves it by
 * hand and covers that arm. Saying "re-read-and-resend" here would claim M5.5
 * is exercised by a sweep that cannot reach half of it.
 *
 * <p>⚠️ A DOOMED FORWARD DOES NOT COST THE ROUND. With no leader the driver
 * falls through to the election, because a follower that cannot reach anybody
 * must not stop the cluster electing somebody -- measured, consuming those
 * rounds dropped takeovers while inflating attempts.
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitProtocolForwardingSweepTest {

    /**
     * ⚠️ NO SHRINKING PROPERTY, DELIBERATELY. A draft had
     * {@code forwarding.sweep.seeds} mirroring the leader-only sweep's knob,
     * and review MEASURED that it reaches the forked test JVM through neither
     * {@code -D} nor {@code systemProp.} -- so it was a knob that did nothing
     * while looking like the supported way to bisect, and one more name a
     * future audit of sweep-shrinking properties would have to know. Bisect by
     * editing this constant in a working copy, which leaves a diff.
     */
    private static final int SEEDS = 1000;
    private static final int ROUNDS = 120;
    private static final int PODS = 3;

    @Test
    void theInvariantsHoldOverAFleetWhereMostPodsDoNotHoldTheLease() throws Exception {
        // ⚠️ THE WORKLOAD IS ASSERTED, NOT DECLARED, exactly as the leader-only
        // sweep does it and for the same reason: a run at 1/100th the depth
        // satisfies every floor below, so the depth has to be part of the test
        // rather than a constant beside it.
        assertThat(SEEDS)
                .as("the same 1,000 seeds M4's completion condition names, over the fleet "
                        + "whose forwarding protocol M5 builds -- no production transport "
                        + "carries it yet (M5.6e, M8)")
                .isEqualTo(1000);
        assertThat(ROUNDS).as("act-or-fail steps per seed").isEqualTo(120);
        assertThat(PODS).as("logical pods, of which at most one holds the lease").isEqualTo(3);

        List<String> failing = new ArrayList<>();
        long commits = 0;
        long forwarded = 0;
        long attempts = 0;
        long refusals = 0;
        long acked = 0;
        long zombieWrites = 0;
        int zeroCommitSeeds = 0;
        int seedsThatForwarded = 0;
        int seedsThatSawARefusal = 0;
        for (long seed = 0; seed < SEEDS; seed++) {
            var run = CommitProtocolSimulation.runForwarding(
                    seed, ROUNDS, PODS, CommitProtocolSweepTest.SWEEP_FAULTS);
            commits += run.commits();
            forwarded += run.forwardedCommits();
            attempts += run.forwardAttempts();
            refusals += run.forwardRefusals();
            zombieWrites += run.zombieWrites();
            acked += run.acks().stream().filter(AckOrderInvariants.AckEvent::isAck).count();
            if (run.commits() == 0) {
                zeroCommitSeeds++;
            }
            if (run.forwardedCommits() > 0) {
                seedsThatForwarded++;
            }
            if (run.forwardRefusals() > 0) {
                seedsThatSawARefusal++;
            }
            if (!run.violations().isEmpty()) {
                // ⚠️ ONE LINE PER SEED, not one per violation: `checkChain`
                // walks once per epoch, so one chain-level defect is re-reported
                // at every epoch whose walk covers it -- megabytes of XML with
                // the seed, which is the whole point, buried in it.
                var first = run.violations().get(0);
                failing.add("seed " + seed + ": " + first.invariant() + " -- " + first.detail()
                        + (run.violations().size() > 1
                                ? " (+" + (run.violations().size() - 1) + " more on this seed)"
                                : ""));
            }
        }

        // ⚠️ ANTI-VACUITY FIRST, before the invariant assertion, because a sweep
        // over a fleet that never forwarded reports CLEAN about the one thing
        // this file exists to exercise -- and would keep reporting clean if the
        // forwarding branch were deleted outright.
        assertThat(forwarded)
                .as("mean forwarded commits per seed must stay near the measured 4.52, not "
                        + "collapse toward the zero a fleet with no forwarding gives")
                .isGreaterThan(SEEDS * 2L);
        assertThat(attempts)
                .as("and attempts near the measured 39.54 -- attempts and successes are "
                        + "different numbers, and only the pair separates 'no pod forwarded' "
                        + "from 'every forward was refused'")
                .isGreaterThan(SEEDS * 20L);
        assertThat(refusals)
                .as("and REFUSALS near the measured 22.82: they are the only rounds where the "
                        + "lease and the routing table disagree, so a sweep without them "
                        + "leaves per-pod endpoints, `gone()` and RemoteSequencer's "
                        + "re-read-and-resend arm all unfalsifiable")
                .isGreaterThan(SEEDS * 10L);
        assertThat(seedsThatForwarded)
                .as("forwarding must be spread across seeds, not concentrated in a few: a "
                        + "total floor alone is satisfiable by one seed forwarding thousands "
                        + "of times. Measured 956 of 1,000")
                .isGreaterThan(SEEDS * 8 / 10);
        assertThat(seedsThatSawARefusal)
                .as("and so must the disagreement -- measured 996 of 1,000")
                .isGreaterThan(SEEDS * 8 / 10);
        assertThat(acked)
                .as("every commit is ACKED in the trace `checkAckOrder` judges -- the leader's, "
                        + "the forwarded ones, and a ZOMBIE's if one ever lands. Deleting the "
                        + "forwarded `acks.add` leaves this short by the forwarded count while "
                        + "every other floor here is untouched. ⚠️ `zombieWrites` is in the sum "
                        + "although it is 0 today (859 attempts, none landed, because M4.47's "
                        + "seal-the-run fences a zombie first): without it, the day M4.13e makes "
                        + "one land this fails and blames the forwarded population")
                .isEqualTo(commits + zombieWrites);
        assertThat(commits)
                .as("and the fleet must still be committing at least at the leader-only rate "
                        + "-- measured 14.82 per seed against 11.11 without forwarding. "
                        + "⚠️ Followers do not purely ADD: leader-originated commits fall from "
                        + "11.110 to 10.304 per seed, because a consumed round is one the "
                        + "leader did not get")
                .isGreaterThan(SEEDS * 3L);
        assertThat(zeroCommitSeeds)
                .as("seeds that commit nothing hold I1-I5 VACUOUSLY; measured 3 of 1,000. "
                        + "⚠️ Derived from THIS fleet rather than inherited from the "
                        + "leader-only sweep's SEEDS/40, which against a measured 3 would be "
                        + "8.3x headroom -- a bound that loose stops nothing")
                .isLessThanOrEqualTo(SEEDS / 100);

        assertThat(failing)
                .as("I1-I5 over a fleet where most pods forward rather than lead")
                .isEmpty();
    }
}
