// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;
import java.util.List;

/**
 * What the store JUDGED while a leader released its lease politely (M5.26).
 *
 * <p>⚠️ EXTRACTED FROM {@code CommitProtocolSimulation} under code-structure.md
 * rule 1. NO LINE-COUNT TRIGGER IS CLAIMED (M5.54). The seam is real: the
 * ACTOR of a release is the question it exists to ask, and folding
 * the reading into the driver is what let the first version measure nothing.
 *
 * <p>⚠️ IT READS THE STORE, NOT THE CALLER'S OWN ASSIGNMENT. The first version
 * of this compared {@code faulty.actingPod()} against the pod it had been set
 * to one statement earlier, in a single-threaded driver -- true by
 * construction. Both reviewers killed it with the same mutation: restore the
 * stale actor AFTER the count and the whole pre-M5.26 defect is back with the
 * sweep green. {@code FaultInjectingStore} already records every partition
 * refusal as {@code Injected("partition", "pod:X")}, so what it actually
 * refused, and who it blamed, is on the record without new machinery.
 */
final class GracefulReleaseMeter {

    /** A release that may fail, so the meter can see whether it did. */
    interface Release {
        void run() throws IOException;
    }

    private final FaultInjectingStore faulty;
    private int released;
    private int refusedForAnotherPod;
    private int refusalsSeen;
    private int releasedDespiteOwnPartition;

    GracefulReleaseMeter(FaultInjectingStore faulty) {
        this.faulty = faulty;
    }

    /**
     * Runs {@code release} with {@code leaderPod} acting, and records what the
     * store did.
     *
     * <p>⚠️ THE ACTOR IS SET HERE AND UNCONDITIONALLY, which is M5.26 itself.
     * Leaving it unset let the store judge a graceful release against whoever
     * acted last and refuse it for a partition injected somewhere else.
     */
    void observe(String leaderPod, Release release) {
        faulty.actingAs(leaderPod);
        int before = faulty.injected().size();
        // ⚠️ THE VERB, NOT THE TOTAL (M5.29). See `released()`.
        int writesBefore = faulty.calls("putIfMatch");
        boolean completed = true;
        try {
            release.run();
        } catch (IOException injected) {
            // a fault during release leaves the lease to expire, which is
            // exactly the ungraceful path
            completed = false;
        }
        if (completed && faulty.calls("putIfMatch") > writesBefore) {
            // ⚠️ THE RELEASE THAT ISSUED THE CONDITIONAL WRITE, not the
            // branch that was entered, not merely a call that returned, and
            // not merely a call that reached the store. "ISSUED", because
            // `releaseLocked` discards the Optional -- see `released()`. MEASURED, all three: counting branch
            // entries kept a floor at 1,713 with `leader.close()` DELETED
            // outright; counting normal returns kept it green with the close
            // replaced by an empty lambda; and counting ANY store call kept it
            // green at 1,626 with the release swapped for a `stat`.
            released++;
            // ⚠️ ASKED OF THE LEADER, NOT OF THE ACTOR (M5.31). The actor is
            // what the store judged; the leader is who the release was FOR, and
            // the defect is exactly the two differing. A release that completed
            // while the leader was cut off is one the leader's own partition
            // should have refused.
            if (faulty.isPartitioned(leaderPod)) {
                releasedDespiteOwnPartition++;
            }
        }
        List<FaultInjectingStore.Injected> during =
                faulty.injected().subList(before, faulty.injected().size());
        for (FaultInjectingStore.Injected one : during) {
            // ⚠️ A REFUSAL NAMING ANOTHER POD IS THE DEFECT ITSELF. The
            // leader's OWN partition refusing its release is legitimate --
            // that is the ungraceful path. Being refused for somebody else's
            // is what naming no actor caused.
            if (!"partition".equals(one.kind())) {
                continue;
            }
            // ⚠️ COUNTED BEFORE THE CARVE-OUT, so the floor over it measures
            // what this loop can SEE rather than what it concludes. Counting
            // after survives the sweep and is caught by
            // `aRefusalBLAMINGANOTHERPODIsCountedAndTheReleaseIsNotCredited`
            // and by `TWORefusalsBLAMINGANOTHERPODCountTheWINDOWOnce` --
            // MEASURED, both ways. Named rather than counted, because the
            // count here read "one" until the second of those cases arrived.
            // ⚠️ THIS INCREMENT SITS ABOVE THE KEY TEST, so it counts every
            // refusal the scan REACHES -- and the `break` below ends the scan,
            // so what it reaches is every refusal up to and including the
            // first that blames another pod. It separates from
            // `refusedForAnotherPod` only across refusals blaming the LEADER,
            // which is `TWORefusalsInOneWindowAreCountedTWICE`;
            // `TWORefusalsBLAMINGANOTHERPODCountTheWINDOWOnce` is where the
            // two COUNTERS coincide, at 1.
            // ⚠️ THE TWO UNITS COINCIDE OVER THE SWEEP TODAY AT 38 REFUSALS
            // SEEN -- which is NOT 38 apiece, and the counters are not what
            // coincides there: `CommitProtocolSweepTest` asserts
            // `gracefulReleasesRefused` is ZERO. `refuseIfPartitioned` throws
            // and aborts the release, so no window carries two, and forcing
            // this loop to stop at the first also gives 38. A release path
            // that swallowed the IOException and kept calling would separate
            // them.
            refusalsSeen++;
            if (!("pod:" + leaderPod).equals(one.key())) {
                refusedForAnotherPod++;
                break;
            }
        }
    }

    /**
     * Releases that returned AND issued the conditional write.
     *
     * <p>⚠️ "ISSUED", NOT "WROTE". {@code LeaseManager.releaseLocked}
     * DISCARDS the {@code Optional} its {@code putIfMatch} returns, and
     * {@code release()}'s own javadoc says the call is "harmless when already
     * fenced, because the conditional write simply loses" -- so a fenced leader
     * completes, is credited here, and changed nothing on the store. The floor
     * still binds: an issued-write count goes to 0 under the verb swap exactly
     * as a landed-write count would.
     *
     * <p>⚠️ NOT "returned rather than throwing", which is the weaker rule
     * this rejected first: an empty lambda returns perfectly well and hands
     * nothing back, and review MEASURED that version staying green with
     * {@code leader.close()} replaced by one. Simplifying the guard to
     * {@code if (completed)} reinstates exactly that hole.
     *
     * <p>⚠️ AND NOT "reached the store", which is the weaker rule M5.29
     * replaced. A {@code stat} reaches the store exactly as a release does, so
     * that version could not tell WHICH work was done: review MEASURED
     * {@code leases.release()} in {@code LocalSequencer.close} swapped for
     * another verb leaving the sweep's floor GREEN at 1,626. A release, in
     * {@code LeaseManager.releaseLocked}, is a CONDITIONAL WRITE of the expired
     * lease -- so {@code putIfMatch} is what gets credited, and the same swap
     * now reds the floor at 0. MEASURED both ways.
     */

    int released() {
        return counts().released();
    }

    /**
     * All four counters together, as ONE value (M5.51).
     *
     * <p>⚠️ THIS EXISTS TO MAKE A TRANSPOSITION UNREPRESENTABLE, which is rung
     * 1 rather than a guard. `CommitProtocolSimulation.Result` used to take the
     * four as loose {@code int}s, and M5.31 MEASURED the consequence: passing
     * {@code refusedForAnotherPod()} where {@code releasedDespiteOwnPartition()}
     * belongs leaves the WHOLE suite green including the 1,000-seed sweep, and
     * silently removes its only view of the wrongly-ALLOWED direction.
     *
     * <p>⚠️ AND A NUMERIC RELATION CANNOT ANSWER IT, which is what separates
     * this from M5.28's `refusalsSeen &lt; gracefulReleases`: two of these four
     * are HONESTLY ZERO on a healthy sweep, and no relation distinguishes a
     * counter whose true value is 0 from another whose true value is 0.
     *
     * <p>⚠️ THIS DOES NOT CLOSE THE ROW. This constructor and {@link
     * Counts#plus} are still transposable, and so are the sweep's reads of
     * them. **M5.68 owns the residue and enumerates it. Do not re-derive that
     * enumeration here** -- two copies of it is how this paragraph's own
     * history went wrong.
     *
     * <p>⚠️ WHAT IS MEASURED ABOUT THIS CONSTRUCTOR, AND NOTHING BEYOND IT.
     * The M5.31 zero-pair swap ({@code refusedForAnotherPod} for {@code
     * releasedDespiteOwnPartition}) reds exactly three cases:
     * {@code aReleaseTheLEADERSOwnPartitionShouldHaveREFUSEDIsCounted},
     * {@code aRefusalBLAMINGANOTHERPODIsCountedAndTheReleaseIsNotCredited},
     * {@code TWORefusalsBLAMINGANOTHERPODCountTheWINDOWOnce}. A swap of {@code
     * released} with {@code refusalsSeen} reds the sweep even with every
     * accessor rewritten to read its field directly, because {@link
     * CommitProtocolSimulation} builds its {@code Result} from {@code counts()}
     * as well.
     *
     * <p>⚠️ EVERY SENTENCE ABOVE IS A MEASUREMENT AND NOT A MECHANISM, and that
     * is deliberate: four earlier drafts named a mechanism -- a test that never
     * calls this method, then "the only route", which was one of two -- and
     * each was false. Re-run the mutation before editing any of them.
     */
    Counts counts() {
        return new Counts(released, refusedForAnotherPod, refusalsSeen, releasedDespiteOwnPartition);
    }

    /** The four release counters as one value; see {@link #counts()}. */
    record Counts(int released, int refusedForAnotherPod, int refusalsSeen,
            int releasedDespiteOwnPartition) {

        /** The zero of {@link #plus}, for a sweep that has run no seed yet. */
        static Counts none() {
            return new Counts(0, 0, 0, 0);
        }

        /**
         * Componentwise sum, so a sweep accumulates ONE value rather than four.
         *
         * <p>⚠️ THIS EXISTS BECAUSE CLOSING THE CONSTRUCTION SITE WAS NOT
         * ENOUGH, and review measured the gap in the first version of M5.51:
         * `CommitProtocolSweepTest`'s loop still added the four components into
         * four longs, so
         * {@code releasesDespiteOwnPartition += run.releases().refusedForAnotherPod()}
         * compiled AND COULD NOT FAIL -- the sweep asserts both of those
         * isZero, so swapping two expressions that are honestly 0 changes no
         * assertion, no floor, not even the printf. That is verbatim the defect
         * M5.31 measured, reintroduced one layer out by the fix for it.
         *
         * <p>⚠️ ACCUMULATING ONE VALUE COLLAPSES THOSE FOUR SITES INTO THIS
         * CONSTRUCTOR -- pinned NOT the way {@link #counts()} is, which an
         * earlier version of this sentence claimed. No accessor reaches here.
         * What reds a swap here is {@code
         * plusCarriesEachCounterToItsOWNComponent}, whose four counters are
         * DISTINCT so no swap hides in a coincidence of zeros, and the
         * 1,000-seed sweep itself. MEASURED: summing {@code refusalsSeen} into
         * the {@code refusedForAnotherPod} component reds both.
         */
        Counts plus(Counts other) {
            return new Counts(released + other.released,
                    refusedForAnotherPod + other.refusedForAnotherPod,
                    refusalsSeen + other.refusalsSeen,
                    releasedDespiteOwnPartition + other.releasedDespiteOwnPartition);
        }
    }


    /** Releases the store refused while blaming a pod other than the leader. */
    int refusedForAnotherPod() {
        return counts().refusedForAnotherPod();
    }

    /**
     * Partition refusals the store raised inside a release window, whoever it
     * blamed (M5.28).
     *
     * <p>⚠️ REFUSALS, NOT RELEASES. {@link #refusedForAnotherPod} counts at
     * most one per window, because it breaks. This one counts every refusal
     * the scan REACHES, which the same break bounds: every refusal up to and
     * including the first that blames another pod. So the two units separate
     * only across refusals blaming the LEADER -- {@code
     * TWORefusalsInOneWindowAreCountedTWICE} is 2 there, and {@code
     * TWORefusalsBLAMINGANOTHERPODCountTheWINDOWOnce} is 1 for a window that
     * also holds two. Reading this as a release count is how a reader
     * re-derives the retracted claim that the two are ordered by construction.
     *
     * <p>⚠️ THIS IS WHAT MAKES {@link #refusedForAnotherPod} FALSIFIABLE. That
     * one is asserted to be ZERO, so it can only fail by OVER-counting: review
     * MEASURED that misspelling {@code "partition"} in the scan predicate --
     * or emptying the window, or deleting the increment -- leaves the sweep
     * green, because 0 is also the fixed tree's answer. A floor on the
     * refusals the meter can SEE kills TWO of those three -- MEASURED. It does
     * NOT kill a deleted {@code refusedForAnotherPod++}, because on a correct
     * tree that counter is legitimately 0 and the sweep's totals cannot tell
     * blinded from correct. {@link GracefulReleaseMeterTest} is what catches
     * that one, and deleting it restores the unconstrained state this counter
     * was added to end.
     */
    int refusalsSeen() {
        return counts().refusalsSeen();
    }

    /**
     * Releases that completed while the LEADER's own partition should have
     * refused them (M5.31).
     *
     * <p>⚠️ THE DIRECTION {@link #refusedForAnotherPod} CANNOT SEE. Both are
     * M5.26's defect -- the store judging a release against whoever acted last
     * -- but they fail opposite ways. A stale actor that is PARTITIONED refuses
     * a release it should have allowed, which this meter has counted since
     * M5.26. A stale actor that is NOT partitioned ALLOWS one it should have
     * refused, and until this counter nothing in the tree looked for it: review
     * MEASURED the two sets of seeds being different: seed 323 runs 3 releases and 9 commits on a CORRECT tree and 4 and 13 UNDER THE DEFECT, with zero wrongly-blamed refusals either way --
     * the unseen direction showing up in the totals and in no assertion.
     *
     * <p>⚠️ AND IT IS THE WORSE DIRECTION. A wrongly refused release leaves
     * the lease to expire, which is the ungraceful path the cluster already
     * tolerates. A wrongly allowed one hands the lease back for a leader that
     * is cut off and cannot know -- and {@link #released} credits it, so the
     * sweep's graceful-release floor is partly met by releases that should
     * never have completed.
     *
     * <p>⚠️ IT INHERITS {@link #released}'s {@code completed} SCOPE, and that
     * is a stated limit rather than an oversight. A wrongly-allowed release
     * whose write LANDED through the injector's {@code ambiguousPut} arm and
     * then threw is invisible here -- the lease is written expired for a
     * partitioned leader and nothing counts it. Dropping the conjunct is NOT
     * the fix: M5.29 measured {@code record} running above
     * {@code refuseIfPartitioned}, so all 38 legitimate refusals would become
     * false positives on a correct tree.
     *
     * <p>⚠️ ZERO ON A CORRECT TREE BY CONSTRUCTION, which is why the T1 case
     * builds the situation rather than waiting for it: {@code observe} sets the
     * actor to the leader, so a partitioned leader's own release is refused and
     * never completes. Same discipline as
     * {@code aRefusalBLAMINGANOTHERPODIsCountedAndTheReleaseIsNotCredited}.
     */
    int releasedDespiteOwnPartition() {
        return counts().releasedDespiteOwnPartition();
    }
}
