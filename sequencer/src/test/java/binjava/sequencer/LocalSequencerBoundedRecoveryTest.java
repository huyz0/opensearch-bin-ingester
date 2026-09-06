// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.BoundedRecoveryFixture.PREFIX;
import static binjava.sequencer.BoundedRecoveryFixture.frozen;
import static binjava.sequencer.BoundedRecoveryFixture.manager;
import static binjava.sequencer.BoundedRecoveryFixture.noRenew;
import static binjava.sequencer.BoundedRecoveryFixture.request;
import static binjava.sequencer.BoundedRecoveryFixture.streamOfTerm;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Recovery bounded by a checkpoint, not by the cluster's history (M4.9).
 *
 * <p>⚠️ ASSERTED BY REQUEST COUNT, never only by the reconstructed result — a
 * recovery that reads the whole chain anyway produces exactly the right
 * offsets. The row says so in as many words, and the M4.6e cost test it cites
 * is the reason the claim has to be true HERE rather than merely asserted
 * there.
 *
 * <p>⚠️ AND IT MUST BOUND THE CROSSING, not merely own-chain replay. M4.6e's
 * crossing is TRANSITIVE: a takeover re-reads every entry of every ancestor, so
 * per-failover cost is O(all commit-log entries ever written) and grows without
 * bound over a cluster's lifetime. Measured on THIS harness against the tree
 * before the change: 29 total requests at one prior term, 284 at sixteen.
 *
 * <p>⚠️ THREE REQUEST CLASSES SCALE, NOT ONE — GETs 18 to 243, LISTs 3 to 18,
 * stats 4 to 19 across that same 1-to-16 range — so bounding GETs alone would
 * leave two classes growing. Everything here asserts {@code total()}.
 *
 * <p>⚠️ AN EARLIER VERSION OF THIS PARAGRAPH QUOTED 26/43/77/145/281 with
 * "LISTs and stats each growing 3/4/6/10/18", inherited from the task row. Those
 * describe an older tree, and the stat series was the LIST series written twice.
 * Re-measured here rather than carried forward.
 *
 * <p>⚠️ A CHECKPOINT'S OFFSETS ARE CUMULATIVE, which is what lets one of them
 * terminate the walk: it is built from the writing leader's own recovered
 * state, so it already carries everything that leader inherited. That is the
 * whole mechanism — not "read fewer deltas", but "stop walking".
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerBoundedRecoveryTest {

    /**
     * {@code terms} prior leaders, each committing {@code entries} deltas and
     * checkpointing on EVERY one, then one more takeover measured.
     *
     * <p>⚠️ K IS INJECTED AT 1. The shipped policy takes a thousand deltas, so a
     * fixture using the 4-arg {@code start} would build an ancestry with NO
     * checkpoints in it and measure the UNBOUNDED path while claiming to measure
     * the bounded one. ⚠️ This paragraph was deleted once, with the duplicated
     * fixture it sat beside, leaving two unexplained {@code 1} literals.
     */
    private static StoreCounts takeoverCost(int terms, int entries) throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        for (int t = 0; t < terms; t++) {
            LocalSequencer s = LocalSequencer.start(backing, PREFIX, manager(backing, "pod" + t), 8,
                    noRenew(), 1, frozen()).orElseThrow();
            for (int i = 0; i < entries; i++) {
                s.commit(request("pod" + t, i, "seg/" + t + "/" + i));
            }
            s.close();
        }
        CountingBinStore counting = new CountingBinStore(backing);
        LocalSequencer next = LocalSequencer.start(counting, PREFIX, manager(counting, "last"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        assertThat(next.epoch()).isEqualTo(terms + 1);
        next.close();
        return counting.counts();
    }

    @Test
    void takeoverCostIsCONSTANTInTheNumberOfPriorTerms() throws Exception {
        // ⚠️ THE DIMENSION THAT MAKES THE OLD COST UNBOUNDED OVER A LIFETIME.
        // Before this change: 29 requests at 1 prior term, 284 at 16.
        StoreCounts oneTerm = takeoverCost(1, 12);
        StoreCounts sixteenTerms = takeoverCost(16, 12);

        // ⚠️ ABSOLUTE, not "equal to each other". Review measured
        // `epoch != ownEpoch && crossFromCheckpoint(...)` -- which silently
        // unbounds `CommitLog.recover()` on a chain holding its own checkpoint
        // -- making both sides read 16 and both comparisons pass. Two numbers
        // agreeing says nothing about whether either is right.
        assertThat(oneTerm.total())
                .as("one prior term: the bounded crossing's exact cost")
                .isEqualTo(17L);
        assertThat(sixteenTerms.total())
                .as("sixteen ancestors cost the same as one -- the checkpoint stops the walk")
                .isEqualTo(17L);
    }

    @Test
    void takeoverCostIsCONSTANTInTheLengthOfTheAncestorsChain() throws Exception {
        // ⚠️ THE OTHER DIMENSION. A checkpoint after every delta means the
        // newest one covers the whole chain, so no delta need be read at all.
        StoreCounts shortChain = takeoverCost(1, 12);
        StoreCounts longChain = takeoverCost(1, 60);

        assertThat(shortChain.total())
                .as("a twelve-entry predecessor: the same exact cost")
                .isEqualTo(17L);
        assertThat(longChain.total())
                .as("a five-times longer predecessor costs the same to cross")
                .isEqualTo(17L);
    }



    @Test
    void aPredecessorTOOSHORTToCheckpointStillStopsAtItsAncestorsCheckpoint()
            throws Exception {
        // ⚠️ THE IN-LOOP PROBE IS REACHED BY NOTHING ELSE. Every other fixture
        // checkpoints EVERY term, so the first-hop cross always succeeds and the
        // walk's own probe never fires with a hit. Measured: issuing the probe
        // and ignoring its answer survived all 294 tests, and the request counts
        // did not move either.
        // ⚠️ THE UNCOVERED CASE IS THE COMMON ONE -- a term shorter than K and
        // than T, sitting above terms that did checkpoint. Measured with K above
        // the term length and no in-loop probe: 30/84/300 requests at 1/4/16.
        MemoryBinStore backing = new MemoryBinStore();
        for (int t = 0; t < 8; t++) {
            LocalSequencer s = LocalSequencer.start(backing, PREFIX, manager(backing, "pod" + t), 8,
                    noRenew(), 1, frozen()).orElseThrow();
            for (int i = 0; i < 3; i++) {
                s.commit(request("pod" + t, i, "seg/" + t + "/" + i, streamOfTerm(t)));
            }
            s.close();
        }
        // The last prior term is too short to reach K, so it has NO checkpoint.
        LocalSequencer shortTerm = LocalSequencer.start(backing, PREFIX,
                manager(backing, "podshort"), 8, noRenew(), 1_000_000, frozen()).orElseThrow();
        shortTerm.commit(request("podshort", 0, "seg/short", streamOfTerm(99)));
        shortTerm.close();

        CountingBinStore counting = new CountingBinStore(backing);
        LocalSequencer next = LocalSequencer.start(counting, PREFIX, manager(counting, "last"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        try {
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("term 0's records survive eight terms and an uncheckpointed one")
                    .isEqualTo(3);
            // ⚠️ EXACT, and it was an inequality until review measured what
            // lived in the slack: `Hop(chainEpoch, checkpoint.sequence() - 1,
            // upTo)` -- an off-by-one that re-reads one covered delta --
            // measures 27 and stayed green under `<= 30`.
            assertThat(counting.counts().total())
                    .as("the walk stopped at the first ancestor that HAS a checkpoint, "
                            + "rather than running to the origin")
                    .isEqualTo(26L);
        } finally {
            next.close();
        }
    }




}
