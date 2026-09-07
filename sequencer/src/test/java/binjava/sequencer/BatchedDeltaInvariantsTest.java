// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import binjava.sequencer.Invariants.Violation;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The invariant checkers over a BATCHED delta -- one carrying more than one
 * segment (M4.13n).
 *
 * <p>⚠️ THIS IS THE NORMAL CASE, not an edge one. M4's scope item 5 is "one
 * delta carries every stream and every contributing pod", and it is what
 * non-negotiable 6 rests on: request rates scale with segments, never with pods.
 * All three checker fold sites called {@code delta.runs()}, which REFUSES a
 * multi-segment delta, so every one of them threw
 * {@code IllegalStateException: this delta batches N segments}.
 *
 * <p>⚠️ THE GUARD IS RIGHT AND THE CALLERS WERE WRONG. {@code runs()} refuses
 * because pairing a run with the wrong segment reads records from the wrong
 * object. But an OFFSET is a stream fact rather than a segment fact --
 * {@code ChainReplay.fold} says exactly that and uses {@code allRuns()} -- and
 * all three sites are folding offsets.
 *
 * <p>⚠️ NOTHING CAUGHT IT BECAUSE THE SWEEP NEVER BATCHES:
 * {@code CommitProtocolSimulation} drives {@code commit(} and never
 * {@code commitAll}. The suite was green because the case was ABSENT, which
 * would have let M4's completion condition be met by 1,000 seeds that never
 * exercised batching.
 */
class BatchedDeltaInvariantsTest {

    private static final RunKey RA = new RunKey(InvariantFixtures.A, 0);
    private static final RunKey RB = new RunKey(InvariantFixtures.A, 1);
    private static final String PREFIX = InvariantFixtures.PREFIX;

    private static MemoryBinStore batched() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        log.open(0, 0);
        // Two pods' segments in ONE delta, both committing stream RA, and one
        // also committing RB -- the shape `commitAll` exists to produce.
        log.commitAll(List.of(
                new CommitRequest("pod1", "inc-1", 1, "seg/A", Map.of(RA, 3)),
                new CommitRequest("pod2", "inc-2", 1, "seg/B", Map.of(RA, 4, RB, 2))));
        return store;
    }

    @Test
    void checkChainREADSABatchedDeltaRatherThanThrowing() throws Exception {
        assertThat(Invariants.checkChain(batched(), PREFIX, 1))
                .as("a well-formed batched delta violates nothing")
                .isEmpty();
    }

    /**
     * ⚠️ AND IT MUST STILL CATCH A DEFECT INSIDE ONE, or reading the batch would
     * have been swapped for ignoring it. {@code allRuns()} returns runs in
     * SEGMENT order, so a batch whose second segment carries the EARLIER offsets
     * has segment order disagreeing with offset order -- the writer-side reorder
     * I4's second clause is about in M4.
     *
     * <p>⚠️ NO OFFSET IS HANDED OUT TWICE HERE, and an earlier draft of this
     * comment said one was. The two runs are DISJOINT -- 3 records at 4 covers
     * 4..6, 4 records at 0 covers 0..3, together 0..6 exactly once. What is
     * wrong is only their ORDER.
     *
     * <p>⚠️ SO THE `I2` LABEL IS A MISNOMER FOR THIS SHAPE, which the assertion
     * below records rather than hides. {@code checkChain} reports both a `gap`
     * (segment A resumes at 4, skipping 4) and an `I2` (segment B resumes at 0
     * below the mark of 7) -- and I2 means "an offset was REASSIGNED", which did
     * not happen. Detecting it is right; naming it I2 is the conflation
     * {@code checkChain}'s own comment warns against, arriving from a direction
     * that comment did not anticipate. Renaming belongs to **M4.13f**, which is
     * pointed at exactly this shape.
     */
    @Test
    void aBatchedDeltaWhoseSECONDSegmentREWINDSIsStillCaught() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 4)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 4, 0)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("both arms fire: a gap for A resuming at 4, then I2 for B resuming at 0 -- "
                        + "and I2 is a MISNOMER here, since nothing was reassigned (M4.13f)")
                .containsExactlyInAnyOrder("gap", "I2");
    }

    @Test
    void checkReaderJUDGESAReaderOfABatchedDelta() throws Exception {
        MemoryBinStore store = batched();
        CommitLog reader = new CommitLog(store, PREFIX, 1);
        reader.recover();

        assertThat(reader.offsets())
                .as("production folds both segments: RA gets 3 then 4 more")
                .containsEntry(RA, 7L).containsEntry(RB, 2L);
        assertThat(ReaderInvariants.checkReader(
                store, PREFIX, ReaderInvariants.ReaderView.of(reader)))
                .as("and the checker must agree rather than throw")
                .isEmpty();
    }

    /**
     * ⚠️ THE SOLE PIN FOR THE INHERITED FOLD, which is the third checker site
     * and the one reached only across an epoch boundary. Reverting
     * {@code offsetsSealedInto} to {@code runs()} fails THIS TEST AND NOTHING
     * ELSE in the suite.
     *
     * <p>⚠️ AN EARLIER DRAFT OF THIS COMMENT CLAIMED THE OPPOSITE, and said so
     * as a MEASURED result. It was measured wrongly: the probe searched for the
     * fold line by its sixteen-space indent, which also matches INSIDE the
     * twenty-space line in {@code checkChain}, so both runs mutated that site
     * and never touched this one. Review caught it. Mutating by LINE NUMBER
     * rather than by a whitespace-sensitive substring is what settles which
     * site a measurement actually moved.
     *
     * <p>⚠️ IT ALSO COMMITS IN THE SUCCESSOR, and that line is load-bearing.
     * Without it the inherited base is computed and then discarded, because no
     * delta in epoch 2 is folded against it -- so a fold that read only the
     * FIRST segment of the batched predecessor left the whole suite green.
     */
    @Test
    void aSUCCESSORInheritsFromABatchedPredecessor() throws Exception {
        MemoryBinStore store = batched();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        first.recover();
        binjava.format.Seal seal = first.seal(2, 8);

        CommitLog second = new CommitLog(store, PREFIX, 2);
        second.open(1, seal.sequence());
        // ⚠️ WITHOUT THIS COMMIT THE INHERITED BASE IS NEVER COMPARED AGAINST
        // ANYTHING, so a fold reading only the predecessor's FIRST segment is
        // invisible. Measured: with it, that mutation reports a spurious
        // `gap ... resumes at 7, skipping 4` against a correct chain.
        assertThat(second.offsets())
                .as("the successor carries BOTH segments of the batched predecessor")
                .containsEntry(RA, 7L);

        second.commit("seg/C", Map.of(RA, 2));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .as("and the successor's own delta continues from 7, not from 3")
                .isEmpty();
    }
}
