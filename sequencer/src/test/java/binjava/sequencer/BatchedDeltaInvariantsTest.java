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
     * has segment order disagreeing with offset order -- the writer-side permutation a
     * batch can carry.
     *
     * <p>⚠️ NO OFFSET IS HANDED OUT TWICE HERE, and an earlier draft of this
     * comment said one was. The two runs are DISJOINT -- 3 records at 4 covers
     * 4..6, 4 records at 0 covers 0..3, together 0..6 exactly once. What is
     * wrong is only their ORDER.
     *
     * <p>⚠️ IT IS NAMED `order`, NOT I4, AND THAT IS M4.13f's CONCLUSION.
     * architecture.md defines I4 as "Uncommitted records may be reordered or
     * dropped; committed ones may not", and every record in a delta is
     * UNCOMMITTED until that delta lands -- so an intra-delta permutation is the
     * clause's PERMITTED half. It is still malformed and still worth reporting;
     * it joins `chain`, `gap` and `link` as a defect no invariant covers by
     * name.
     *
     * <p>⚠️ WHAT IT REPLACED. This reported `gap` PLUS `I2`
     * until then -- I2 meaning "an offset was REASSIGNED", which did not happen,
     * and a gap meaning records were skipped, which they were not. Both came
     * from folding the runs in SEGMENT order against a high-water mark, which
     * cannot tell a permutation from a reassignment. The fold now takes a
     * delta's runs in OFFSET order for that arithmetic and reports the
     * segment-order disagreement separately, so this shape gets one violation
     * with the right name.
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
                .as("ONE violation, named for what actually happened: the runs are disjoint and "
                        + "cover 0..6 exactly once, so nothing was reassigned and nothing was "
                        + "skipped -- only the ORDER is wrong (M4.13f)")
                .containsExactly("order");
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

    /**
     * ⚠️ A GENUINE REASSIGNMENT MUST STILL BE I2, or M4.13f would have traded a
     * wrong name for a missing check. Here the two runs OVERLAP -- 3 records at
     * 0 covers 0..2, 3 records at 2 covers 2..4 -- so offset 2 really is handed
     * to two different records, which is what I2 forbids and what
     * `all_active` replica convergence rests on.
     */
    @Test
    void aBatchWhoseSegmentsOVERLAPIsStillI2NotAReorder() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 0)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 3, 2)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("offset 2 is assigned twice -- a reassignment, and NOT also an `order` defect: "
                        + "the segments ascend, so renaming this `order` would lose the real defect")
                .containsExactly("I2");
    }

    /**
     * ⚠️ AND A REAL HOLE MUST STILL BE A GAP. The runs are in segment order and
     * ascending, so nothing is permuted; they simply skip offsets 3 and 4, which
     * is the hole NFR-11 and every reader care about for reasons that are not
     * I2's.
     */
    @Test
    void aBatchThatSKIPSOffsetsIsStillAGapNotAReorder() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 0)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 2, 5)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("ascending but discontiguous is a gap, and must not be renamed `order`")
                .containsExactly("gap");
    }

    /**
     * ⚠️ TWO STREAMS IN ONE BATCH ARE JUDGED INDEPENDENTLY. RA is permuted and
     * RB is well-ordered, so exactly one `order` violation must be reported and
     * it must name RA -- a check that collapsed the streams together would report one
     * violation for the wrong reason, or two.
     */
    @Test
    void ONEStreamPermutedInABatchDoesNotIndictTheOTHER() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A", List.of(
                        new binjava.format.RunCommit(RA, 3, 4),
                        new binjava.format.RunCommit(RB, 2, 0)), null),
                new binjava.format.SegmentCommit("seg/B", List.of(
                        new binjava.format.RunCommit(RA, 4, 0),
                        new binjava.format.RunCommit(RB, 5, 2)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("only RA is out of order")
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.invariant()).isEqualTo("order");
                    assertThat(v.detail()).contains(RA.toString()).doesNotContain(RB.toString());
                });
    }

    /**
     * ⚠️ TWO SEGMENTS STARTING A STREAM AT THE SAME OFFSET IS A REASSIGNMENT,
     * NOT A REORDER, and the boundary is exactly where the reorder test says
     * {@code <} rather than {@code <=}. Equal starts mean both segments claim
     * offset 0 for different records, which is what I2 forbids; calling it
     * `order` as well would report one defect under two names and let a reader believe
     * the records are all present in the wrong order when one set is lost.
     */
    @Test
    void TWOSegmentsStartingAtTheSAMEOffsetIsI2Alone() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 0)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 2, 0)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("equal starts are a reassignment; naming it `order` too would double-count it")
                .containsExactly("I2");
    }

    /**
     * ⚠️ ONE STREAM OUT OF ORDER IS ONE VIOLATION, however many pairs disagree.
     * Three segments descending give TWO out-of-order adjacent pairs, and a
     * check without its {@code break} reports the same defect twice -- which
     * makes a violation COUNT useless for deciding how many things are wrong,
     * and is the ambiguity `checkReader` discards `inheritedOffsets`' violations
     * to avoid.
     */
    @Test
    void aStreamOutOfOrderTWICEIsStillONEViolation() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 2, 6)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 3, 3)), null),
                new binjava.format.SegmentCommit("seg/C",
                        List.of(new binjava.format.RunCommit(RA, 3, 0)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("0..7 is covered exactly once, in reverse segment order -- one reorder")
                .containsExactly("order");
    }

    /**
     * ⚠️ THE COMPARISON IS BETWEEN ADJACENT SEGMENTS, not against the first one,
     * and only a stream whose disorder appears LATE can tell: the second segment
     * ascends from the first, so a check anchored on the first sees a clean
     * batch. The records still arrive in the wrong sequence, and they tile their
     * range exactly once, so no `gap` or `I2` fires either -- silent.
     *
     * <p>⚠️ THE OFFSETS ARE NOT RESTATED HERE, and that is deliberate. Two
     * drafts of this javadoc carried them and both went stale the moment the
     * fixture moved -- twice measured by review, the second time in the same
     * commit whose argued round-cap entry claimed that defect class was
     * remedied. The literals live three lines below in the fixture and again in
     * the assertions; a third copy in prose is a drift surface and nothing else.
     */
    @Test
    void aStreamOutOfOrderOnlyInALATERPairIsStillCaught() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        // ⚠️ A CLEAN DELTA FIRST, so the permuted one sits at sequence 2 rather
        // than 1. MEASURED: with it at sequence 1, replacing `e.sequence()` in
        // the message with the constant `1L` left the whole suite green -- the
        // assertion could not tell the field from a literal that happened to
        // match. Any sequence but 1 makes that mutation observable.
        InvariantFixtures.put(store, log, 1, InvariantFixtures.delta(1, 3, 0));
        InvariantFixtures.put(store, log, 2, new binjava.format.CommitDelta(2, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 3)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 2, 8)), null),
                new binjava.format.SegmentCommit("seg/C",
                        List.of(new binjava.format.RunCommit(RA, 2, 6)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("3..9 covered exactly once, but segment C belongs before B")
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.invariant()).isEqualTo("order");
                    // ⚠️ THE MESSAGE IS ASSERTED, NOT JUST THE NAME. On a failing
                    // sweep this string is how a pinned seed gets debugged, and
                    // three mutations of it were measured surviving: the
                    // sequence replaced by a constant, the two offsets
                    // transposed, and the pre-M4.13f wording restored -- which
                    // names a "segment" index that need not exist, the very
                    // defect the comment beside it says was fixed.
                    assertThat(v.detail())
                            .contains("at sequence 2")
                            .contains("a run starting at 6")
                            .contains("follows one starting at 8")
                            .doesNotContain("segment");
                });
    }

    /**
     * ⚠️ A PERMUTATION THAT ALSO OVERLAPS MUST REPORT BOTH, and this is the
     * fixture the whole row exists for: keeping the names separate is worth
     * nothing if one swallows the other. Segment A covers 4..6 and segment B
     * covers 0..5, so the order is wrong AND offsets 4..5 are handed to two
     * different records.
     *
     * <p>⚠️ MEASURED SURVIVING WITHOUT IT: skipping the offset-order fold for
     * any stream that already reported `order` leaves the suite green, reports
     * `[order]` alone, and -- worse than the lost name -- never advances
     * {@code nextOffset}, so every later delta folds against a stale mark.
     */
    @Test
    void aPermutationThatALSOOverlapsReportsBOTHNames() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, new binjava.format.CommitDelta(1, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 3, 4)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 6, 0)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Violation::invariant)
                .as("out of order AND overlapping -- neither name may swallow the other")
                .containsExactlyInAnyOrder("order", "I2");
    }

    /**
     * ⚠️ THE MIRROR OF THE LATE-ONLY CASE, and review measured the hole it
     * leaves: narrowing the comparison to the LAST adjacent pair alone left the
     * whole suite green. Here the disorder is in the FIRST pair, so a check
     * reading only the last pair sees an ascending step and reports nothing,
     * while the records tile their range exactly once so no `gap` or `I2` fires
     * either. Silent, like its twin. Offsets are in the fixture and the
     * assertion, not restated here -- see the sibling above for why.
     */
    @Test
    void aStreamOutOfOrderOnlyInANEARLYPairIsStillCaught() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        InvariantFixtures.put(store, log, 0, new binjava.format.Continue(0, 0, 0).encode());
        InvariantFixtures.put(store, log, 1, InvariantFixtures.delta(1, 3, 0));
        InvariantFixtures.put(store, log, 2, new binjava.format.CommitDelta(2, List.of(
                new binjava.format.SegmentCommit("seg/A",
                        List.of(new binjava.format.RunCommit(RA, 2, 5)), null),
                new binjava.format.SegmentCommit("seg/B",
                        List.of(new binjava.format.RunCommit(RA, 2, 3)), null),
                new binjava.format.SegmentCommit("seg/C",
                        List.of(new binjava.format.RunCommit(RA, 2, 7)), null))).encode());

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("3..8 covered exactly once, but segment B belongs before A")
                .extracting(Violation::invariant)
                .containsExactly("order");
    }
}
