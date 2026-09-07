// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.RA;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static binjava.sequencer.DedupFixtures.firstOffsetOf;
import static binjava.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.CommitDelta;
import java.io.IOException;
import binjava.format.RunCommit;
import binjava.format.SegmentCommit;
import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A commit is applied AT MOST ONCE, keyed on {@code (podId, incarnationId,
 * flushSeq)} per ADR-0036 (M4.10d).
 *
 * <p>⚠️ THE CASE THIS EXISTS FOR IS THE AMBIGUOUS PUT. A conditional PUT whose
 * response was lost has still landed, so the retry that follows is
 * indistinguishable, to the caller, from a first attempt. Until this row the
 * retry assigned a SECOND set of offsets to the same records.
 *
 * <p>⚠️ THE ASSERTION IS ON THE OFFSETS AND ON THE CHAIN, never on the offsets
 * alone. {@code ChainReplay} folds with {@code merge(key, lastOffset + 1,
 * Math::max)}, so re-appending a run at its ORIGINAL absolute offsets leaves
 * every stream's {@code nextOffset} unchanged -- an offsets-only assertion is
 * structurally blind to a duplicate delta.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SequencerDedupTest {


    @Test
    void aReplayedTripleReturnsTheORIGINALOffsetsAndAppendsNoSecondDelta() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            CommitDelta first = sequencer.commitAll(List.of(flush));
            long assigned = firstOffsetOf(first, "seg/0");

            // ⚠️ THE SAME TRIPLE, byte for byte: this is the retry after an
            // ambiguous IOException, not a new flush.
            CommitDelta replay = sequencer.commitAll(List.of(flush));

            assertThat(firstOffsetOf(replay, "seg/0"))
                    .as("the retry is answered with the offsets that ALREADY apply")
                    .isEqualTo(assigned);
            assertThat(replay.sequence())
                    .as("and from the delta that already holds them")
                    .isEqualTo(first.sequence());
            // ⚠️ THE CHAIN, not just the returned delta. Answering from the
            // window AND appending anyway satisfies every assertion above.
            assertThat(deltasCarrying(store, "seg/0"))
                    .as("ONE delta carries these records -- the replay APPENDED NOTHING")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aRestartedPodReissuingFlushSeqZeroIsACCEPTED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            sequencer.commitAll(List.of(new CommitRequest("poda", "i1", 0, "seg/0", counts(3))));

            // ⚠️ THE DISCRIMINATOR IS A POD RESTART, NOT A TAKEOVER. The same
            // podId reissues flushSeq 0 under a NEW incarnation for a NEW
            // segment; dedup on the bare (podId, flushSeq) pair refuses this,
            // which is the defect ADR-0036 exists for.
            CommitDelta second = sequencer.commitAll(
                    List.of(new CommitRequest("poda", "i2", 0, "seg/1", counts(4))));

            assertThat(firstOffsetOf(second, "seg/1"))
                    .as("a restart is not a replay -- these are NEW records")
                    .isEqualTo(3L);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ THE POINTED DELTA IS NOT ITSELF THE ANSWER. The window records ONE
     * pointer per incarnation -- the newest -- so replaying an OLDER
     * {@code flushSeq} lands on a delta that does not carry it. Answering with
     * whatever that delta does hold would hand the producer another flush's
     * offsets, and it would acknowledge records committed nowhere.
     */
    @Test
    void aReplayThePointedDeltaDoesNotCarryIsREFUSED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest zero = new CommitRequest("poda", "i1", 0, "seg/0", counts(3));
            sequencer.commitAll(List.of(zero));
            sequencer.commitAll(List.of(new CommitRequest("poda", "i1", 1, "seg/1", counts(4))));

            // flushSeq 0 <= lastApplied 1, so this IS a replay -- but the
            // pointer names the delta that applied 1, which carries seg/1 only.
            assertThatThrownBy(() -> sequencer.commitAll(List.of(zero)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("does not carry");
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ THE RIGHT SEGMENT OUT OF A BATCHED DELTA. `BatchingSequencer` puts many
     * pods' flushes in ONE delta, so the answering segment must be selected by
     * the exact triple; taking the delta's first segment returns another pod's
     * offsets to this one.
     */
    @Test
    void aReplayIsAnsweredWithITSOwnSegmentOutOfABatchedDelta() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest a = new CommitRequest("poda", "i1", 0, "seg/a", counts(3));
            CommitRequest b = new CommitRequest("podb", "i9", 0, "seg/b", counts(4));
            CommitDelta batched = sequencer.commitAll(List.of(a, b));
            long offsetB = firstOffsetOf(batched, "seg/b");

            CommitDelta replay = sequencer.commitAll(List.of(b));

            assertThat(replay.segments())
                    .as("only podb's flush is answered, not the whole delta")
                    .hasSize(1);
            assertThat(firstOffsetOf(replay, "seg/b"))
                    .as("podb's OWN offsets, not poda's")
                    .isEqualTo(offsetB);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ A BATCH MIXING A REPLAY WITH A FRESH FLUSH MUST NOT FAIL EITHER.
     * `BatchingSequencer.commitBatch` hands every caller in a window the SAME
     * delta and fails them all together, so refusing the batch over one pod's
     * retry loses a co-batched flush that has not been committed at all.
     */
    @Test
    void aBatchMixingAReplayWithAFreshFlushCommitsTheFreshOneAndAnswersTheReplay()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest a = new CommitRequest("poda", "i1", 0, "seg/a", counts(3));
            CommitDelta first = sequencer.commitAll(List.of(a));
            long offsetA = firstOffsetOf(first, "seg/a");

            CommitRequest fresh = new CommitRequest("podb", "i9", 0, "seg/b", counts(4));
            CommitDelta mixed = sequencer.commitAll(List.of(a, fresh));

            assertThat(firstOffsetOf(mixed, "seg/a"))
                    .as("the replay keeps its original offsets")
                    .isEqualTo(offsetA);
            assertThat(firstOffsetOf(mixed, "seg/b"))
                    .as("and the co-batched fresh flush is COMMITTED, not lost")
                    .isEqualTo(3L);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ OUT OF ORDER WITHIN ONE BATCH. `BatchingSequencer` aggregates whatever
     * arrived in the window, so one incarnation's flushes can reach `commitAll`
     * in any order. Recording the LAST one seen rather than the HIGHEST would
     * lower the watermark, and every flush between the two would then be
     * accepted a second time.
     */
    @Test
    void aBatchAPPLYINGTwoFlushesOutOfOrderKeepsTheHIGHESTWatermark() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            // 4 BEFORE 3, both fresh, both from one incarnation.
            sequencer.commitAll(List.of(
                    new CommitRequest("podx", "i1", 4, "seg/4", counts(1)),
                    new CommitRequest("podx", "i1", 3, "seg/3", counts(1))));

            // If the watermark went to 3, flushSeq 4 is no longer a replay and
            // commits its records a second time.
            CommitDelta replay = sequencer.commitAll(
                    List.of(new CommitRequest("podx", "i1", 4, "seg/4", counts(1))));

            assertThat(replay.segments()).as("answered, not re-applied").hasSize(1);
            assertThat(firstOffsetOf(replay, "seg/4"))
                    .as("the ORIGINAL offsets, so the watermark stayed at 4")
                    .isEqualTo(0L);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ EACH HALF OF THE KEY, SEPARATELY. Review measured that dropping
     * `podId.equals` alone survived, and dropping `incarnationId.equals` alone
     * survived -- each masked the other, because every fixture varied both at
     * once. A delta carrying ANOTHER pod's flush at the same flushSeq must not
     * answer this one, and neither must a DEAD incarnation's.
     */
    @Test
    void aSegmentMatchingOnlyPARTOfTheTripleAnswersNOTHING() {
        CommitRequest want = new CommitRequest("podx", "i1", 4, "seg/x", counts(1));
        RunCommit run = new RunCommit(RA, 1, 0);

        SegmentCommit otherPod = new SegmentCommit("seg/x", List.of(run),
                new SegmentCommit.Attribution("podOTHER", "i1", 4));
        SegmentCommit otherIncarnation = new SegmentCommit("seg/x", List.of(run),
                new SegmentCommit.Attribution("podx", "iOTHER", 4));
        SegmentCommit otherFlushSeq = new SegmentCommit("seg/x", List.of(run),
                new SegmentCommit.Attribution("podx", "i1", 9));
        SegmentCommit exact = new SegmentCommit("seg/x", List.of(run),
                new SegmentCommit.Attribution("podx", "i1", 4));

        assertThat(IdempotencyWindow.answers(otherPod, want))
                .as("same incarnation and flushSeq, DIFFERENT pod").isFalse();
        assertThat(IdempotencyWindow.answers(otherIncarnation, want))
                .as("same pod and flushSeq, DIFFERENT incarnation").isFalse();
        assertThat(IdempotencyWindow.answers(otherFlushSeq, want))
                .as("same pod and incarnation, DIFFERENT flushSeq").isFalse();
        SegmentCommit otherKey = new SegmentCommit("seg/OTHER", List.of(run),
                new SegmentCommit.Attribution("podx", "i1", 4));
        assertThat(IdempotencyWindow.answers(otherKey, want))
                .as("the WHOLE triple matches, but it is a different SEGMENT -- one flush may "
                        + "submit several under one flushSeq")
                .isFalse();
        assertThat(IdempotencyWindow.answers(exact, want)).as("the exact triple and key").isTrue();

        // ⚠️ AN UNATTRIBUTED SEGMENT ANSWERS NOTHING. Deltas written before
        // M4.10c carry no attribution at all, and returning true for one would
        // answer a retry with a segment nobody can show belongs to it.
        assertThat(IdempotencyWindow.answers(
                new SegmentCommit("seg/x", List.of(run)), want))
                .as("no attribution at all").isFalse();
    }

    /**
     * ⚠️ THE COST CLAIM, ASSERTED RATHER THAN WRITTEN DOWN. `answer`'s javadoc
     * states AT MOST ONE GET PER DISTINCT {@code (epoch, sequence)}, which is
     * the bound ADR-0036 gives; review measured that deleting the read cache
     * left every test green, so the claim was prose. A batch of retries from
     * one pod points at ONE delta and must read it ONCE.
     */
    @Test
    void aBatchOfReplaysPointingAtONEDeltaReadsItONCE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest a = new CommitRequest("podx", "i1", 0, "seg/a", counts(1));
            CommitRequest b = new CommitRequest("podx", "i1", 1, "seg/b", counts(1));
            CommitRequest c = new CommitRequest("podx", "i1", 2, "seg/c", counts(1));
            sequencer.commitAll(List.of(a, b, c));

            long before = store.counts().gets();
            sequencer.commitAll(List.of(a, b, c));
            long spent = store.counts().gets() - before;

            assertThat(spent)
                    .as("three replays, one delta between them: ONE get, not three")
                    .isEqualTo(1L);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ THE HIGHEST SEQUENCE, NOT THE LOWEST. An all-replay batch returns a
     * delta whose sequence must name the newest object involved; review
     * measured `Math.max` mutated to `Math.min` surviving.
     */
    @Test
    void anAllReplayBatchReportsTheHIGHESTSequenceInvolved() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            // ⚠️ TWO PODS, because the window keeps ONE pointer per
            // incarnation -- replaying an older flushSeq of the SAME pod lands
            // on a delta that does not carry it and is refused instead.
            CommitRequest first = new CommitRequest("podx", "i1", 0, "seg/a", counts(1));
            CommitRequest second = new CommitRequest("pody", "i2", 0, "seg/b", counts(1));
            long lower = sequencer.commitAll(List.of(first)).sequence();
            long higher = sequencer.commitAll(List.of(second)).sequence();
            assertThat(higher).as("the fixture needs two DIFFERENT slots").isGreaterThan(lower);

            assertThat(sequencer.commitAll(List.of(first, second)).sequence())
                    .as("the newest slot the replays point at")
                    .isEqualTo(higher);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ ONLY A REFUSABLE REPLAY DISCRIMINATES THE ORDER. The sibling test
     * batches an ANSWERABLE replay, where committing before or after answering
     * is unobservable -- review measured the two statements swapped, which is
     * the exact ordering the production comment says was "offset 7 instead of
     * 12", and the whole suite stayed green.
     *
     * <p>`BatchingSequencer` fails a whole window together, so the co-batched
     * pod's flush is a producer-visible loss if the refusal comes first.
     */
    @Test
    void aBatchWithANUNANSWERABLEReplayStillCommitsTheFreshFlush() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest zero = new CommitRequest("podx", "i1", 0, "seg/0", counts(3));
            sequencer.commitAll(List.of(zero));
            sequencer.commitAll(List.of(new CommitRequest("podx", "i1", 1, "seg/1", counts(4))));
            // The pointer now names the delta that applied 1, so replaying 0 is
            // a replay the chain cannot answer.
            CommitRequest fresh = new CommitRequest("podb", "i9", 0, "seg/b", counts(5));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(zero, fresh)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("does not carry");

            assertThat(deltasCarrying(store, "seg/b"))
                    .as("the co-batched fresh flush was made DURABLE before the refusal")
                    .isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ ONE FLUSH, TWO SEGMENTS, ONE TRIPLE. `answers` matches the segment key
     * as well as the triple because the triple alone is not unique: review
     * measured both replays resolving to the SAME `SegmentCommit`, so the
     * returned delta named one segment twice and `CommitDelta` refused it --
     * turning a landed flush into a producer-visible failure.
     */
    @Test
    void aFlushSubmittingTWOSegmentsUnderONETripleIsAnsweredPerSEGMENT() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            CommitRequest a = new CommitRequest("podx", "i1", 5, "seg/a", counts(3));
            CommitRequest b = new CommitRequest("podx", "i1", 5, "seg/b", counts(4));
            CommitDelta first = sequencer.commitAll(List.of(a, b));
            long offsetA = firstOffsetOf(first, "seg/a");
            long offsetB = firstOffsetOf(first, "seg/b");

            CommitDelta retry = sequencer.commitAll(List.of(a, b));

            assertThat(firstOffsetOf(retry, "seg/a")).as("seg/a keeps ITS offsets").isEqualTo(offsetA);
            assertThat(firstOffsetOf(retry, "seg/b")).as("seg/b keeps ITS offsets").isEqualTo(offsetB);
        } finally {
            sequencer.close();
        }
    }

    /**
     * ⚠️ THE KEY SEPARATOR IS LOAD-BEARING. Concatenating podId and
     * incarnationId without one makes {@code ("ab","c")} and {@code ("a","bc")}
     * the same slot, so one pod's watermark refuses another pod's flush. Review
     * measured the separator droppable with the suite green.
     */
    @Test
    void twoIncarnationsWhoseNamesCONCATENATEAlikeDoNotShareASlot() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer =
                LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8).orElseThrow();
        try {
            sequencer.commitAll(List.of(new CommitRequest("ab", "c", 4, "seg/1", counts(3))));

            // Same concatenation, different pod: this is a FRESH flush.
            CommitDelta second = sequencer.commitAll(
                    List.of(new CommitRequest("a", "bc", 4, "seg/2", counts(2))));

            assertThat(firstOffsetOf(second, "seg/2"))
                    .as("committed, not mistaken for the other pod's replay")
                    .isEqualTo(3L);
        } finally {
            sequencer.close();
        }
    }
}
