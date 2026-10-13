// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A checkpoint's split flushes, read back (ADR-0086): a retry of a segment
 * committed across two deltas, behind the checkpoint a graceful handover
 * wrote, is answered from both -- by the window's answer, its per-delta
 * answer, and the leader's commit answer -- where its one pointer refused it.
 */
class IdempotencyWindowSplitSeedTest {

    private static final RunKey RB = new RunKey(A, 1);
    private static final long EPOCH = 0; // behind every term these tests start
    private static final SegmentCommit.Attribution FLUSH5 =
            new SegmentCommit.Attribution("pod", "i1", 5);
    private static final CommitRequest RETRY =
            new CommitRequest("pod", "i1", 5, "seg/a", Map.of(RA, 2, RB, 3));

    private static MemoryBinStore twoDeltas() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        int seq = 0;
        for (RunCommit run : List.of(new RunCommit(RA, 2, 10), new RunCommit(RB, 3, 20))) {
            store.put(new LogKeys(PREFIX, EPOCH).keyFor(seq), Body.ofBytes(new CommitDelta(seq,
                    List.of(new SegmentCommit("seg/a", List.of(run), FLUSH5))).encode()));
            seq++;
        }
        return store;
    }

    /** The slot a checkpoint carries: the pointer at the first delta, and the split. */
    private static Map<String, Checkpoint.PodState> seeded() {
        return Map.of("pod\0i1", new Checkpoint.PodState("i1", 5, EPOCH, 0, List.of(
                new Checkpoint.SplitFlush(5, "seg/a", List.of(new Checkpoint.DeltaAt(EPOCH, 0),
                        new Checkpoint.DeltaAt(EPOCH, 1))))));
    }

    @Test
    void aSEEDEDSplitIsAnsweredFromBothDeltas() throws Exception {
        IdempotencyWindow window = new IdempotencyWindow(twoDeltas(), PREFIX);
        window.seed(seeded());

        assertThat(window.answer(List.of(RETRY))).singleElement().satisfies(s -> assertThat(
                s.runs()).containsExactly(new RunCommit(RA, 2, 10), new RunCommit(RB, 3, 20)));
    }

    @Test
    void aSPLITWhoseDeltaIsMissingIsRefusedAndKeptForTheRetry() throws Exception {
        MemoryBinStore store = twoDeltas();
        String second = new LogKeys(PREFIX, EPOCH).keyFor(1);
        byte[] held;
        try (var in = store.get(second)) {
            held = in.readAllBytes();
        }
        store.delete(List.of(second));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.seed(seeded());

        assertThatThrownBy(() -> window.answer(List.of(RETRY)))
                .isInstanceOf(java.io.IOException.class);
        store.put(second, Body.ofBytes(held));
        assertThat(window.answer(List.of(RETRY))).as("the split kept for the retry")
                .singleElement().satisfies(s -> assertThat(s.runs()).hasSize(2));
    }

    @Test
    void aSEEDEDSplitIsAnsweredDeltaByDelta() throws Exception {
        IdempotencyWindow window = new IdempotencyWindow(twoDeltas(), PREFIX);
        window.seed(seeded());

        assertThat(window.answerByDelta(RETRY)).extracting(CommitDelta::sequence)
                .containsExactly(0L, 1L);
    }

    @Test
    void theLEADERsCommitAnswerToASeededSplitListsBothDeltas() throws Exception {
        MemoryBinStore store = twoDeltas();
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            sequencer.window.seed(seeded());

            CommitAnswer answer = sequencer.commitAnswering(RETRY);

            assertThat(answer.deltas()).extracting(CommitDelta::sequence)
                    .containsExactly(0L, 1L);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aSUCCESSORReplayingPastACheckpointKeepsItsSplitThoughALaterFlushLanded()
            throws Exception {
        // ⚠️ the replay merges the checkpoint's slot with a later delta's, and
        // the higher flush's slot must not drop the lower one's split.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer crashed = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        long first = crashed.commitAll(List.of(new CommitRequest("pod", "i1", 5, "seg/a",
                Map.of(RA, 2)))).sequence();
        long second = crashed.applyFresh(List.of(new CommitRequest("pod", "i1", 5, "seg/a",
                Map.of(RB, 3)))).sequence();
        long epoch = crashed.epoch();
        LogKeys keys = new LogKeys(PREFIX, epoch);
        byte[] checkpoint = new Checkpoint(second, Map.of(
                RA, new Checkpoint.StreamOffsets(2, 0), RB, new Checkpoint.StreamOffsets(3, 0)),
                Map.of("pod", new Checkpoint.PodState("i1", 5, epoch, first, List.of(
                        new Checkpoint.SplitFlush(5, "seg/a", List.of(
                                new Checkpoint.DeltaAt(epoch, first),
                                new Checkpoint.DeltaAt(epoch, second)))))))
                .encode();
        store.put(keys.checkpointKeyFor(second), Body.ofBytes(checkpoint));
        store.put(keys.latestCheckpointKey(), Body.ofBytes(checkpoint));
        crashed.commitAll(List.of(new CommitRequest("pod", "i1", 6, "seg/b", Map.of(RA, 1))));

        java.time.Clock later = java.time.Clock.fixed(
                java.time.Instant.ofEpochMilli(1_000_000L + 60_000L), java.time.ZoneOffset.UTC);
        LocalSequencer successor = LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                new LeaseConfig(PREFIX, "podc", "", java.time.Duration.ofSeconds(10),
                        java.time.Duration.ofSeconds(3)), later), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            CommitDelta answered = successor.commitAll(List.of(RETRY));

            assertThat(answered.segments()).singleElement().satisfies(s -> assertThat(s.runs())
                    .extracting(RunCommit::key).containsExactlyInAnyOrder(RA, RB));
        } finally {
            successor.close();
        }
    }
}
