// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A split segment's runs across a takeover (M13.34v; ADR-0081 §8): the
 * successor seeds the per-segment window from the deltas its replay read, so
 * a retry is answered from both; and a retry the window knows only by its
 * one pointer is refused when the pointed delta lacks a run it asks for --
 * never answered from that delta alone, which would ack records at offsets
 * nobody gave them.
 */
class IdempotencyWindowSeedRunsTest {

    private static final RunKey RB = new RunKey(A, 1);
    private static final SegmentCommit.Attribution FLUSH5 =
            new SegmentCommit.Attribution("pod", "i1", 5);
    private static final CommitRequest RETRY =
            new CommitRequest("pod", "i1", 5, "seg/a", Map.of(RA, 2, RB, 3));

    private static SegmentCommit part(RunCommit run) {
        return new SegmentCommit("seg/a", List.of(run), FLUSH5);
    }

    private static CommitDelta delta(MemoryBinStore store, long sequence, SegmentCommit segment)
            throws Exception {
        CommitDelta delta = new CommitDelta(sequence, List.of(segment));
        store.put(new LogKeys(PREFIX, 1).keyFor(sequence), Body.ofBytes(delta.encode()));
        return delta;
    }

    @Test
    void aSPLITSegmentsRunsSeededFromReplayedDeltasAreAnsweredFromBoth() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitDelta first = delta(store, 0, part(new RunCommit(RA, 2, 10)));
        CommitDelta second = delta(store, 1, part(new RunCommit(RB, 3, 20)));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);

        window.seedRuns(List.of(new EpochDelta(1, first), new EpochDelta(1, second)));

        assertThat(window.answer(List.of(RETRY))).singleElement().satisfies(s -> assertThat(
                s.runs()).containsExactly(new RunCommit(RA, 2, 10), new RunCommit(RB, 3, 20)));
    }

    @Test
    void aPOINTEROnlyFlushWhoseDeltaLacksARunIsRefusedNotAnsweredInPart() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        delta(store, 0, part(new RunCommit(RA, 2, 10)));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.applied(FLUSH5, 1, 0); // the pointer alone, as a checkpoint seeds it

        assertThatThrownBy(() -> window.answer(List.of(RETRY)))
                .isInstanceOfSatisfying(IdempotencyWindow.IncompleteReplayException.class,
                        e -> assertThat(e.missing()).containsExactly(RB));
    }

    @Test
    void aSUCCESSORAfterACrashAnswersARetryOfASegmentSplitAcrossTwoDeltas() throws Exception {
        // ⚠️ A CRASH, NOT A CLOSE: a close checkpoints, and a replay starting
        // past both deltas reads neither -- M13.34h's, which the checkpoint
        // must carry. Here the predecessor's lease simply runs out.
        MemoryBinStore store = new MemoryBinStore();
        CommitRequest whole = new CommitRequest("podb", "i1", 7, "seg/0", Map.of(RA, 2, RB, 3));
        LocalSequencer crashed = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        crashed.commitAll(List.of(new CommitRequest("podb", "i1", 7, "seg/0", Map.of(RA, 2))));
        crashed.applyFresh(List.of(new CommitRequest("podb", "i1", 7, "seg/0", Map.of(RB, 3))));

        LeaseManager later = new LeaseManager(store, new LeaseConfig(PREFIX, "podc", "",
                Duration.ofSeconds(10), Duration.ofSeconds(3)),
                Clock.fixed(Instant.ofEpochMilli(1_000_000L + 60_000L), ZoneOffset.UTC));
        LocalSequencer successor = LocalSequencer.start(store, PREFIX, later, 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            CommitDelta answered = successor.commitAll(List.of(whole));

            assertThat(answered.segments()).singleElement().satisfies(s -> assertThat(s.runs())
                    .extracting(RunCommit::key).containsExactlyInAnyOrder(RA, RB));
        } finally {
            successor.close();
        }
        assertThat(deltasCarrying(store, "seg/0")).as("answered, never committed again")
                .isEqualTo(2);
    }
}
