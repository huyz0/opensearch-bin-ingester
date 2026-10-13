// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Per-segment idempotency (M13.34b; ADR-0081 §8): a switch's paused run is
 * committed in a later delta than the rest of its segment, so a retry is
 * answered from every delta carrying one of its runs -- and refused while a
 * run it asks for is not committed yet, never answered from the first delta
 * alone, which would ack records never committed.
 */
class IdempotencyWindowSegmentsTest {

    private static final String PREFIX = "p";
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);
    private static final SegmentCommit.Attribution FLUSH5 =
            new SegmentCommit.Attribution("pod", "i1", 5);

    private static SegmentCommit part(RunCommit run) {
        return new SegmentCommit("seg/a", List.of(run), FLUSH5);
    }

    private static void delta(MemoryBinStore store, long sequence, SegmentCommit segment)
            throws Exception {
        store.put(new LogKeys(PREFIX, 1).keyFor(sequence),
                Body.ofBytes(new CommitDelta(sequence, List.of(segment)).encode()));
    }

    private static final CommitRequest RETRY =
            new CommitRequest("pod", "i1", 5, "seg/a", Map.of(A, 2, B, 3));

    @Test
    void aSEGMENTsRunsCommittedInTwoDeltasAreAnsweredFromBoth() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        delta(store, 0, part(new RunCommit(A, 2, 10)));
        delta(store, 1, part(new RunCommit(B, 3, 20)));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.applied(part(new RunCommit(A, 2, 10)), 1, 0);
        window.applied(part(new RunCommit(B, 3, 20)), 1, 1);

        List<SegmentCommit> answered = window.answer(List.of(RETRY));

        assertThat(answered).singleElement().satisfies(s -> {
            assertThat(s.segmentKey()).isEqualTo("seg/a");
            assertThat(s.runs()).containsExactly(new RunCommit(A, 2, 10),
                    new RunCommit(B, 3, 20));
            assertThat(s.attribution()).isEqualTo(FLUSH5);
        });
        assertThat(window.sequenceOf(List.of(RETRY))).as("the later delta").isEqualTo(1);
    }

    @Test
    void aRETRYBeforeItsPausedRunCommitsIsRefusedAsIncomplete() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        delta(store, 0, part(new RunCommit(A, 2, 10)));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.applied(part(new RunCommit(A, 2, 10)), 1, 0);

        assertThatThrownBy(() -> window.answer(List.of(RETRY)))
                .isInstanceOfSatisfying(IdempotencyWindow.IncompleteReplayException.class,
                        e -> assertThat(e.missing()).containsExactly(B));
    }

    @Test
    void anOLDERFlushWithinTheRetainedOnesIsAnsweredFromItsOwnDelta() throws Exception {
        // ⚠️ where the one pointer, moved to flush 6's delta, refused it.
        MemoryBinStore store = new MemoryBinStore();
        delta(store, 0, part(new RunCommit(A, 2, 10)));
        SegmentCommit six = new SegmentCommit("seg/b", List.of(new RunCommit(A, 1, 12)),
                new SegmentCommit.Attribution("pod", "i1", 6));
        delta(store, 1, six);
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.applied(new CommitRequest("pod", "i1", 5, "seg/a", Map.of(A, 2)), 1, 0);
        window.applied(new CommitRequest("pod", "i1", 6, "seg/b", Map.of(A, 1)), 1, 1);

        assertThat(window.answer(List.of(new CommitRequest("pod", "i1", 5, "seg/a",
                Map.of(A, 2))))).singleElement().extracting(SegmentCommit::runs)
                .isEqualTo(List.of(new RunCommit(A, 2, 10)));
    }

    @Test
    void onlyTheLATESTFlushesOfAnIncarnationKeepTheirRuns() {
        IdempotencyWindow window = new IdempotencyWindow(new MemoryBinStore(), PREFIX);
        int last = 3 * IdempotencyWindow.RETAINED_FLUSHES;
        for (int seq = 0; seq <= last; seq++) {
            window.applied(new CommitRequest("pod", "i1", seq, "seg/" + seq, Map.of(A, 1)), 1,
                    seq);
        }

        assertThat(window.runsOf(new CommitRequest("pod", "i1", last, "seg/" + last,
                Map.of(A, 1)))).isPresent();
        int oldest = last - IdempotencyWindow.RETAINED_FLUSHES + 1;
        assertThat(window.runsOf(new CommitRequest("pod", "i1", oldest, "seg/" + oldest,
                Map.of(A, 1)))).as("the oldest retained").isPresent();
        assertThat(window.runsOf(new CommitRequest("pod", "i1", oldest - 1,
                "seg/" + (oldest - 1), Map.of(A, 1)))).as("one past them").isEmpty();
    }
}
