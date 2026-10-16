// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A split segment's runs committed in two chains (M13.34ae; ADR-0082 §7): its
 * paused run by the successor of the leader that committed the rest. Each
 * chain numbers its deltas from its own start, so both can be sequence 1 --
 * and the answer, which lists deltas by sequence alone, refused to be built.
 * It is answered with one delta holding every run, at the newest's sequence.
 */
class IdempotencyWindowAcrossChainsTest {

    private static final String PREFIX = "p";
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);
    private static final SegmentCommit.Attribution FLUSH5 =
            new SegmentCommit.Attribution("pod", "i1", 5);

    private static SegmentCommit part(RunCommit run) {
        return new SegmentCommit("seg/a", List.of(run), FLUSH5);
    }

    @Test
    void aSEGMENTCommittedInTwoChainsIsAnsweredInOneDeltaAtTheNewestsSequence()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(new LogKeys(PREFIX, 1).keyFor(1), Body.ofBytes(
                new CommitDelta(1, List.of(part(new RunCommit(A, 2, 10)))).encode()));
        store.put(new LogKeys(PREFIX, 2).keyFor(1), Body.ofBytes(
                new CommitDelta(1, List.of(part(new RunCommit(B, 3, 20)))).encode()));
        IdempotencyWindow window = new IdempotencyWindow(store, PREFIX);
        window.applied(part(new RunCommit(A, 2, 10)), 1, 1);
        window.applied(part(new RunCommit(B, 3, 20)), 2, 1);

        List<CommitDelta> deltas = window.answerByDelta(
                new CommitRequest("pod", "i1", 5, "seg/a", Map.of(A, 2, B, 3)));

        assertThat(deltas).singleElement().satisfies(d -> {
            assertThat(d.sequence()).isEqualTo(1);
            assertThat(d.segments()).singleElement().satisfies(s -> assertThat(s.runs())
                    .containsExactly(new RunCommit(A, 2, 10), new RunCommit(B, 3, 20)));
        });
        assertThat(new CommitAnswer(deltas, List.of()).deltas())
                .as("an answer the format takes").hasSize(1);
    }
}
