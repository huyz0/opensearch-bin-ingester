// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a stream's OLDEST RETAINED offset is after a GC pass (M7.10, FR-9).
 *
 * <p>⚠️ `CheckpointWriter` WRITES A LITERAL 0 TODAY, and its own comment says
 * that is truthful only until M7 gives retention a meaning. ⚠️ THIS IS THE
 * MUTATION THAT SURVIVES EVERY OTHER M7 TEST: leave the 0 in place and every
 * other case in this milestone stays green, while the refusal a consumer below
 * the boundary should meet (M7.16) becomes unreachable by construction —
 * there is no boundary to be below.
 *
 * <p>⚠️ AND THE ANSWER IS THE LOWEST RETAINED FIRST OFFSET, NOT THE HIGHEST
 * DELETED END. They differ exactly when a kept segment sits between two deleted
 * ones — which happens whenever one stream in a segment is behind — and the
 * highest-deleted reading then claims data is gone that is still there, so a
 * consumer is refused for records it could have had.
 */
class RetainedOffsetAdvanceTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final RunKey OTHER = new RunKey(INDEX, 1);

    private static SegmentCommit segment(String key, RunKey stream, long firstOffset,
            int count) {
        return new SegmentCommit(key, List.of(new RunCommit(stream, count, firstOffset)),
                new SegmentCommit.Attribution("poda", "i1", 1));
    }

    private static CommitDelta delta(long sequence, SegmentCommit... segments) {
        return new CommitDelta(sequence, List.of(segments));
    }

    @Test
    void aStreamWithNOTHINGDeletedRetainsFromItsFIRSTOffset() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5), segment("b", STREAM, 5, 5))),
                Set.of(), Map.of(STREAM, 10L));
        assertThat(retained).containsEntry(STREAM, 0L);
    }

    @Test
    void aStreamWithITSOldestSegmentDeletedRetainsFromTheNEXTOne() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5), segment("b", STREAM, 5, 5))),
                Set.of("a"), Map.of(STREAM, 10L));
        assertThat(retained)
                .as("the oldest offset still READABLE is the first offset of the oldest "
                        + "segment still THERE")
                .containsEntry(STREAM, 5L);
    }

    @Test
    void aGAPTakesTheLOWESTRetainedRatherThanTheHIGHESTDeleted() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5), segment("b", STREAM, 5, 5),
                        segment("c", STREAM, 10, 5))),
                Set.of("a", "c"), Map.of(STREAM, 15L));
        assertThat(retained)
                .as("⚠️ A KEPT SEGMENT BETWEEN TWO DELETED ONES IS NOT AN EDGE CASE: it "
                        + "happens whenever one stream in a shared segment is behind. "
                        + "Reading the HIGHEST deleted end here answers 15 and claims "
                        + "records 5..9 are gone while they are still in the bucket, so a "
                        + "consumer is refused for data it could have had")
                .containsEntry(STREAM, 5L);
    }

    @Test
    void aStreamWithEVERYSegmentDeletedRetainsFromItsNEXTOffset() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5), segment("b", STREAM, 5, 5))),
                Set.of("a", "b"), Map.of(STREAM, 10L));
        assertThat(retained)
                .as("everything written so far is gone, so the oldest retained offset is "
                        + "the next one to be assigned -- and `oldestRetainedOffset == "
                        + "nextOffset` is exactly what an empty stream means")
                .containsEntry(STREAM, 10L);
    }

    @Test
    void EACHStreamGetsITSOwnBoundary() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, new SegmentCommit("shared",
                        List.of(new RunCommit(STREAM, 5, 0), new RunCommit(OTHER, 5, 100)),
                        new SegmentCommit.Attribution("poda", "i1", 1)),
                        segment("b", STREAM, 5, 5))),
                Set.of("shared"), Map.of(STREAM, 10L, OTHER, 105L));
        assertThat(retained)
                .as("one object carried both streams' records, and deleting it moves each "
                        + "stream's boundary by what THAT stream had in it")
                .containsEntry(STREAM, 5L)
                .containsEntry(OTHER, 105L);
    }

    @Test
    void aStreamNOTInTheChainIsABSENTRatherThanZero() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", OTHER, 0, 5))), Set.of(), Map.of(STREAM, 10L));
        assertThat(retained)
                .as("the chain is NOT empty -- it names another stream -- so the absence "
                        + "is the bookkeeping's answer rather than a loop that never ran")
                .doesNotContainKey(STREAM);
        assertThat(retained)
                .as("a stream no delta names has no boundary to report, and reporting 0 "
                        + "for it would say 'everything is retained' about a stream that "
                        + "may have been emptied by an earlier pass")
                .containsOnlyKeys(OTHER);
    }

    @Test
    void aDeletedSegmentNamingNOStreamOfInterestChangesNothing() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5)), delta(1, segment("z", OTHER, 0, 5))),
                Set.of("z"), Map.of(STREAM, 5L, OTHER, 5L));
        assertThat(retained).containsEntry(STREAM, 0L).containsEntry(OTHER, 5L);
    }

    @Test
    void aStreamWithNOKnownNextOffsetIsABSENTRatherThanGuessedAt() {
        Map<RunKey, Long> retained = RetainedOffsets.after(
                List.of(delta(0, segment("a", STREAM, 0, 5))), Set.of("a"), Map.of());
        assertThat(retained)
                .as("with every segment gone the boundary IS nextOffset, and a caller that "
                        + "did not supply one cannot be given a made-up number -- a low "
                        + "guess says deleted records are readable, a high one refuses a "
                        + "consumer that is fine")
                .isEmpty();
    }
}
