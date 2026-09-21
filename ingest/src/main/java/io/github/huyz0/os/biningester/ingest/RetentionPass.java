// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One GC pass, with its boundary reported (M7.10, FR-9).
 *
 * <p>⚠️ IT EXISTS BECAUSE THE TWO HALVES ARE USELESS APART. {@link SegmentGc}
 * knows what it deleted; {@code CheckpointWriter} is what a consumer's refusal
 * is read from; and between them {@link RetainedOffsets} turns one into the
 * other. Review found the halves shipped unconnected, which leaves the
 * checkpoint saying 0 forever — the literal the SPEC calls the mutation that
 * survives every other case in this milestone.
 *
 * <p>⚠️ THE BOUNDARY IS REPORTED FROM WHAT WAS DELETED, NEVER FROM WHAT WAS
 * CONDEMNED. A failed batch leaves its objects readable, and moving the
 * boundary over them refuses a consumer for records that are still there.
 *
 * <p>⚠️ IT IS HANDED AN ALREADY-BUILT {@link SegmentGc}, so the store that GC
 * deletes through is chosen by whoever built it. Composed with
 * {@link LeasedGc}, that must be the FENCED store {@code runIfLeader} hands the
 * pass — a {@code RetentionPass} built outside the lambda and merely CALLED
 * inside it compiles, passes, and deletes unfenced.
 *
 * <p>⚠️ AND IT REPORTS EVEN WHEN IT DELETED NOTHING, because "every segment of
 * this stream was already gone" is a boundary too — it is how a stream whose
 * records have all expired stops looking infinitely readable.
 */
public final class RetentionPass {

    /** Where the boundary is recorded — {@code CheckpointWriter::observeRetained}. */
    @FunctionalInterface
    public interface RetainedSink {
        void observeRetained(Map<RunKey, Long> oldestRetained);
    }

    private final SegmentGc gc;
    private final RetainedSink sink;

    public RetentionPass(SegmentGc gc, RetainedSink sink) {
        this.gc = Objects.requireNonNull(gc, "gc");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * Collects what the rule condemns, then reports where each stream now
     * starts.
     *
     * @param nextOffsets each stream's next offset, for the streams whose
     *     segments are all gone
     */
    public SegmentGc.Result run(List<CommitDelta> chain, Map<RunKey, Long> nextOffsets) {
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(nextOffsets, "nextOffsets");
        SegmentGc.Result result = gc.collect(chain);
        Map<RunKey, Long> retained = RetainedOffsets.after(chain,
                java.util.Set.copyOf(result.deletedKeys()), nextOffsets);
        if (!retained.isEmpty()) {
            sink.observeRetained(retained);
        }
        return result;
    }
}
