// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory {@link Sequencer} for tests that need offsets assigned without a
 * store. Ships with the seam in the same commit — architecture.md's rule for all
 * four I/O seams.
 *
 * <p>⚠️ IT HONOURS THE SAME CONTRACT THE INTERFACE STATES, and its own tests pin
 * it there. A fake that quietly differs is worse than no fake at all: every test
 * written against it passes while the real implementation is free to behave
 * differently, and the divergence surfaces only in production.
 *
 * <p>⚠️ NOT thread-safe, and deliberately so — a test that needs concurrency is
 * testing the real implementation's locking, not this. The real one is M4.2
 * onward.
 *
 * <p>⚠️ What this fake does NOT model, on purpose: durability, leases, epochs,
 * sealing, and — for now — idempotency on {@code (podId, flushSeq)}, which is
 * M4.10's task and lands here in the same commit as the real behaviour so the
 * two cannot drift. ⚠️ Until then a replayed {@code (podId, flushSeq)} is
 * treated as a NEW commit and assigned fresh offsets, here and in the real
 * implementation alike — see {@link Sequencer}'s failure-and-retry note.
 */
public final class FakeSequencer implements Sequencer {

    /** ⚠️ Per stream, not global — offsets are per (index, partition) (FR-3). */
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();

    private long nextSequence;

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) {
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("a commit with no segments commits nothing");
        }
        // ⚠️ VALIDATED BEFORE ANYTHING IS MUTATED, like the real log. An earlier
        // draft built the segments first and let `CommitDelta`'s constructor
        // refuse a duplicate key — by which point `nextOffsets` had advanced and
        // `nextSequence` had been incremented, so a fake that REJECTED a batch
        // had still moved. A caller's retry would then see offsets skip, which
        // the real `CommitLog` never does.
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (CommitRequest r : requests) {
            if (!keys.add(r.segmentKey())) {
                throw new IllegalArgumentException(
                        "two submissions in one batch name the same segment: " + r.segmentKey());
            }
        }
        // ⚠️ KEPT IN STEP WITH THE REAL LOG, including the part that is easy to
        // get wrong: offsets advance ACROSS segments, so two pods flushing the
        // same stream into one batch do not both start where the fake had
        // reached. A fake that got this wrong would let a caller's tests pass
        // while the real sequencer assigned one range twice — which is I2, and
        // is exactly the bug a fake exists to make visible rather than hide.
        List<binjava.format.SegmentCommit> segments = new ArrayList<>(requests.size());
        for (CommitRequest request : requests) {
            List<RunCommit> runs = new ArrayList<>(request.recordCounts().size());
            // ⚠️ Sorted, so a delta's runs are in a deterministic order and two
            // replays of the same commit produce byte-identical deltas. The real
            // CommitLog sorts for the same reason.
            request.recordCounts().entrySet().stream()
                    // ⚠️ RunKey is already Comparable, sorted by
                    // (indexId, partitionId) "so that ONE consumer's runs are
                    // adjacent" -- reuse that rather than restate it here.
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        long first = nextOffsets.getOrDefault(e.getKey(), 0L);
                        runs.add(new RunCommit(e.getKey(), e.getValue(), first));
                        nextOffsets.put(e.getKey(), first + e.getValue());
                    });
            segments.add(new binjava.format.SegmentCommit(request.segmentKey(), runs));
        }
        return new CommitDelta(nextSequence++, segments);
    }

    /** The offset the next commit for {@code key} would receive. */
    public long nextOffset(RunKey key) {
        return nextOffsets.getOrDefault(key, 0L);
    }

    @Override
    public void close() {
    }
}
