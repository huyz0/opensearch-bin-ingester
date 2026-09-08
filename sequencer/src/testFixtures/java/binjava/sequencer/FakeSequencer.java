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
 * sealing, and — for now — idempotency, which is M4.10d's task and lands here in
 * the same commit as the real behaviour so the two cannot drift. ⚠️ Until then a
 * replayed commit is treated as a NEW one and assigned fresh offsets, here and
 * in the real implementation alike — see {@link Sequencer}'s failure-and-retry
 * note.
 *
 * <p>⚠️ THE KEY IS NOT {@code (podId, flushSeq)}, and an earlier version of this
 * comment said it was. ADR-0036 measured that pair unable to tell a RESTART from
 * a REPLAY — {@code flushSeq} restarts at 0 while {@code podId} is stable — so
 * the key is {@code (podId, incarnationId, flushSeq)}. This fake DOES copy the
 * attribution onto each segment, because a fake that dropped it would let a test
 * pass while the real delta carried nothing for a successor to rebuild from.
 */
public final class FakeSequencer implements Sequencer {

    /** ⚠️ Per stream, not global — offsets are per (index, partition) (FR-3). */
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();
    /** Every triple this fake has applied, so a replay is answered (M4.10d). */
    private final Map<String, binjava.format.SegmentCommit> applied = new java.util.HashMap<>();

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
        // ⚠️ THE REFUSAL IS MODELLED (M4.10d), because a fake that applied a
        // replay twice would let a caller's retry tests pass while the real
        // sequencer answered instead of committing. Kept deliberately SIMPLER
        // than the real one: this answers from segments it remembers rather
        // than reading a delta back, so it models the CONTRACT -- a replayed
        // triple gets its original offsets and appends nothing -- without
        // modelling the chain.
        List<binjava.format.SegmentCommit> answered = new ArrayList<>();
        List<CommitRequest> fresh = new ArrayList<>(requests.size());
        for (CommitRequest request : requests) {
            binjava.format.SegmentCommit already = applied.get(tripleOf(request));
            if (already != null) {
                answered.add(already);
            } else {
                fresh.add(request);
            }
        }
        if (fresh.isEmpty()) {
            return new CommitDelta(nextSequence - 1, answered);
        }
        List<binjava.format.SegmentCommit> segments = new ArrayList<>(fresh.size());
        for (CommitRequest request : fresh) {
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
            binjava.format.SegmentCommit segment =
                    new binjava.format.SegmentCommit(request.segmentKey(), runs,
                            new binjava.format.SegmentCommit.Attribution(request.podId(),
                                    request.incarnationId(), request.flushSeq()));
            segments.add(segment);
            applied.put(tripleOf(request), segment);
        }
        segments.addAll(answered);
        return new CommitDelta(nextSequence++, segments);
    }

    private static String tripleOf(CommitRequest request) {
        // ⚠️ AND THE SEGMENT KEY, because production's `answers` requires it.
        // Without it one flush submitting several segments under one flushSeq
        // overwrites its own earlier entry, so both retried requests resolve to
        // the LAST segment and the returned delta names it twice -- measured by
        // review as the fake throwing where the real sequencer answers. A fake
        // that diverges here is worse than no fake: it fails a caller's retry
        // test for a reason production does not have.
        return request.podId() + "\0" + request.incarnationId() + "\0"
                + request.flushSeq() + "\0" + request.segmentKey();
    }

    /** The offset the next commit for {@code key} would receive. */
    public long nextOffset(RunKey key) {
        return nextOffsets.getOrDefault(key, 0L);
    }

    @Override
    public void close() {
    }
}
