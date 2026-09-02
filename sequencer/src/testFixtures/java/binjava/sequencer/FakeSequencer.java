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
    public CommitDelta commit(CommitRequest request) {
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
        return new CommitDelta(nextSequence++, request.segmentKey(), runs);
    }

    /** The offset the next commit for {@code key} would receive. */
    public long nextOffset(RunKey key) {
        return nextOffsets.getOrDefault(key, 0L);
    }

    @Override
    public void close() {
    }
}
