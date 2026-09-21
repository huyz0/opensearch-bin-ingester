// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Which chain objects may be collected (M7.7, FR-9,
 * <a href="../../../../../../docs/internal/product/decisions/0033-a-checkpoint-carries-offsets-and-flush-seqs-never-the-segment-index.md">ADR-0033</a>,
 * <a href="../../../../../../docs/internal/product/decisions/0036-the-idempotency-key-carries-an-explicit-pod-incarnation.md">ADR-0036</a>).
 *
 * <p>⚠️ A DELTA IS COLLECTABLE ONLY WHEN ALL THREE HOLD, and each of the last
 * two has a silent failure behind it:
 * <ol>
 * <li><b>Its sequence is BELOW the newest checkpoint's.</b> That sequence is
 * EXCLUSIVE — the next slot not covered — so a reader resumes by replaying FROM
 * it, and collecting the delta AT it drops one delta's offsets (invariant
 * I2).</li>
 * <li><b>Every segment it indexes has itself been deleted.</b> ADR-0033 keeps
 * the offset-to-segment index in the DELTAS and deliberately out of the
 * checkpoint, so a delta is the only object that can say which segment holds a
 * given offset. Collecting it while its segments are retained leaves them
 * unlocatable: a consumer reading an offset INSIDE the retention window
 * resolves to nothing, and every delete-side assertion stays green.</li>
 * <li><b>No live checkpoint pod slot points at it.</b> ADR-0036 answers a
 * detected replay by READING the pointed delta, so a 404 there turns "a
 * detected replay is ALWAYS answered" into an exception on a retry that is
 * safe. ⚠️ AND THE POINTER CAN BE OF ANY AGE: since M5.55 inherited pointers
 * ride in every checkpoint and are re-propagated by each successor, so pinning
 * by age passes on a young chain and deletes the pointer's target on a real
 * one.</li>
 * </ol>
 *
 * <p>⚠️ A POINTER IS {@code (epoch, sequence)} AND BOTH HALVES MATTER. Matching
 * on the sequence alone pins the wrong object across a takeover — or fails to
 * pin the right one.
 *
 * <p>⚠️ OLDER CHECKPOINTS ARE HISTORY, AND A TAIL OF THEM IS KEPT. The newest
 * is what {@code LATEST} points at and what bounds every recovery walk, and a
 * recovery that has already read {@code LATEST} and is following it would 404
 * on a checkpoint deleted between the two requests. {@code keepNewest} bounds
 * that race by {@code keepNewest - 1} CHECKPOINT INTERVALS, which is not the
 * same as making it harmless: {@code CheckpointWriter} checkpoints on a ticker,
 * so the number written between a recovery's {@code LATEST} read and its
 * follow-up GET is bounded by that interval and by how long the reader stalled,
 * never by one. Two is the floor this class refuses below; an operator whose
 * readers can stall for several intervals sets it higher.
 *
 * <p>⚠️ IT ISSUES NO LIST AND NO GET. The caller already replayed the chain it
 * passes in, and re-reading each delta to delete it would double the cost of
 * every recovery. DELETEs are free and batch 1,000 keys per call.
 */
public final class ChainGc {

    private static final Logger LOG = java.lang.System.getLogger(ChainGc.class.getName());

    /**
     * One checkpoint, and where in the chain it lives.
     *
     * <p>⚠️ IT CARRIES ITS EPOCH, like a delta does. A checkpoint key is per
     * epoch — {@code <prefix>/ctl/log/0/<epoch>/ckpt/<seq>.ckpt} — so a bare
     * sequence deletes a key that does not exist, counts it as collected, and
     * leaves the real object behind.
     */
    public record CheckpointAt(long epoch, long sequence) implements Comparable<CheckpointAt> {

        public CheckpointAt {
            if (epoch < 0 || sequence < 0) {
                throw new IllegalArgumentException(
                        "a chain address is never negative: " + epoch + "/" + sequence);
            }
        }

        @Override
        public int compareTo(CheckpointAt other) {
            int byEpoch = Long.compare(epoch, other.epoch);
            return byEpoch != 0 ? byEpoch : Long.compare(sequence, other.sequence);
        }
    }

    /** One delta, and where in the chain it lives. */
    public record DeltaAt(long epoch, long sequence, CommitDelta delta) {

        public DeltaAt {
            Objects.requireNonNull(delta, "delta");
            if (epoch < 0 || sequence < 0) {
                throw new IllegalArgumentException(
                        "a chain address is never negative: " + epoch + "/" + sequence);
            }
        }
    }

    /**
     * What one pass did.
     *
     * <p>⚠️ THE TWO KEEP REASONS ARE COUNTED SEPARATELY, because they mean
     * different things to an operator: a chain full of pointer-pinned deltas is
     * a fleet that keeps restarting, and one full of retained-segment deltas is
     * simply retention doing its job.
     */
    public record Result(int deltasDeleted, int pinnedByPointer, int holdingRetainedSegments,
            int checkpointsDeleted, List<DeltaAt> collected,
            List<CheckpointAt> collectedCheckpoints) {

        /**
         * ⚠️ {@code collected} AND {@code collectedCheckpoints} ARE WHAT LANDED
         * (M8.39), so a caller holding them in memory forgets exactly those: a
         * failed batch is still in the bucket, and is judged again.
         */
        public Result {
            collected = List.copyOf(collected);
            collectedCheckpoints = List.copyOf(collectedCheckpoints);
        }

        /** A pass that deleted nothing. */
        static Result none() {
            return new Result(0, 0, 0, 0, List.of(), List.of());
        }
    }

    private final BinStore store;
    private final String prefix;
    private final int deleteBatchSize;
    private final int keepNewest;

    /**
     * ⚠️ NO EPOCH PARAMETER, AND THAT IS THE FIX RATHER THAN AN OMISSION. Both
     * a delta and a checkpoint carry their own epoch in their address, because
     * one chain holds objects written before a takeover as well as after it —
     * so a key built from a single configured epoch deletes a key that does not
     * exist, counts it as collected, and leaves the real object to leak.
     */
    public ChainGc(BinStore store, String prefix, int deleteBatchSize, int keepNewest) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (deleteBatchSize <= 0) {
            throw new IllegalArgumentException("deleteBatchSize is never " + deleteBatchSize);
        }
        if (keepNewest < 2) {
            // ⚠️ TWO, NOT ONE. Keeping only the newest leaves a recovery that
            // read `LATEST` a request ago following a pointer to an object this
            // pass just deleted.
            throw new IllegalArgumentException("keepNewest is at least 2, not " + keepNewest
                    + " -- a recovery that has read LATEST and is following it would 404 "
                    + "on a checkpoint deleted between the two requests");
        }
        this.deleteBatchSize = deleteBatchSize;
        this.keepNewest = keepNewest;
    }

    /**
     * One pass over a chain the caller has already replayed.
     *
     * @param newest the newest checkpoint, whose pod slots pin deltas
     * @param newestAt where that checkpoint is: its chain, and the sequence it
     *     covers up to, EXCLUSIVE. ⚠️ BOTH, since M8.39: a chain held across a
     *     takeover holds two numbering spaces, and a sequence alone kept a
     *     predecessor's deltas for ever -- their sequences run past a young
     *     successor's checkpoint -- while collecting a NEWER epoch's delta that
     *     no checkpoint covers
     * @param retainedSegments segment keys that still exist — a delta naming
     *     any of them is kept
     * @param checkpoints every checkpoint of this chain, in ANY order — they
     *     are sorted here rather than trusted, because the caller's natural
     *     order is a walk back from {@code LATEST}, which is descending
     */
    public Result collect(Checkpoint newest, CheckpointAt newestAt, List<DeltaAt> chain,
            Set<String> retainedSegments, List<CheckpointAt> checkpoints) {
        Objects.requireNonNull(newest, "newest");
        Objects.requireNonNull(newestAt, "newestAt");
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(retainedSegments, "retainedSegments");
        Objects.requireNonNull(checkpoints, "checkpoints");

        Set<String> pointers = new java.util.HashSet<>();
        for (Checkpoint.PodState pod : newest.pods().values()) {
            if (pod.hasPointer()) {
                pointers.add(address(pod.epoch(), pod.sequence()));
            }
        }

        List<DeltaAt> doomed = new ArrayList<>();
        int pinned = 0;
        int retaining = 0;
        for (DeltaAt at : chain) {
            if (!coveredBy(at, newestAt)) {
                continue;
            }
            if (pointers.contains(address(at.epoch(), at.sequence()))) {
                pinned++;
                continue;
            }
            if (namesARetainedSegment(at.delta(), retainedSegments)) {
                retaining++;
                continue;
            }
            doomed.add(at);
        }

        // ⚠️ SORTED HERE RATHER THAN TRUSTED. The caller's natural order is a
        // walk BACK from `LATEST`, which is DESCENDING -- and keeping "the last
        // keepNewest of the list" over a descending list keeps the OLDEST and
        // deletes the newest, the one object recovery cannot lose.
        List<CheckpointAt> ordered = new ArrayList<>(checkpoints);
        java.util.Collections.sort(ordered);
        List<CheckpointAt> doomedCheckpoints = new ArrayList<>();
        for (int i = 0; i < ordered.size() - keepNewest; i++) {
            doomedCheckpoints.add(ordered.get(i));
        }

        // ⚠️ TWO PASSES, NOT ONE LIST. A partial failure lands wherever the
        // store put it, so deriving one count by subtracting the other from a
        // total mis-attributes it: review MEASURED a first-batch failure
        // reporting two deltas deleted and no checkpoints, while one delta was
        // still in the bucket and a checkpoint had gone. The cost is at most
        // one extra DELETE call per pass, at the boundary between the two.
        List<DeltaAt> collected = deleteInBatches(doomed,
                at -> new LogKeys(prefix, at.epoch()).keyFor(at.sequence()));
        List<CheckpointAt> collectedCheckpoints = deleteInBatches(doomedCheckpoints,
                at -> new LogKeys(prefix, at.epoch()).checkpointKeyFor(at.sequence()));
        return new Result(collected.size(), pinned, retaining, collectedCheckpoints.size(),
                collected, collectedCheckpoints);
    }

    /** Whether {@code at} is before {@code checkpoint}, in (epoch, sequence) order. */
    private static boolean coveredBy(DeltaAt at, CheckpointAt checkpoint) {
        return at.epoch() < checkpoint.epoch()
                || (at.epoch() == checkpoint.epoch() && at.sequence() < checkpoint.sequence());
    }

    private static boolean namesARetainedSegment(CommitDelta delta, Set<String> retained) {
        for (SegmentCommit segment : delta.segments()) {
            if (retained.contains(segment.segmentKey())) {
                return true;
            }
        }
        return false;
    }

    private static String address(long epoch, long sequence) {
        return epoch + "/" + sequence;
    }

    /** Deletes {@code items} by key a batch at a time, and returns those whose batch landed. */
    private <T> List<T> deleteInBatches(List<T> items,
            java.util.function.Function<T, String> keyOf) {
        List<T> deleted = new ArrayList<>();
        for (int from = 0; from < items.size(); from += deleteBatchSize) {
            List<T> batch = items.subList(from, Math.min(from + deleteBatchSize, items.size()));
            try {
                store.delete(batch.stream().map(keyOf).toList());
                deleted.addAll(batch);
            } catch (IOException failed) {
                // ⚠️ NOT COUNTED, AND THE PASS CONTINUES. The objects are still
                // there and the next pass judges them again from the same
                // chain, which is why this class needs no memory of its own.
                LOG.log(Logger.Level.WARNING, () -> "a batch of " + batch.size()
                        + " chain objects was not deleted; the next pass judges them "
                        + "again: " + failed);
            }
        }
        return deleted;
    }
}
