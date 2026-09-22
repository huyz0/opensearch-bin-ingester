// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import java.util.ArrayList;
import java.util.Optional;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Reads catch-up candidates from the recovered in-memory committed-chain snapshot. */
public final class SnapshotCommittedDeltaSource implements CommittedDeltaSource {

    private final Supplier<ChainMemory.Snapshot> snapshot;

    public SnapshotCommittedDeltaSource(Supplier<ChainMemory.Snapshot> snapshot) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    @Override
    public List<CommittedRun> replay(RunKey key, long exclusiveOffset, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("replay limit must be positive: " + limit);
        }
        SnapshotCursor cursor = cursor(key, exclusiveOffset);
        List<CommittedRun> result = new ArrayList<>(Math.min(limit, 16));
        while (result.size() < limit) {
            Optional<CommittedRun> next = cursor.next();
            if (next.isEmpty()) {
                break;
            }
            result.add(next.get());
        }
        return List.copyOf(result);
    }

    @Override
    public ReplayCursor open(RunKey key, long exclusiveOffset) {
        return cursor(key, exclusiveOffset);
    }

    private SnapshotCursor cursor(RunKey key, long exclusiveOffset) {
        Objects.requireNonNull(key, "key");
        if (exclusiveOffset < -1) {
            throw new IllegalArgumentException("exclusive offset is invalid: " + exclusiveOffset);
        }
        ChainMemory.Snapshot current = Objects.requireNonNull(snapshot.get(), "snapshot result");
        if (!current.complete()) {
            throw new IllegalStateException(
                    "the committed-delta snapshot is incomplete and cannot replay safely");
        }
        return new SnapshotCursor(key, exclusiveOffset, current.deltas());
    }

    private static final class SnapshotCursor implements ReplayCursor {
        private final RunKey key;
        private final List<CommitDelta> deltas;
        private int deltaIndex;
        private int segmentIndex;
        private int runIndex;
        private long exclusiveOffset;
        private int retryDeltaIndex;
        private int retrySegmentIndex;
        private int retryRunIndex;
        private long retryExclusiveOffset;
        private boolean canRetry;

        SnapshotCursor(RunKey key, long exclusiveOffset, List<CommitDelta> deltas) {
            this.key = key;
            this.exclusiveOffset = exclusiveOffset;
            this.deltas = deltas;
        }

        @Override
        public Optional<CommittedRun> next() {
            while (deltaIndex < deltas.size()) {
                List<SegmentCommit> segments = deltas.get(deltaIndex).segments();
                while (segmentIndex < segments.size()) {
                    SegmentCommit segment = segments.get(segmentIndex);
                    while (runIndex < segment.runs().size()) {
                        int candidateRunIndex = runIndex++;
                        RunCommit run = segment.runs().get(candidateRunIndex);
                        if (run.key().equals(key) && run.lastOffset() > exclusiveOffset) {
                            long first = Math.max(run.firstOffset(), exclusiveOffset + 1);
                            CommittedRun result = new CommittedRun(key, segment.segmentKey(),
                                    (int) (run.lastOffset() - first + 1), first);
                            retryDeltaIndex = deltaIndex;
                            retrySegmentIndex = segmentIndex;
                            retryRunIndex = candidateRunIndex;
                            retryExclusiveOffset = exclusiveOffset;
                            exclusiveOffset = result.lastOffset();
                            canRetry = true;
                            return Optional.of(result);
                        }
                    }
                    segmentIndex++;
                    runIndex = 0;
                }
                deltaIndex++;
                segmentIndex = 0;
            }
            return Optional.empty();
        }

        @Override
        public void retry() {
            if (!canRetry) {
                throw new IllegalStateException("no replay run is available to retry");
            }
            deltaIndex = retryDeltaIndex;
            segmentIndex = retrySegmentIndex;
            runIndex = retryRunIndex;
            exclusiveOffset = retryExclusiveOffset;
            canRetry = false;
        }
    }
}
