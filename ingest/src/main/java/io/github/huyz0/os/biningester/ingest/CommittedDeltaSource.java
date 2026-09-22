// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Durable committed-run source used by the node-scoped catch-up path (M8.24). */
@FunctionalInterface
public interface CommittedDeltaSource {

    /**
     * Returns at most {@code limit} committed runs after the exclusive offset.
     * Implementations must not materialize more than the requested limit.
     */
    List<CommittedRun> replay(RunKey key, long exclusiveOffset, int limit);

    /** Opens a cursor so successive catch-up turns do not rescan the chain. */
    default ReplayCursor open(RunKey key, long exclusiveOffset) {
        Objects.requireNonNull(key, "key");
        if (exclusiveOffset < -1) {
            throw new IllegalArgumentException("exclusive offset is invalid: " + exclusiveOffset);
        }
        return new ReplayCursor() {
            private long nextExclusive = exclusiveOffset;
            private long previousExclusive = exclusiveOffset;
            private boolean canRetry;

            @Override
            public Optional<CommittedRun> next() {
                List<CommittedRun> runs = replay(key, nextExclusive, 1);
                if (runs.isEmpty()) {
                    return Optional.empty();
                }
                CommittedRun run = runs.get(0);
                previousExclusive = nextExclusive;
                nextExclusive = run.lastOffset();
                canRetry = true;
                return Optional.of(run);
            }

            @Override
            public void retry() {
                if (!canRetry) {
                    throw new IllegalStateException("no replay run is available to retry");
                }
                nextExclusive = previousExclusive;
                canRetry = false;
            }
        };
    }

    /** One stream's sequential replay cursor. */
    interface ReplayCursor {
        Optional<CommittedRun> next();

        /** Rewinds the last returned run after its sink rejected delivery. */
        void retry();
    }

    /** One stream and its next exclusive resume offset in a node request. */
    record ReplayRequest(RunKey key, long exclusiveOffset) {
        public ReplayRequest {
            Objects.requireNonNull(key, "key");
            if (exclusiveOffset < -1) {
                throw new IllegalArgumentException("exclusive offset is invalid: "
                        + exclusiveOffset);
            }
        }
    }

    /** One committed run and the segment that contains its records. */
    record CommittedRun(RunKey key, String segmentKey, int recordCount, long firstOffset) {
        public CommittedRun {
            Objects.requireNonNull(key, "key");
            if (segmentKey == null || segmentKey.isBlank()) {
                throw new IllegalArgumentException("segment key is required");
            }
            if (recordCount <= 0 || firstOffset < 0) {
                throw new IllegalArgumentException("committed run bounds are invalid");
            }
        }

        long lastOffset() {
            return firstOffset + recordCount - 1;
        }
    }
}
