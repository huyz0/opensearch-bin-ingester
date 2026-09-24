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

    /**
     * Opens a request-wide cursor in committed segment order.
     *
     * <p>Implementations must stream one segment group at a time. The legacy per-stream replay
     * method cannot safely derive this cursor: doing so would retain the complete backlog and
     * could reorder segments across streams.
     */
    default SegmentReplayCursor openSegments(List<ReplayRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        if (requests.size() != 1) {
            throw new UnsupportedOperationException(
                    "multi-stream segment-ordered replay requires a segment-ordered source");
        }
        ReplayRequest request = Objects.requireNonNull(requests.get(0), "request");
        ReplayCursor cursor = open(request.key(), request.exclusiveOffset());
        return () -> cursor.next().map(run -> new ReplaySegment(run.segmentKey(), List.of(run)));
    }

    /** One stream's sequential replay cursor. */
    interface ReplayCursor {
        Optional<CommittedRun> next();

        /** Rewinds the last returned run after its sink rejected delivery. */
        void retry();
    }

    /** One physical segment and the requested runs it carries. */
    record ReplaySegment(String segmentKey, List<CommittedRun> runs) {
        public ReplaySegment {
            if (segmentKey == null || segmentKey.isBlank()) {
                throw new IllegalArgumentException("segment key is required");
            }
            runs = List.copyOf(Objects.requireNonNull(runs, "runs"));
            if (runs.isEmpty() || runs.stream().anyMatch(run -> !segmentKey.equals(run.segmentKey()))) {
                throw new IllegalArgumentException("a replay segment needs matching runs");
            }
        }
    }

    /** Request-wide cursor yielding one segment's matching runs at a time. */
    @FunctionalInterface
    interface SegmentReplayCursor {
        Optional<ReplaySegment> next();
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
    record CommittedRun(RunKey key, String segmentKey, int recordCount, long firstOffset,
            long chainSequence) {
        public static final long CHAIN_SEQUENCE_UNKNOWN = -1L;

        public CommittedRun(RunKey key, String segmentKey, int recordCount, long firstOffset) {
            this(key, segmentKey, recordCount, firstOffset, CHAIN_SEQUENCE_UNKNOWN);
        }

        public CommittedRun {
            Objects.requireNonNull(key, "key");
            if (segmentKey == null || segmentKey.isBlank()) {
                throw new IllegalArgumentException("segment key is required");
            }
            if (recordCount <= 0 || firstOffset < 0 || chainSequence < CHAIN_SEQUENCE_UNKNOWN) {
                throw new IllegalArgumentException("committed run bounds are invalid");
            }
        }

        long lastOffset() {
            return firstOffset + recordCount - 1;
        }
    }
}
