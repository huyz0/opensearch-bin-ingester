// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.Function;

/** Reads catch-up candidates from the recovered in-memory committed-chain snapshot. */
public final class SnapshotCommittedDeltaSource implements CommittedDeltaSource {

    private final Supplier<ChainMemory.Snapshot> snapshot;
    private final Function<ChainMemory.Snapshot, Iterator<CommitDelta>> deltaIterator;
    private final Function<CommitDelta, Iterator<SegmentCommit>> segmentIterator;

    public SnapshotCommittedDeltaSource(Supplier<ChainMemory.Snapshot> snapshot) {
        this(snapshot, current -> current.deltas().iterator(),
                delta -> delta.segments().iterator());
    }

    SnapshotCommittedDeltaSource(Supplier<ChainMemory.Snapshot> snapshot,
            Function<ChainMemory.Snapshot, Iterator<CommitDelta>> deltaIterator) {
        this(snapshot, deltaIterator, delta -> delta.segments().iterator());
    }

    SnapshotCommittedDeltaSource(Supplier<ChainMemory.Snapshot> snapshot,
            Function<ChainMemory.Snapshot, Iterator<CommitDelta>> deltaIterator,
            Function<CommitDelta, Iterator<SegmentCommit>> segmentIterator) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.deltaIterator = Objects.requireNonNull(deltaIterator, "deltaIterator");
        this.segmentIterator = Objects.requireNonNull(segmentIterator, "segmentIterator");
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

    @Override
    public SegmentReplayCursor openSegments(List<ReplayRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("a node replay needs at least one stream");
        }
        Map<RunKey, Long> offsets = new LinkedHashMap<>();
        for (ReplayRequest request : requests) {
            Objects.requireNonNull(request, "request");
            if (offsets.put(request.key(), request.exclusiveOffset()) != null) {
                throw new IllegalArgumentException("duplicate catch-up stream: " + request.key());
            }
        }
        ChainMemory.Snapshot current = completeSnapshot();
        return segmentCursor(deltaIterator.apply(current), offsets, segmentIterator);
    }

    static SegmentReplayCursor segmentCursor(Iterator<CommitDelta> deltas,
            Map<RunKey, Long> offsets) {
        return segmentCursor(deltas, offsets, delta -> delta.segments().iterator());
    }

    static SegmentReplayCursor segmentCursor(Iterator<CommitDelta> deltas,
            Map<RunKey, Long> offsets,
            Function<CommitDelta, Iterator<SegmentCommit>> segmentIterator) {
        Objects.requireNonNull(deltas, "deltas");
        Objects.requireNonNull(offsets, "offsets");
        Objects.requireNonNull(segmentIterator, "segmentIterator");
        return new SegmentReplayCursor() {
            private Iterator<SegmentCommit> segments = List.<SegmentCommit>of().iterator();

            @Override
            public Optional<ReplaySegment> next() {
                while (true) {
                    if (segments.hasNext()) {
                        SegmentCommit segment = segments.next();
                        List<CommittedRun> matching = new ArrayList<>();
                        for (RunCommit run : segment.runs()) {
                            Long exclusiveOffset = offsets.get(run.key());
                            if (exclusiveOffset != null && run.lastOffset() > exclusiveOffset) {
                                long first = Math.max(run.firstOffset(), exclusiveOffset + 1);
                                matching.add(new CommittedRun(run.key(), segment.segmentKey(),
                                        (int) (run.lastOffset() - first + 1), first));
                            }
                        }
                        if (!matching.isEmpty()) {
                            return Optional.of(new ReplaySegment(segment.segmentKey(), matching));
                        }
                        continue;
                    }
                    if (!deltas.hasNext()) {
                        return Optional.empty();
                    }
                    segments = segmentIterator.apply(deltas.next());
                }
            }
        };
    }

    private SnapshotCursor cursor(RunKey key, long exclusiveOffset) {
        Objects.requireNonNull(key, "key");
        if (exclusiveOffset < -1) {
            throw new IllegalArgumentException("exclusive offset is invalid: " + exclusiveOffset);
        }
        ChainMemory.Snapshot current = completeSnapshot();
        return new SnapshotCursor(key, exclusiveOffset, current.deltas());
    }

    private ChainMemory.Snapshot completeSnapshot() {
        ChainMemory.Snapshot current = Objects.requireNonNull(snapshot.get(), "snapshot result");
        if (!current.complete()) {
            throw new IllegalStateException(
                    "the committed-delta snapshot is incomplete and cannot replay safely");
        }
        return current;
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
