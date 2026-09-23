// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SnapshotCommittedDeltaSourceSegmentLazinessTest {

    @Test
    void emitsCurrentSegmentWithoutTraversingLaterSegmentsInTheSameDelta() {
        RunKey key = new RunKey(UUID.randomUUID(), 1);
        CommitDelta delta = new CommitDelta(1, List.of(
                new SegmentCommit("seg-first", List.of(new RunCommit(key, 1, 0)), null),
                new SegmentCommit("seg-later", List.of(new RunCommit(key, 1, 1)), null)));
        AtomicInteger traversed = new AtomicInteger();
        Function<CommitDelta, Iterator<SegmentCommit>> segmentIterator = ignored -> new Iterator<>() {
            private final Iterator<SegmentCommit> delegate = delta.segments().iterator();

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public SegmentCommit next() {
                if (!delegate.hasNext()) {
                    throw new NoSuchElementException();
                }
                traversed.incrementAndGet();
                return delegate.next();
            }
        };
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(delta), true, 1, 1, 1, 1, true);
        var source = new SnapshotCommittedDeltaSource(
                () -> snapshot, ignored -> List.of(delta).iterator(), segmentIterator);
        var cursor = source.openSegments(List.of(new CommittedDeltaSource.ReplayRequest(key, -1)));

        assertThat(traversed).hasValue(0);
        assertThat(cursor.next()).get().extracting(CommittedDeltaSource.ReplaySegment::segmentKey)
                .isEqualTo("seg-first");
        assertThat(traversed).hasValue(1);
        assertThat(cursor.next()).get().extracting(CommittedDeltaSource.ReplaySegment::segmentKey)
                .isEqualTo("seg-later");
        assertThat(traversed).hasValue(2);
    }
}
