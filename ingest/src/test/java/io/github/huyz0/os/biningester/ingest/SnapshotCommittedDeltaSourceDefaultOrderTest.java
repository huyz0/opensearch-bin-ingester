// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SnapshotCommittedDeltaSourceDefaultOrderTest {

    @Test
    void openSegmentsPreservesCommitOrderAcrossDeltasWithTheProductionIterator() {
        RunKey key = new RunKey(UUID.randomUUID(), 2);
        CommitDelta first = new CommitDelta(1, List.of(new SegmentCommit("seg-first", List.of(
                new RunCommit(key, 1, 0)), null)));
        CommitDelta second = new CommitDelta(2, List.of(new SegmentCommit("seg-second", List.of(
                new RunCommit(key, 1, 1)), null)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(first, second), true, 1, 1, 1, 2, true);
        var cursor = new SnapshotCommittedDeltaSource(() -> snapshot)
                .openSegments(List.of(new CommittedDeltaSource.ReplayRequest(key, -1)));

        assertThat(cursor.next()).get().extracting(CommittedDeltaSource.ReplaySegment::segmentKey)
                .isEqualTo("seg-first");
        assertThat(cursor.next()).get().extracting(CommittedDeltaSource.ReplaySegment::segmentKey)
                .isEqualTo("seg-second");
        assertThat(cursor.next()).isEmpty();
    }
}
