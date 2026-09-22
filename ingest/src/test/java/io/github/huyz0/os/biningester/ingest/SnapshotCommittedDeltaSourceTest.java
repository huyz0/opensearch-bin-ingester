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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SnapshotCommittedDeltaSourceTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 2);

    @Test
    void selectsOnlyTheRequestedStreamAfterItsExclusiveOffset() {
        CommitDelta first = new CommitDelta(1, List.of(new SegmentCommit("seg-a", List.of(
                new RunCommit(KEY, 3, 10)), null)));
        CommitDelta second = new CommitDelta(2, List.of(new SegmentCommit("seg-b", List.of(
                new RunCommit(KEY, 2, 13),
                new RunCommit(new RunKey(UUID.randomUUID(), 2), 4, 0)), null)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(first, second), true, 1, 1, 1, 2, true);

        var source = new SnapshotCommittedDeltaSource(() -> snapshot);

        assertThat(source.replay(KEY, 10, 8))
                .extracting(CommittedDeltaSource.CommittedRun::segmentKey)
                .containsExactly("seg-a", "seg-b");
        assertThat(source.replay(KEY, 10, 1)).hasSize(1);
        assertThat(source.replay(KEY, 10, 8).get(0).key()).isEqualTo(KEY);
        assertThat(source.replay(KEY, 10, 8).get(0).firstOffset()).isEqualTo(11);
        assertThat(source.replay(KEY, 10, 8).get(0).recordCount()).isEqualTo(2);
        assertThat(source.replay(KEY, 14, 8)).isEmpty();
    }

    @Test
    void rejectsAnOffsetBeforeTheSupportedSentinel() {
        var source = new SnapshotCommittedDeltaSource(() -> new ChainMemory.Snapshot(
                List.of(), true, 0, 0, 0, 0, true));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> source.replay(KEY, -2, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesAnIncompleteSnapshotInsteadOfSilentlySkippingHistory() {
        CommitDelta recent = new CommitDelta(2, List.of(new SegmentCommit("recent", List.of(
                new RunCommit(KEY, 1, 20)), null)));
        ChainMemory.Snapshot incomplete = new ChainMemory.Snapshot(
                List.of(recent), false, 1, 1, 1, 2, false);
        var source = new SnapshotCommittedDeltaSource(() -> incomplete);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> source.replay(KEY, -1, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incomplete");
    }

    @Test
    void coordinatorUsesTheSnapshotCursorWithoutRescanningItPerTurn() {
        CommitDelta delta = new CommitDelta(1, List.of(new SegmentCommit("seg-a", List.of(
                new RunCommit(KEY, 1, 0)), null)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(delta), true, 1, 1, 1, 1, true);
        var seen = new java.util.ArrayList<String>();
        AtomicInteger snapshots = new AtomicInteger();
        var coordinator = new CatchUpCoordinator(
                new SnapshotCommittedDeltaSource(() -> {
                    snapshots.incrementAndGet();
                    return snapshot;
                }), 1, 2,
                run -> seen.add(run.segmentKey()));

        assertThat(coordinator.enqueueCatchUp(KEY, -1)).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isFalse();
        assertThat(seen).containsExactly("seg-a");
        assertThat(snapshots).hasValue(1);
    }

    @Test
    void retriesTheSnapshotRunWhenItsSinkRejectsIt() {
        CommitDelta delta = new CommitDelta(1, List.of(new SegmentCommit("seg-a", List.of(
                new RunCommit(KEY, 1, 0)), null)));
        ChainMemory.Snapshot snapshot = new ChainMemory.Snapshot(
                List.of(delta), true, 1, 1, 1, 1, true);
        AtomicInteger deliveries = new AtomicInteger();
        var coordinator = new CatchUpCoordinator(
                new SnapshotCommittedDeltaSource(() -> snapshot), 1, 2,
                run -> {
                    if (deliveries.getAndIncrement() == 0) {
                        throw new IllegalStateException("sink unavailable");
                    }
                });

        assertThat(coordinator.enqueueCatchUp(KEY, -1)).isTrue();
        org.assertj.core.api.Assertions.assertThatThrownBy(coordinator::runNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("sink unavailable");
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isTrue();
        assertThat(coordinator.runNext()).isFalse();
        assertThat(deliveries).hasValue(2);
    }
}
