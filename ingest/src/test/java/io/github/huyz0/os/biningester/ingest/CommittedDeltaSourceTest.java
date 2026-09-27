// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CommittedDeltaSourceTest {

    @Test
    void legacyPerStreamSourceCannotSilentlyMaterializeNodeReplay() {
        CommittedDeltaSource source = (key, offset, limit) -> offset < 0
                ? List.of(new CommittedDeltaSource.CommittedRun(
                        key, "segments/one", 1, 0))
                : List.of();

        assertThatThrownBy(() -> source.openSegments(List.of(
                new CommittedDeltaSource.ReplayRequest(
                        new RunKey(UUID.randomUUID(), 0), -1),
                new CommittedDeltaSource.ReplayRequest(
                        new RunKey(UUID.randomUUID(), 1), -1))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("segment-ordered replay");
    }

    // M9.31-T1: production snapshot replay overrides openSegments, so this legacy SINGLETON
    // default is otherwise unpinned. One stream must still replay through the per-stream
    // cursor, one run per segment, in order and after the exclusive offset.
    @Test
    void legacyPerStreamSourceReplaysOneStreamAsSingletonSegments() {
        RunKey key = new RunKey(UUID.randomUUID(), 0);
        List<CommittedDeltaSource.CommittedRun> committed = List.of(
                new CommittedDeltaSource.CommittedRun(key, "segments/one", 2, 0),
                new CommittedDeltaSource.CommittedRun(key, "segments/two", 3, 2),
                new CommittedDeltaSource.CommittedRun(key, "segments/three", 1, 5));
        CommittedDeltaSource source = (k, offset, limit) -> committed.stream()
                .filter(run -> run.firstOffset() + run.recordCount() - 1 > offset)
                .limit(limit)
                .toList();

        var cursor = source.openSegments(List.of(
                new CommittedDeltaSource.ReplayRequest(key, 1)));

        assertThat(cursor.next()).contains(new CommittedDeltaSource.ReplaySegment(
                "segments/two", List.of(committed.get(1))));
        assertThat(cursor.next()).contains(new CommittedDeltaSource.ReplaySegment(
                "segments/three", List.of(committed.get(2))));
        assertThat(cursor.next()).isEmpty();
    }
}
