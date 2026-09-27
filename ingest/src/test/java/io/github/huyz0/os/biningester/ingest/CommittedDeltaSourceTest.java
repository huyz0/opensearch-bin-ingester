// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Optional;
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

    /**
     * ⚠️ THE SINGLETON DEFAULT, PINNED ON ITS OWN (M10.11, harvested from
     * 8a0e9ec's review M9.31-T1). Production's snapshot source overrides it,
     * so no production test reaches it: a legacy source asked for ONE stream
     * yields that stream's runs one segment each, in offset order, after the
     * exclusive offset, and then nothing.
     */
    @Test
    void aLegacySourceAskedForOneStreamYieldsItsRunsOneSegmentEachAfterTheOffset() {
        RunKey key = new RunKey(UUID.randomUUID(), 0);
        List<CommittedDeltaSource.CommittedRun> runs = List.of(
                new CommittedDeltaSource.CommittedRun(key, "segments/a", 2, 0),
                new CommittedDeltaSource.CommittedRun(key, "segments/b", 3, 2),
                new CommittedDeltaSource.CommittedRun(key, "segments/c", 1, 5));
        CommittedDeltaSource source = (k, exclusive, limit) -> runs.stream()
                .filter(run -> run.key().equals(k) && run.firstOffset() > exclusive)
                .limit(limit)
                .toList();

        CommittedDeltaSource.SegmentReplayCursor cursor = source.openSegments(List.of(
                new CommittedDeltaSource.ReplayRequest(key, 1)));

        assertThat(cursor.next()).contains(
                new CommittedDeltaSource.ReplaySegment("segments/b", List.of(runs.get(1))));
        assertThat(cursor.next()).contains(
                new CommittedDeltaSource.ReplaySegment("segments/c", List.of(runs.get(2))));
        assertThat(cursor.next()).as("and then nothing").isEqualTo(Optional.empty());
    }
}
