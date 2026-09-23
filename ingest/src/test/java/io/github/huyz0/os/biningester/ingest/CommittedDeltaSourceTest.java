// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

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
}
