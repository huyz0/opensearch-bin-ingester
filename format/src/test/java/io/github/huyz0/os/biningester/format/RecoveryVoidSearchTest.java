// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The void-against-run overlap check finds the one void that can overlap a
 * run, from either side (M13.25a, review round 3 T1).
 *
 * <p>⚠️ IT IS A BINARY SEARCH over the stream's voids, so a search that stops
 * at the wrong void either refuses an entry it must accept or accepts one it
 * must refuse. Each case below fails one of those two ways.
 */
class RecoveryVoidSearchTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static Recovery recovery(long first, int count, List<Recovery.VoidRange> voids) {
        return new Recovery(0,
                List.of(new SegmentCommit("seg/a", List.of(new RunCommit(A, count, first)))),
                voids);
    }

    @Test
    void aVOIDWhollyBelowTheRunIsAccepted() {
        Recovery accepted = recovery(10, 5, List.of(new Recovery.VoidRange(A, 0, 5)));

        assertThat(accepted.voids()).hasSize(1);
    }

    @Test
    void aRUNOverlappingTheSecondVoidIsRefused() {
        assertThatThrownBy(() -> recovery(10, 12, List.of(
                        new Recovery.VoidRange(A, 0, 5),
                        new Recovery.VoidRange(A, 20, 30))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overlaps");
    }
}
