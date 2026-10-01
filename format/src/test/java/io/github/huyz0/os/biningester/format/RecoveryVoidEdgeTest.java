// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A void ending exactly where its stream's run begins does not overlap it
 * (M13.25a review round 1, T2): the bound is exclusive.
 *
 * <p>⚠️ REFUSED, that recovery would fail every reader's decode and the
 * sequencer could not start over its own chain.
 */
class RecoveryVoidEdgeTest {

    @Test
    void aVOIDEndingWhereTheRunBeginsIsAccepted() {
        RunKey a = new RunKey(new UUID(1, 1), 0);

        Recovery accepted = new Recovery(0,
                List.of(new SegmentCommit("seg/a", List.of(new RunCommit(a, 5, 10)))),
                List.of(new Recovery.VoidRange(a, 0, 10)));

        assertThat(accepted.voids()).containsExactly(new Recovery.VoidRange(a, 0, 10));
    }
}
