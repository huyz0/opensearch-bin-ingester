// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Which fast frames are control (M13.66, ADR-0081 §12): the per-pod, per-term
 * JOIN, JOINED, DEPART, HELD and HELD_STATUS, under their own budget; every
 * other kind carries a write's data, inside NFR-5's ratio.
 */
class FastFrameControlKindsTest {

    @Test
    void theFIVEControlKindsAndNoOther() {
        Set<Integer> control = Set.of(FastFrame.KIND_JOIN, FastFrame.KIND_JOINED,
                FastFrame.KIND_DEPART, FastFrame.KIND_HELD, FastFrame.KIND_HELD_STATUS);
        for (int kind = 1; kind <= 17; kind++) {
            assertThat(FastFrame.isControl(kind)).as("kind %d", kind)
                    .isEqualTo(control.contains(kind));
        }
        assertThat(FastFrame.isControl(FastWriteFrame.KIND_COMMIT)).isFalse();
        assertThat(FastFrame.isControl(FastWriteFrame.KIND_REPLICA)).isFalse();
    }
}
