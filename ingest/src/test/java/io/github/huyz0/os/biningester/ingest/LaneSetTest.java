// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The pod's active lane set (M10.6, ADR-0074, M10 criterion 8).
 *
 * <p>⚠️ THE +2 CAP IS WHAT GIVES NFR-1's LANE TERM A CEILING: lane {@code +l}
 * may spend up to {@code 2^l} flushes per interval ceiling, so an active set
 * reaching {@code +7} would reinstate the full floor regime.
 */
class LaneSetTest {

    @Test
    void theDefaultIsMinusTwoToTwo() {
        LaneSet lanes = LaneSet.defaults();

        for (int lane = -2; lane <= 2; lane++) {
            assertThat(lanes.contains((byte) lane)).as("lane %d", lane).isTrue();
        }
        assertThat(lanes.contains((byte) 3)).isFalse();
        assertThat(lanes.contains((byte) -3)).isFalse();
        assertThat(lanes.highest()).isEqualTo((byte) 2);
    }

    @Test
    void aSetTheCostBoundCannotHoldIsRefusedAtConfiguration() {
        assertThatThrownBy(() -> LaneSet.of((byte) -8, (byte) -7, (byte) -6, (byte) -5,
                (byte) -4, (byte) -3, (byte) -2, (byte) -1, (byte) 0))
                .as("nine lanes: FR-18 caps the active set at 8")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("8");
        assertThatThrownBy(() -> LaneSet.of((byte) 0, (byte) 3))
                .as("+3 would spend 8 flushes per ceiling, 16 data+commit PUTs")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("+2");
        assertThatThrownBy(() -> LaneSet.of((byte) -1, (byte) 1))
                .as("0 is the lane every producer that says nothing is in")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("0");
        assertThat(LaneSet.of((byte) -7, (byte) -6, (byte) -5, (byte) -4, (byte) -3,
                (byte) -2, (byte) -1, (byte) 0).highest())
                .as("exactly eight, all at or below 0, is allowed").isEqualTo((byte) 0);
    }

    @Test
    void aSetIsParsedFromItsSettingsText() {
        LaneSet lanes = LaneSet.parse(" -1, 0,2 ");

        assertThat(lanes.contains((byte) -1)).isTrue();
        assertThat(lanes.contains((byte) 2)).isTrue();
        assertThat(lanes.contains((byte) 1)).isFalse();
        assertThatThrownBy(() -> LaneSet.parse("0,one"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LaneSet.parse("0,200"))
                .as("a lane is an i8").isInstanceOf(IllegalArgumentException.class);
    }
}
