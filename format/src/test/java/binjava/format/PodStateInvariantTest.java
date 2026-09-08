// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.Checkpoint.PodState;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * `PodState`'s constructor guards, which review measured DELETABLE with the
 * whole build green.
 *
 * <p>⚠️ THIS IS THE UNTRUSTED-DECODE PATH. A checkpoint is bytes from an object
 * store, so every other field of it is held by
 * {@link CheckpointDecodeRefusalTest}; these three guards had none, and a
 * malformed slot is exactly what a torn or hostile object produces.
 */
class PodStateInvariantTest {

    private static final UUID INDEX = UUID.fromString("11112222-3333-4444-5555-666677778888");

    /**
     * ⚠️ `-1/-1`, NOT `0/0`. `(0, 0)` is a REAL delta address — epoch 0,
     * sequence 0 — so a bare slot encoded that way looks addressable, and a
     * replay would be answered from whatever object sits there.
     */
    @Test
    void aBareSlotCarriesNoAddressableePointer() {
        PodState bare = PodState.bare(7);

        assertThat(bare.epoch()).as("not 0, which is a real epoch").isEqualTo(-1);
        assertThat(bare.sequence()).as("not 0, which is a real sequence").isEqualTo(-1);
        assertThat(bare.hasPointer()).isFalse();
        assertThat(bare.incarnationId()).isNull();
    }

    @Test
    void aHalfPointerIsRefusedInBothDirections() {
        assertThatThrownBy(() -> new PodState("inc", 1, 5, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both parts or neither");
        assertThatThrownBy(() -> new PodState("inc", 1, -1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both parts or neither");
    }

    @Test
    void anIncarnationAndAPointerTravelTogether() {
        // an incarnation with no pointer...
        assertThatThrownBy(() -> new PodState("inc", 1, -1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("travel together");
        // ...and a pointer with no incarnation, which is the slot a replay
        // would be answered from without knowing whose it is.
        assertThatThrownBy(() -> new PodState(null, 1, 2, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("travel together");
    }

    @Test
    void aBlankIncarnationIsRefusedAndANegativeWatermarkToo() {
        assertThatThrownBy(() -> new PodState(" ", 1, 2, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never blank");
        assertThatThrownBy(() -> PodState.bare(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never negative");
    }

    /** ⚠️ A null VALUE reaches the map through an unmodifiable view, which
     * `Map.copyOf` would have rejected but this constructor does not use. */
    @Test
    void aNullSlotIsRefused() {
        Map<String, PodState> withNull = new java.util.HashMap<>();
        withNull.put("poda", null);
        assertThatThrownBy(() -> new Checkpoint(1, Map.of(), withNull))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("poda");
    }
}
