// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * A peer is rejected rather than silently unusable (M5.8).
 *
 * <p>⚠️ A BLANK AZ IS THE ONE THAT BITES QUIETLY. {@code StaticMembership}
 * files peers by AZ, so a blank one lands under {@code ""} and that pod simply
 * never appears in any ring -- it stops fetching and nothing says so. Review
 * MEASURED that all three guards could be deleted with the suite green.
 */
class PeerTest {

    @Test
    void aPeerWithoutAPODIDIsRefused() {
        assertThatThrownBy(() -> new Peer(" ", "10.0.1.1:9000", "az-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podId");
    }

    @Test
    void aPeerWithoutAnENDPOINTIsRefused() {
        assertThatThrownBy(() -> new Peer("pod-a1", "", "az-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
    }

    /**
     * NULL is refused as well as blank.
     *
     * <p>⚠️ REVIEW MEASURED that dropping the null half of each guard survives
     * the blank cases -- {@code isBlank()} throws NPE on null, so the failure
     * arrives as the wrong exception type from a different line.
     */
    @Test
    void aNULLFieldIsRefusedTooAndNotJustABlankOne() {
        assertThatThrownBy(() -> new Peer(null, "10.0.1.1:9000", "az-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podId");
        assertThatThrownBy(() -> new Peer("pod-a1", null, "az-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
    }

    @Test
    void aPeerWithoutAnAZIsRefused() {
        assertThatThrownBy(() -> new Peer("pod-a1", "10.0.1.1:9000", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intra-AZ only");
        // ⚠️ AND BLANK, which is the case this class's own javadoc calls the
        // one that bites quietly -- and which nothing pinned until review
        // MEASURED that narrowing the guard to `az == null` left the whole
        // `:ingest` module green at 124 tests. podId and endpoint each had
        // both halves; az had only null.
        assertThatThrownBy(() -> new Peer("pod-a1", "10.0.1.1:9000", "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intra-AZ only");
    }
}
