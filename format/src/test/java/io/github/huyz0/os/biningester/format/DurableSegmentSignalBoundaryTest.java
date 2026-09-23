// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class DurableSegmentSignalBoundaryTest {
    @Test
    void exactFieldAndFrameLimitsRoundTripAndBlankClaimsAreRejected() throws IOException {
        DurableSegmentSignalFrame atLimit = new DurableSegmentSignalFrame(
                "p".repeat(256), "a".repeat(256), "k".repeat(1024));
        byte[] encoded = atLimit.encode();

        assertThat(encoded).hasSize(1550);
        assertThat(DurableSegmentSignalFrame.decode(encoded)).isEqualTo(atLimit);
        assertThat(atLimit).isNotEqualTo(new DurableSegmentSignalFrame(
                "p".repeat(256), "a".repeat(256), "z".repeat(1024)));
        assertThatThrownBy(() -> new DurableSegmentSignalFrame("pod", " ", "key"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DurableSegmentSignalFrame("pod", "az", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
