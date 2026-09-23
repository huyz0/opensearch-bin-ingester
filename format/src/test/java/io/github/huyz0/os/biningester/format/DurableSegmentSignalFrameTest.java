// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class DurableSegmentSignalFrameTest {

    private static final DurableSegmentSignalFrame SMALLEST =
            new DurableSegmentSignalFrame("pod", "az", "k");
    private static final String GOLDEN_V1 = "425044530000000103706f6402617a016b";

    @Test
    void encodingMatchesTheVersionedGoldenAndRoundTrips() throws IOException {
        assertThat(SMALLEST.encode()).containsExactly(HexFormat.of().parseHex(GOLDEN_V1));
        assertThat(DurableSegmentSignalFrame.decode(SMALLEST.encode())).isEqualTo(SMALLEST);
    }

    @Test
    void unknownVersionAndTrailingBytesAreRefused() {
        byte[] future = HexFormat.of().parseHex(GOLDEN_V1);
        future[7] = 2;
        assertThatThrownBy(() -> DurableSegmentSignalFrame.decode(future))
                .isInstanceOf(IOException.class).hasMessageContaining("unsupported");

        byte[] trailing = java.util.Arrays.copyOf(SMALLEST.encode(), SMALLEST.encode().length + 1);
        assertThatThrownBy(() -> DurableSegmentSignalFrame.decode(trailing))
                .isInstanceOf(IOException.class).hasMessageContaining("trailing");
    }

    @Test
    void fieldBoundsAndBlankValuesAreRefused() {
        assertThatThrownBy(() -> new DurableSegmentSignalFrame(" ", "az", "k"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
        assertThatThrownBy(() -> new DurableSegmentSignalFrame("p".repeat(257), "az", "k"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bound");
    }

    @Test
    void maximumFrameIs1550BytesAndOversizedKeysAreRefused() {
        DurableSegmentSignalFrame maximum = new DurableSegmentSignalFrame(
                "p".repeat(256), "a".repeat(256), "k".repeat(1024));
        assertThat(maximum.encode()).hasSize(1550);
        assertThatThrownBy(() -> new DurableSegmentSignalFrame("p", "az", "k".repeat(1025)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bound");
        assertThatThrownBy(() -> DurableSegmentSignalFrame.decode(new byte[1551]))
                .isInstanceOf(IOException.class).hasMessageContaining("1550");
    }
}
