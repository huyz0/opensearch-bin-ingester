// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** The cross-AZ hint: 24 fixed bytes, pinned (M10.17, ADR-0075). */
class DeltaHintFrameTest {

    private static byte[] golden(String name) throws IOException {
        try (var in = DeltaHintFrameTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aHintEncodesToItsStoredBytes() throws Exception {
        assertThat(new DeltaHintFrame(9, 41).encode()).isEqualTo(golden("delta-hint-v1.bin"));
    }

    @Test
    void theStoredHintStillDecodesToWhatItMeant() throws Exception {
        DeltaHintFrame decoded = DeltaHintFrame.decode(golden("delta-hint-v1.bin"));

        assertThat(decoded.epoch()).as("not swapped with the sequence").isEqualTo(9);
        assertThat(decoded.sequence()).isEqualTo(41);
    }

    @Test
    void malformedHintsAreRefused() throws Exception {
        byte[] good = new DeltaHintFrame(9, 41).encode();

        byte[] badMagic = good.clone();
        badMagic[0] = 0;
        assertThatThrownBy(() -> DeltaHintFrame.decode(badMagic)).isInstanceOf(IOException.class);

        byte[] badVersion = good.clone();
        badVersion[7] = 2;
        assertThatThrownBy(() -> DeltaHintFrame.decode(badVersion))
                .isInstanceOf(IOException.class).hasMessageContaining("version");

        assertThatThrownBy(() -> DeltaHintFrame.decode(java.util.Arrays.copyOf(good, 25)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> DeltaHintFrame.decode(java.util.Arrays.copyOf(good, 23)))
                .isInstanceOf(IOException.class);

        byte[] negativeSequence = good.clone();
        negativeSequence[16] = (byte) 0x80;
        assertThatThrownBy(() -> DeltaHintFrame.decode(negativeSequence))
                .isInstanceOf(IOException.class);
    }

    @Test
    void theBoundsAreEnforcedOnConstruction() {
        assertThatThrownBy(() -> new DeltaHintFrame(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeltaHintFrame(1, -1)).isInstanceOf(IllegalArgumentException.class);
        // The first epoch and the first sequence are real positions, not errors.
        assertThat(new DeltaHintFrame(1, 0).sequence()).isZero();
    }
}
