// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The recovery's remaining bounds (M13.25 review round 2, T2).
 */
class RecoveryBoundsTest {

    @Test
    void aSEGMENTCountPastTheBytesIsRefusedBeforeItSizesAList() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(8).putInt(ChainEntry.MAGIC)
                .putInt(ChainEntry.VERSION_KINDED).array());
        out.write(ChainEntry.KIND_RECOVERY);
        out.write(1);
        // uvarint 0x7FFFFFFF segments
        out.writeBytes(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07});

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("segments");
    }

    @Test
    void aNEGATIVESequenceIsRefused() {
        assertThatThrownBy(() -> new Recovery(-1, List.of(),
                        List.of(new Recovery.VoidRange(new RunKey(new UUID(1, 1), 0), 0, 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
