// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * The pod's epoch file (ADR-0082 §4 as amended by M13.26c), held to golden
 * bytes from an independent encoder, and refused whole when it does not decode.
 */
class FastEpochFileTest {

    private static byte[] golden(String name) throws IOException {
        try (var in = FastEpochFileTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void anEPOCHEncodesToAndDecodesFromItsGoldenBytes() throws Exception {
        assertThat(FastEpochFile.encode(0x0102030405060708L)).isEqualTo(golden("fast-epoch-v1.bin"));
        assertThat(FastEpochFile.encode(0)).isEqualTo(golden("fast-epoch-zero-v1.bin"));
        assertThat(FastEpochFile.decode(golden("fast-epoch-v1.bin"))).isEqualTo(0x0102030405060708L);
        assertThat(FastEpochFile.decode(golden("fast-epoch-zero-v1.bin"))).isZero();
    }

    @Test
    void aDAMAGEDOrForeignFileIsRefused() throws Exception {
        byte[] good = golden("fast-epoch-v1.bin");
        byte[] flipped = good.clone();
        flipped[8] ^= 1;
        byte[] magic = good.clone();
        magic[0] = 0;
        byte[] version = good.clone();
        version[4] = 2;

        for (byte[] bad : new byte[][] {flipped, magic, version, new byte[0],
                java.util.Arrays.copyOf(good, 16)}) {
            assertThatThrownBy(() -> FastEpochFile.decode(bad)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void aNEGATIVEEpochIsNeverWrittenNorRead() throws Exception {
        assertThatThrownBy(() -> FastEpochFile.encode(-1))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] negative = java.nio.ByteBuffer.allocate(17).putInt(FastEpochFile.MAGIC).put((byte) 1)
                .putLong(-1).array();
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(negative, 0, 13);
        java.nio.ByteBuffer.wrap(negative).putInt(13, (int) crc.getValue());

        assertThatThrownBy(() -> FastEpochFile.decode(negative)).isInstanceOf(IOException.class);
    }
}
