// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * A seventeen-byte file whose checksum VERIFIES but whose magic or version is
 * not this one's is refused (M13.26c review round 1, T3): another build's or
 * another file, never read as an epoch.
 */
class FastEpochFileHeaderTest {

    private static byte[] checksummed(int magic, int version, long epoch) {
        ByteBuffer out = ByteBuffer.allocate(FastEpochFile.LENGTH);
        out.putInt(magic).put((byte) version).putLong(epoch);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, FastEpochFile.LENGTH - 4);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    @Test
    void aFOREIGNMagicWithAValidChecksumIsRefused() {
        assertThatThrownBy(() -> FastEpochFile.decode(checksummed(0x42464A45, 1, 9)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void aLATERVersionWithAValidChecksumIsRefused() {
        assertThatThrownBy(() -> FastEpochFile.decode(checksummed(FastEpochFile.MAGIC, 2, 9)))
                .isInstanceOf(IOException.class);
    }
}
