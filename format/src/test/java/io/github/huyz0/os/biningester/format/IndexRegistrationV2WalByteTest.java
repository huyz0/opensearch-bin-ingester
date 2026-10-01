// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * A wal byte other than 0 or 1, with nothing behind it (M13.23 review round 2,
 * T1).
 *
 * <p>⚠️ THE OTHER CASE COULD NOT TELL "REFUSED" FROM "READ AS FALSE". Its frame
 * keeps the quorum byte after the corrupted wal byte, so a decoder reading 2
 * as {@code false} was still refused -- by the trailing-bytes check, for the
 * quorum byte left over. Here the corrupted byte is the frame's last, so
 * reading it as {@code false} decodes silently, and only the wal check
 * refuses it.
 */
class IndexRegistrationV2WalByteTest {

    @Test
    void aWALByteOfTwoAtTheEndOfTheFrameIsRefused() throws Exception {
        byte[] bytes;
        try (var in = getClass().getResourceAsStream("/golden/index-registration-v2-timer.bin")) {
            assertThat(in).isNotNull();
            bytes = in.readAllBytes();
        }
        bytes[bytes.length - 1] = 2;

        assertThatThrownBy(() -> IndexRegistration.decode(bytes))
                .as("read as wal=false this decodes cleanly; read as true it runs out of "
                        + "bytes -- only refusing the value itself is right")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("wal byte");
    }
}
