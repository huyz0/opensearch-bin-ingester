// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Two codec holes the golden cases left open (M13.23 review round 3, T1 and
 * T3).
 *
 * <p>⚠️ WAL AT THE DEFAULT TIMER IS STILL V2. Every golden {@code wal=true}
 * case sets a non-default timer, so an encoder that chose v1 by the timer
 * alone passed them all -- and wrote a {@code wal=true} index at 5 s as v1,
 * which every ingester reads as {@code wal=false}: the setting dropped in the
 * codec that test-plan row 7 names.
 */
class IndexRegistrationV2DefaultTimerTest {

    @Test
    void aWALIndexAtTheDEFAULTTimerIsV2AndSurvivesItsRoundTrip() throws Exception {
        IndexRegistration wal = IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "logs", 3)
                .withFastSettings(IndexRegistration.DEFAULT_FLUSH_TIMER_MILLIS, true, 3);

        byte[] bytes = wal.encode();

        assertThat(ByteBuffer.wrap(bytes).getInt(4))
                .as("wal=true is not 'at the defaults', whatever the timer")
                .isEqualTo(IndexRegistration.VERSION_2);
        assertThat(IndexRegistration.decode(bytes)).isEqualTo(wal);
    }

    @Test
    void aWALByteWithTheHIGHBitSetIsRefused() throws Exception {
        byte[] bytes = IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "logs", 3)
                .withFastSettings(250, false, 2).encode();
        bytes[bytes.length - 1] = (byte) 0x80;

        assertThatThrownBy(() -> IndexRegistration.decode(bytes))
                .as("a byte is signed in Java: 0x80 is -128, which `walByte > 1` lets through "
                        + "as wal=false")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("wal byte");
    }
}
