// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The retained-floor frame (M8.6, ADR-0056).
 *
 * <p>⚠️ **THE GOLDEN BYTES WERE WRITTEN FROM THE ADR's LAYOUT BY HAND, NOT BY
 * THE ENCODER**, so the pin is independent of the code it pins: an encoder
 * that wrote the partition before the id, or the floor as a fixed long, would
 * agree with bytes it had produced itself and disagree with these.
 */
class RetainedFloorTest {

    private static final RunKey STREAM = new RunKey(
            new UUID(0x0123456789abcdefL, 0xfedcba9876543210L), 3);

    private static byte[] golden() throws IOException {
        try (var in = RetainedFloorTest.class.getResourceAsStream(
                "/golden/retained-floor-v1.bin")) {
            assertThat(in).as("missing golden file").isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aFLOOREncodesToTheBytesTheADRDescribes() throws Exception {
        assertThat(new RetainedFloor(STREAM, 300).encode())
                .as("⚠️ A MISMATCH IS A FORMAT CHANGE, and a consumer already deployed reads "
                        + "these bytes")
                .isEqualTo(golden());
    }

    @Test
    void theSTOREDBytesDecodeToWhatTheyMeant() throws Exception {
        assertThat(RetainedFloor.decode(golden())).isEqualTo(new RetainedFloor(STREAM, 300));
    }

    @Test
    void aFloorAtZEROAndAtLongMAXRoundTrip() throws Exception {
        // ⚠️ THE TWO ENDS OF A UVARINT, where an encoder that wrote a signed
        // varint or a fixed int would lose one of them.
        for (long floor : new long[] {0, Long.MAX_VALUE}) {
            RetainedFloor frame = new RetainedFloor(STREAM, floor);
            assertThat(RetainedFloor.decode(frame.encode())).isEqualTo(frame);
        }
    }

    @Test
    void anUNKNOWNVersionIsREFUSEDRatherThanRead() throws Exception {
        // ⚠️ `!=`, NOT `>`: version 0 is what zeroed torn bytes carry.
        for (int version : new int[] {0, 2}) {
            byte[] bytes = golden();
            bytes[7] = (byte) version;
            assertThatThrownBy(() -> RetainedFloor.decode(bytes))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("version " + version);
        }
    }

    @Test
    void aWRONGMagicIsREFUSED() throws Exception {
        byte[] bytes = golden();
        bytes[0] = 0x00;
        assertThatThrownBy(() -> RetainedFloor.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void TRAILINGBytesAreREFUSED() throws Exception {
        byte[] bytes = Arrays.copyOf(golden(), golden().length + 1);
        assertThatThrownBy(() -> RetainedFloor.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void aTRUNCATEDFrameIsREFUSED() throws Exception {
        byte[] whole = golden();
        byte[] torn = Arrays.copyOf(whole, whole.length - 1);
        assertThatThrownBy(() -> RetainedFloor.decode(torn)).isInstanceOf(IOException.class);
    }

    @Test
    void aFloorPastLONGMaxIsREFUSEDRatherThanReadAsNEGATIVE() throws Exception {
        // ⚠️ A UVARINT PAST 2^63 DECODES NEGATIVE IN A LONG, and a negative
        // floor refuses nothing -- it would read as "unknown" and hide itself.
        byte[] head = Arrays.copyOf(golden(), golden().length - 2);
        byte[] huge = new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01};
        byte[] bytes = Arrays.copyOf(head, head.length + huge.length);
        System.arraycopy(huge, 0, bytes, head.length, huge.length);
        assertThatThrownBy(() -> RetainedFloor.decode(bytes)).isInstanceOf(IOException.class);
    }

    @Test
    void theMAGICTellsAFloorFromAnEVENTWithoutDecodingEither() throws Exception {
        // ⚠️ THE POLL ANSWER IS NOW MIXED, and a reader must dispatch by magic
        // rather than try one decoder and catch the other's refusal -- which
        // would read a torn event as "not a floor" and move on.
        assertThat(RetainedFloor.isRetainedFloor(golden())).isTrue();
        byte[] event = new byte[] {0x42, 0x53, 0x55, 0x42, 0, 0, 0, 1};
        assertThat(RetainedFloor.isRetainedFloor(event)).isFalse();
        assertThat(RetainedFloor.isRetainedFloor(new byte[] {0x42, 0x50})).isFalse();
    }

    @Test
    void aNEGATIVEFloorCannotBeConstructed() {
        assertThatThrownBy(() -> new RetainedFloor(STREAM, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPARTITIONPastIntMAXIsREFUSEDRatherThanTRUNCATED() throws Exception {
        // ⚠️ A CAST WOULD WRAP IT, and a floor for partition 2^31 would be
        // applied to whatever partition the low 32 bits name -- a refusal for
        // a stream that did nothing to earn it.
        byte[] head = java.util.Arrays.copyOf(golden(), 24);
        byte[] partition = new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x08};
        byte[] floor = new byte[] {0x01};
        byte[] bytes = new byte[head.length + partition.length + floor.length];
        System.arraycopy(head, 0, bytes, 0, head.length);
        System.arraycopy(partition, 0, bytes, head.length, partition.length);
        System.arraycopy(floor, 0, bytes, head.length + partition.length, floor.length);

        assertThatThrownBy(() -> RetainedFloor.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition");
    }
}
