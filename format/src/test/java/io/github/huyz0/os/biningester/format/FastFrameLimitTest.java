// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * The 1024-byte string limit on both sides (M13.26d review round 1, P2, T5).
 */
class FastFrameLimitTest {

    @Test
    void aJOINWhoseIncarnationExceedsTheLimitIsRefusedAtConstruction() {
        Roster.Incarnation wide = new Roster.Incarnation("p", "uid", "az",
                "e".repeat(FastFrame.MAX_STRING_BYTES + 1));

        assertThatThrownBy(() -> new FastFrame.Join(wide, FastFrame.Held.NONE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDECODEDStringOverTheLimitIsRefused() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(14).putInt(FastFrame.MAGIC).put((byte) 1)
                .put((byte) FastFrame.KIND_REFUSED).putLong(1).array());
        int n = FastFrame.MAX_STRING_BYTES + 1;
        SegmentWriter.putUvarint(out, n);
        out.writeBytes("u".repeat(n).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        SegmentWriter.putUvarint(out, 1);
        out.write('t');
        out.write(1);
        SegmentWriter.putUvarint(out, 0);

        assertThatThrownBy(() -> FastFrame.decode(out.toByteArray()))
                .isInstanceOf(IOException.class).hasMessageContaining("string");
    }
}
