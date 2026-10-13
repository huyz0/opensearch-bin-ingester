// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * REGISTER, writer to leader (ADR-0082 §2; M13.83a): a fast index's
 * registration, its byte length then the {@code IndexRegistration}
 * encoding of §1, so the leader's catalog -- the one its term record is fed
 * from -- learns an index the plugin registered only with another pod.
 */
class FastRegisterFrameTest {

    private static IndexRegistration walAtTwo() {
        return IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "logs", 3)
                .withFastSettings(60_000, true, 2);
    }

    @Test
    void aREGISTEREncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(4, "uid-w", "uid-l", new FastFrame.Register(walAtTwo())))
                .as("the bytes ADR-0082 §2 lays out, from an independent encoder")
                .isEqualTo(FastFrameTest.golden("fast-register-v1.bin"));
    }

    @Test
    void theGOLDENFrameDecodesToItsRegistration() throws Exception {
        FastFrame.Frame frame = FastFrame.decode(FastFrameTest.golden("fast-register-v1.bin"));

        assertThat(frame.header().kind()).isEqualTo(FastFrame.KIND_REGISTER);
        assertThat(frame.body()).isEqualTo(new FastFrame.Register(walAtTwo()));
    }

    @Test
    void aREGISTERLargerThanAControlFrameIsRefused() throws Exception {
        // ⚠️ its review round 1, P3: decoded whole, a frame's 64 MiB would be.
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            aliases.add("alias-number-" + i);
        }
        byte[] large = new IndexRegistration("nVzgup36TLqWp7VBBREj1w", "logs", aliases,
                3, 3, 1, 1).withFastSettings(60_000, true, 2).encode();
        assertThat(large.length).as("the premise").isGreaterThan(FastFrame.MAX_REGISTER_BYTES);
        byte[] header = java.util.Arrays.copyOf(FastFrameTest.golden("fast-register-v1.bin"),
                26);
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        frame.writeBytes(header);
        frame.writeBytes(java.nio.ByteBuffer.allocate(4).putInt(large.length).array());
        frame.writeBytes(large);

        assertThatThrownBy(() -> FastFrame.decode(frame.toByteArray()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void aREGISTERLargerThanAControlFrameIsNeverBuilt() {
        // ⚠️ its review round 2, T6: refused on the way out too.
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            aliases.add("alias-number-" + i);
        }
        IndexRegistration large = new IndexRegistration("nVzgup36TLqWp7VBBREj1w", "logs",
                aliases, 3, 3, 1, 1).withFastSettings(60_000, true, 2);

        assertThatThrownBy(() -> new FastFrame.Register(large))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void noKINDAboveREGISTERIsAFrame() {
        // ⚠️ its review round 1, T2.
        assertThatThrownBy(() -> new FastFrame.Header(FastFrame.KIND_REGISTER + 1, 1, "a", "b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new FastFrame.Header(FastFrame.KIND_REGISTER, 1, "a", "b").kind())
                .isEqualTo(18);
    }

    @Test
    void aREGISTERIsAControlFrame() {
        assertThat(FastFrame.isControl(FastFrame.KIND_REGISTER))
                .as("a few per index change, never per record").isTrue();
    }
}
