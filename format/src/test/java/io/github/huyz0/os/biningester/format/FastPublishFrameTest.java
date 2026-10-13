// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Publication's frames (ADR-0082 §2 kinds 7 and 8; ADR-0081 §6; M13.30c):
 * PUBLISH and INTEREST against goldens from an independent encoder
 * (golden_publish.py), and the frames each refuses.
 */
class FastPublishFrameTest {

    private static final RunKey AA = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 3);
    private static final RunKey BB = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000bb"), 0);

    private static FastJournalRecord.Entry entry() throws IOException {
        return FastWriteFrame.entry(FastFrameTest.golden("fast-journal-entry-v1.bin"));
    }

    @Test
    void aPUBLISHEncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(4, "uid-l", "uid-p",
                new FastPublishFrame.Publish(List.of(entry()))))
                .isEqualTo(FastFrameTest.golden("fast-publish-v1.bin"));
    }

    @Test
    void anINTERESTEncodesToItsGoldenBytes() {
        assertThat(FastFrame.encode(4, "uid-p", "uid-l",
                new FastPublishFrame.Interest(List.of(AA), List.of(BB))))
                .isEqualTo(goldenOrFail("fast-interest-v1.bin"));
    }

    @Test
    void theGOLDENFramesDecodeToTheirBodies() throws Exception {
        FastFrame.Frame publish = FastFrame.decode(FastFrameTest.golden("fast-publish-v1.bin"));
        FastFrame.Frame interest = FastFrame.decode(FastFrameTest.golden("fast-interest-v1.bin"));

        assertThat(publish.header().kind()).isEqualTo(FastPublishFrame.KIND_PUBLISH);
        // ⚠️ BY ENCODING: a record's byte[] payload compares by identity
        assertThat(((FastPublishFrame.Publish) publish.body()).entries())
                .singleElement().satisfies(e -> assertThat(e.encode())
                        .isEqualTo(entry().encode()));
        assertThat(interest.header().kind()).isEqualTo(FastPublishFrame.KIND_INTEREST);
        assertThat(interest.body())
                .isEqualTo(new FastPublishFrame.Interest(List.of(AA), List.of(BB)));
    }

    @Test
    void anEMPTYPublishOrInterestAndAStreamNamedTwiceAreNoFrame() {
        assertThatThrownBy(() -> new FastPublishFrame.Publish(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastPublishFrame.Interest(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastPublishFrame.Interest(List.of(AA, AA), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastPublishFrame.Interest(List.of(AA), List.of(AA)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anINTERESTNamingAStreamTwiceOnTheWireIsAnIOException() throws Exception {
        byte[] bytes = FastFrame.encode(4, "uid-p", "uid-l",
                new FastPublishFrame.Interest(List.of(AA), List.of(BB)));
        byte[] twice = bytes.clone();
        // the removed key, rewritten to the added one's partition: AA in both lists
        System.arraycopy(bytes, bytes.length - 44, twice, bytes.length - 20, 20);

        assertThatThrownBy(() -> FastFrame.decode(twice)).isInstanceOf(IOException.class);
    }

    private static byte[] goldenOrFail(String name) {
        try {
            return FastFrameTest.golden(name);
        } catch (IOException missing) {
            throw new AssertionError(missing);
        }
    }
}
