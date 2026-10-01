// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A fast frame this build cannot place is an {@link IOException}, never a
 * guess (ADR-0082 §2; M13.26d).
 */
class FastFrameStrictTest {

    private static void refused(byte[] bytes) {
        assertThatThrownBy(() -> FastFrame.decode(bytes)).isInstanceOf(IOException.class);
    }

    @Test
    void anUNREADKindIsRefused() throws Exception {
        byte[] commit = FastFrameTest.golden("fast-refused-v1.bin").clone();
        commit[5] = 1;

        refused(commit);
    }

    @Test
    void aWRONGMagicOrVersionIsRefusedAtTheHeader() throws Exception {
        byte[] magic = FastFrameTest.golden("fast-refused-v1.bin").clone();
        magic[0] = 0;
        byte[] version = FastFrameTest.golden("fast-refused-v1.bin").clone();
        version[4] = 2;

        assertThatThrownBy(() -> FastFrame.header(magic)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> FastFrame.header(version)).isInstanceOf(IOException.class);
    }

    @Test
    void TRAILINGBytesAreRefused() throws Exception {
        byte[] golden = FastFrameTest.golden("fast-refused-v1.bin");

        refused(Arrays.copyOf(golden, golden.length + 1));
    }

    @Test
    void aCOUNTPastTheBytesIsRefused() throws Exception {
        byte[] joined = FastFrameTest.golden("fast-joined-v1.bin").clone();
        int at = 6 + 8 + 6 + 6 + 8;
        ByteBuffer.wrap(joined).putInt(at, 0x7FFFFFFF);

        assertThatThrownBy(() -> FastFrame.decode(joined)).isInstanceOf(IOException.class)
                .as("refused at the count, before any element is read")
                .hasMessageContaining("elements");
    }

    @Test
    void anUNKNOWNStatusOrReasonIsRefused() throws Exception {
        byte[] joined = FastFrameTest.golden("fast-joined-v1.bin").clone();
        joined[joined.length - 1] = 9;
        byte[] plain = FastFrameTest.golden("fast-refused-v1.bin").clone();
        plain[6 + 8 + 6 + 6] = 9;

        refused(joined);
        refused(plain);
    }

    @Test
    void aDISCARDNamesItsKeyAndNothingElseDoes() {
        assertThatThrownBy(() -> new FastFrame.Refused(FastFrame.Reason.DISCARDED,
                Optional.empty(), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastFrame.Refused(FastFrame.Reason.BACKPRESSURE,
                Optional.of(new FastJournalRecord.IdempotencyKey("p", new java.util.UUID(1, 1), 1)),
                "")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anOVERLONGStringIsRefused() {
        String long1025 = "u".repeat(FastFrame.MAX_STRING_BYTES + 1);

        assertThatThrownBy(() -> FastFrame.encode(1, long1025, "t",
                new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(), "")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
