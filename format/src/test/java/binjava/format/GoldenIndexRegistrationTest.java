// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The bytes of an index registration, pinned (M6.2, `wire-format-change`).
 *
 * <p>⚠️ A ROUND-TRIP TEST CANNOT SEE A FORMAT CHANGE. Encode-then-decode passes
 * for any self-consistent pair of methods, including one that reorders every
 * field — the writer and the reader move together and the suite stays green
 * while every byte already in flight becomes unreadable. A stored file is the
 * only thing that notices.
 *
 * <p>⚠️ THE FILE IS THE QUESTION, NOT THE ANSWER. When this fails the fix is
 * almost never to regenerate it: it is whether every reader, every writer and
 * every fake moved in the same commit, which is the rule
 * `wire-format-change` exists to enforce and which the golden file is the only
 * mechanical part of.
 *
 * <p>⚠️ TWO SHAPES, because the split fields are where a reordering hides: an
 * unsplit index has {@code routingNumShards == numShards} and
 * {@code routingFactor == 1}, so swapping those two fields in the encoder
 * changes nothing a single-shape fixture could see.
 */
class GoldenIndexRegistrationTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private static byte[] golden(String name) throws IOException {
        try (var in = GoldenIndexRegistrationTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static IndexRegistration split() {
        return new IndexRegistration(UUID, "logs-000002", List.of("logs", "logs-write"),
                8, 32, 4, 1);
    }

    @Test
    void anUNSPLITRegistrationEncodesToItsStoredBytes() throws Exception {
        assertThat(IndexRegistration.unsplit(UUID, "logs", 3).encode())
                .as("the bytes a live writer produces are the bytes already stored -- a "
                        + "mismatch is a format change, and the question is whether every "
                        + "reader moved with it")
                .isEqualTo(golden("index-registration-v1.bin"));
    }

    @Test
    void aSPLITRegistrationEncodesToItsStoredBytes() throws Exception {
        assertThat(split().encode())
                .as("the split fields are where a reordering hides: with numShards == "
                        + "routingNumShards and a factor of 1, swapping them is invisible")
                .isEqualTo(golden("index-registration-split-v1.bin"));
    }

    @Test
    void theSTOREDBytesDecodeToWhatWroteThem() throws Exception {
        assertThat(IndexRegistration.decode(golden("index-registration-split-v1.bin")))
                .as("and the READ side of the pair: bytes written by an older build of this "
                        + "format are still understood by this one")
                .isEqualTo(split());
    }

    @Test
    void theSTOREDBytesCarryTheMAGICAndVERSIONAnOperatorCanSee() throws Exception {
        byte[] stored = golden("index-registration-v1.bin");

        assertThat(java.nio.ByteBuffer.wrap(stored).getInt(0))
                .as("`BIRG` in a hex dump, so an operator holding an unknown frame can tell "
                        + "what it is without the code")
                .isEqualTo(IndexRegistration.MAGIC);
        assertThat(java.nio.ByteBuffer.wrap(stored).getInt(4)).isEqualTo(IndexRegistration.VERSION_1);
    }
}
