// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Joining OpenSearch's index uuid to this project's stream id (M7.2).
 *
 * <p>⚠️ ONE RULE, ONE PLACE. The plugin derives a stream from the index uuid
 * and the ingester now has to derive the same stream from the uuid a progress
 * frame carries. Two implementations of this mapping that disagree do not
 * throw: they produce two different streams for one index, and the watermark
 * for the stream nobody reports on never moves while the one being reported on
 * belongs to nothing.
 */
class RunKeyIndexUuidTest {

    /** A real one, base64url, from a booting node. */
    private static final String REAL = "nVzgup36TLqWp7VBBREj1w";

    @Test
    void theSTREAMIdIsTheSIXTEENBytesTheUuidDecodesTo() {
        assertThatThrownBy(() -> UUID.fromString(REAL))
                .as("the trap this method exists for, asserted beside the rule rather "
                        + "than in a case of its own: UUID.fromString throws on every real "
                        + "index uuid, and no in-process test caught it because they all "
                        + "built a java.util.UUID directly. ⚠️ On its own it is a pin of a "
                        + "JDK fact, which no red run can ever produce")
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(RunKey.ofIndexUuid(REAL, 3))
                .as("⚠️ A LITERAL, NOT A RE-DERIVATION. Recomputing the production "
                        + "algorithm on the fixture side passes for any self-consistent "
                        + "pair, including one that swaps the two halves -- and this id is "
                        + "what a subscription is keyed by, so a change here silently "
                        + "moves every stream")
                .isEqualTo(new RunKey(
                        UUID.fromString("9d5ce0ba-9dfa-4cba-96a7-b541051123d7"), 3));
        byte[] raw = Base64.getUrlDecoder().decode(REAL);
        ByteBuffer b = ByteBuffer.wrap(raw);
        assertThat(RunKey.ofIndexUuid(REAL, 3))
                .isEqualTo(new RunKey(new UUID(b.getLong(), b.getLong()), 3));
    }

    @Test
    void theSAMEUuidAlwaysGivesTheSAMEStream() {
        assertThat(RunKey.ofIndexUuid(REAL, 3)).isEqualTo(RunKey.ofIndexUuid(REAL, 3));
    }

    @Test
    void aDIFFERENTUuidGivesADIFFERENTStream() {
        assertThat(RunKey.ofIndexUuid(REAL, 3))
                .isNotEqualTo(RunKey.ofIndexUuid("8Gk1lQ2HRs-TvA4pZ0bXyQ", 3));
    }

    @Test
    void aUuidThatDecodesToTheWRONGLengthIsREFUSED() {
        assertThatThrownBy(() -> RunKey.ofIndexUuid("c2hvcnQ", 0))
                .as("an index uuid decodes to exactly 16 bytes; anything else is not one, "
                        + "and truncating it silently would map two indices to one stream")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16");
    }

    @Test
    void aUuidThatIsNotBASE64URLIsREFUSED() {
        assertThatThrownBy(() -> RunKey.ofIndexUuid("not base64url!!", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNEGATIVEPartitionIsREFUSED() {
        assertThatThrownBy(() -> RunKey.ofIndexUuid(REAL, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
