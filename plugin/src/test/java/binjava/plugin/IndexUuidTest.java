// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * WARNING: an OpenSearch index UUID is BASE64URL of 16 bytes, not the hyphenated
 * form. {@code UUID.fromString} threw on every real index --
 * "Invalid UUID string: nVzgup36TLqWp7VBBREj1w" -- and NO in-process test could
 * have caught it, because they all build a RunKey from a java.util.UUID
 * directly. Only a booting node hands over a real one.
 */
class IndexUuidTest {

    @Test
    void aBase64UrlIndexUuidDecodesToTheSameSixteenBytes() {
        UUID original = UUID.fromString("9d5ce0ba-9dfa-4cba-96a7-b541051123d7");
        byte[] raw = ByteBuffer.allocate(16)
                .putLong(original.getMostSignificantBits())
                .putLong(original.getLeastSignificantBits())
                .array();
        String openSearchForm = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);

        assertThat(openSearchForm).as("22 unpadded base64url characters").hasSize(22);
        assertThat(BinStoreConsumerFactory.indexUuidOf(openSearchForm)).isEqualTo(original);
    }

    @Test
    void theHyphenatedFormIsNotAnIndexUuid() {
        // WARNING: this is the shape that used to be passed in. It must fail
        // loudly rather than decode to something plausible.
        assertThatThrownBy(() ->
                BinStoreConsumerFactory.indexUuidOf("9d5ce0ba-9dfa-4cba-96a7-b541051123d7"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void somethingThatIsNotSixteenBytesIsRefused() {
        String eightBytes = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(new byte[8]);
        assertThatThrownBy(() -> BinStoreConsumerFactory.indexUuidOf(eightBytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16 bytes");
    }

    @Test
    void twoDifferentIndexUuidsMapToDifferentStreams() {
        String a = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(ByteBuffer.allocate(16).putLong(1).putLong(2).array());
        String b = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(ByteBuffer.allocate(16).putLong(1).putLong(3).array());
        // WARNING: an index deleted and recreated with the same NAME gets a new
        // UUID and is a DIFFERENT stream. Collapsing them would resume the new
        // index from the old one's offsets.
        assertThat(BinStoreConsumerFactory.indexUuidOf(a))
                .isNotEqualTo(BinStoreConsumerFactory.indexUuidOf(b));
    }
}
