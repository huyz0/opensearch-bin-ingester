// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * The consumer's half of a {@code direct} grant, with the redaction that
 * {@code SignedUrl}'s own javadoc records as not travelling this far (M5.44).
 *
 * <p>⚠️ TWO TYPES FOR ONE CONCEPT, AND THE MODULE RULE IS WHY.
 * {@code io.github.huyz0.os.biningester.binstore.SignedUrl} is the STORE side; architecture.md rule 1
 * says {@code format} depends on nothing, and ADR-0023 keeps
 * {@code binstore-spi} out of {@code client} and {@code plugin} — so the value
 * that crosses to a consumer cannot be that type. Until this record existed it
 * crossed as a bare {@code String}, and {@code SignedUrl}'s javadoc says so in
 * as many words: "the value crossing to the consumer at M5.14 is a String again
 * and this guarantee does not travel with it".
 */
class GrantTest {

    private static final Instant SOON = Instant.parse("2026-09-13T12:00:00Z");

    @Test
    void theURLIsREDACTEDByToStringAndTheEXPIRYIsNot() {
        Grant grant = new Grant("https://store.example/seg?sig=secret", SOON);

        assertThat(grant.toString())
                .as("the secret must not reach a log line through concatenation")
                .doesNotContain("secret")
                .doesNotContain("https://")
                .contains("redacted");
        assertThat(grant.toString())
                .as("and the expiry IS what an operator needs when a fetch fails")
                .contains(SOON.toString());
    }

    @Test
    void theURLIsReachableForACallerThatASKSForIt() {
        Grant grant = new Grant("https://store.example/seg?sig=secret", SOON);

        assertThat(grant.url())
                .as("redaction guards the ACCIDENTAL paths; a fetch still needs the value")
                .isEqualTo("https://store.example/seg?sig=secret");
    }

    @Test
    void aGrantWithNoURLOrNoEXPIRYIsRefused() {
        assertThatThrownBy(() -> new Grant(null, SOON))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("url");
        assertThatThrownBy(() -> new Grant("  ", SOON))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("url");
        assertThatThrownBy(() -> new Grant("https://store.example/seg", null))
                .as("ADR-0041 requires a short TTL, and one that is absent is not short")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expiry");
    }
}
