// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * A signed URL cannot be logged by accident (M5.10, ADR-0041).
 *
 * <p>⚠️ security.md rule 4 says a signed URL never reaches a log, a trace or an
 * error message. A {@code String} return makes that a rule every caller must
 * remember, which non-negotiable 9 puts at the weakest rung; a redacting type
 * makes the leak unrepresentable through the paths secrets actually escape by.
 */
class SignedUrlTest {

    private static final Instant SOON = Instant.parse("2026-09-10T12:00:00Z");

    /**
     * The secret is not in {@code toString}, which is how it would escape.
     *
     * <p>⚠️ THREE PATHS, NOT ONE. String concatenation into a log line, an
     * exception message built from the value, and the generated
     * {@code toString} of any record or collection HOLDING one all route
     * through this method.
     */
    @Test
    void theURLIsNOTInToStringOrAnythingBuiltFromIt() {
        SignedUrl signed = new SignedUrl("https://store/seg?sig=SECRET123", SOON);

        // ⚠️ NO PART OF IT, not just the signature. Review MEASURED
        // `url.substring(0, url.indexOf('?'))` surviving a test that only
        // looked for the signature: that leaks the host and the object key,
        // which is the fetch's target and a tenant's data layout.
        assertThat(signed.toString())
                .doesNotContain("SECRET123")
                .doesNotContain("sig=")
                .doesNotContain("https://")
                .doesNotContain("store/seg");
        assertThat("fetching " + signed).doesNotContain("SECRET123");
        assertThat(new RuntimeException("failed for " + signed).getMessage())
                .doesNotContain("SECRET123");
        assertThat(java.util.List.of(signed).toString()).doesNotContain("SECRET123");
        assertThat(java.util.Map.of("u", signed).toString()).doesNotContain("SECRET123");
    }

    /** The expiry IS shown, because that is what an operator needs. */
    @Test
    void theEXPIRYIsShownBecauseAFailedFetchNeedsIt() {
        assertThat(new SignedUrl("https://store/seg?sig=x", SOON).toString())
                .contains("2026-09-10T12:00:00Z");
    }

    /** The value is reachable, deliberately and only deliberately. */
    @Test
    void theURLIsReachableForACallerThatASKSForIt() {
        assertThat(new SignedUrl("https://store/seg?sig=x", SOON).url())
                .isEqualTo("https://store/seg?sig=x");
    }

    @Test
    void aSignedUrlWITHOUTAnExpiryIsRefused() {
        assertThatThrownBy(() -> new SignedUrl("https://store/seg", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("short TTL");
    }

    @Test
    void aSignedUrlWITHOUTAUrlIsRefused() {
        assertThatThrownBy(() -> new SignedUrl(" ", SOON))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a url");
        // ⚠️ NULL AS WELL AS BLANK. Review MEASURED that dropping the
        // `url == null` half leaves this green while `new SignedUrl(null, …)`
        // throws NPE from a different line instead of the documented type.
        assertThatThrownBy(() -> new SignedUrl(null, SOON))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a url");
    }
}
