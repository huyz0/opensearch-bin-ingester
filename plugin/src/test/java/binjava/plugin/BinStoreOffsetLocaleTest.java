// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * A pointer's string form is a wire value: OpenSearch persists it as
 * {@code batch_start} and parses it back on the next restart, possibly in a
 * different process with a different default locale.
 */
class BinStoreOffsetLocaleTest {

    /**
     * WARNING: this is a real defect found in T4, not a hypothetical. The
     * OpenSearch test framework randomizes the default locale; under one with
     * non-ASCII digits the poller logged
     * {@code seeking to earliest pointer ۰۰۰۰۰۰۰۰۰۰۰۰۰۰۰۰۰۰۰}. Those digits
     * round-trip through nothing.
     */
    @Test
    void theStringFormUsesAsciiDigitsUnderAnyDefaultLocale() {
        Locale original = Locale.getDefault();
        try {
            // Arabic-Indic digits. `ar` alone is not enough on every JDK --
            // the -u-nu-arab extension asks for the numbering system directly.
            Locale.setDefault(Locale.forLanguageTag("ar-EG-u-nu-arab"));
            String rendered = new BinStoreOffset(1234L).asString();

            assertThat(rendered).isEqualTo("0000000000000001234");
            assertThat(BinStoreOffset.fromString(rendered)).isEqualTo(new BinStoreOffset(1234L));
        } finally {
            Locale.setDefault(original);
        }
    }
}
