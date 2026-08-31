// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * An object key outlives the process that wrote it and is read by processes
 * that never shared its locale.
 */
class SegmentKeyLocaleTest {

    /**
     * WARNING: found in T4, where the OpenSearch test framework randomizes the
     * default locale.
     *
     * <p>⚠️ What this KILLS is the {@code %019d} and {@code h%d} conversions.
     * It does NOT constrain the {@code withLocale(ROOT)} on the
     * DateTimeFormatter, and saying so matters: {@code DecimalStyle} is
     * {@code STANDARD} regardless of locale, so deleting that call leaves this
     * test green — because the call was never load-bearing, not because the
     * test is weak. {@code hourPrefix} is likewise unconstrained here: both its
     * conversions are {@code %s}, which is never localized.
     */
    @Test
    void everyKeyUsesAsciiDigitsUnderAnyDefaultLocale() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("ar-EG-u-nu-arab"));
            // 2021-01-01T00:00:00Z
            String key = new SegmentKey("p", 1609459200000L, "pod", 5L, 96).key();

            assertThat(key).isEqualTo("p/data/2021/01/01/00/0000001609459200000-pod-"
                    + "0000000000000005-h96-N.bseg");
            assertThat(SegmentKey.hourPrefix("p", 1609459200000L))
                    .isEqualTo("p/data/2021/01/01/00/");
            // The load-bearing property, stated independently of the exact key:
            // no key byte is outside the ASCII range a start-after walk assumes.
            assertThat(key.chars().allMatch(c -> c < 128)).isTrue();
        } finally {
            Locale.setDefault(original);
        }
    }
}
