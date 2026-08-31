// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Where a segment lives, and what a reader can learn before reading it
 * (docs/research/30-design-space/01-object-layout-and-format.md §5).
 *
 * <pre>
 * &lt;prefix&gt;/data/&lt;yyyy&gt;/&lt;MM&gt;/&lt;dd&gt;/&lt;HH&gt;/&lt;ts&gt;-&lt;pod&gt;-&lt;seq&gt;-h&lt;headerLen&gt;-&lt;filter&gt;.bseg
 * </pre>
 *
 * <p>⚠️ {@code h<headerLen>} IS IN THE KEY ON PURPOSE, and it is a strict
 * improvement on AutoMQ's speculative tail read: AutoMQ must GUESS the index
 * size and occasionally pays a second GET. Writing the number in the key means
 * one {@code GET Range: bytes=0-(32+headerLen-1)} retrieves preamble AND
 * directory, always, with no guess and no retry. It costs nothing and removes a
 * whole failure mode — a request the cost model would otherwise have to budget.
 *
 * <p>⚠️ The time path bounds a recovery LIST to a window via {@code start-after},
 * which is what keeps LIST off every path except recovery and GC (rule R2). The
 * millisecond timestamp makes the key lexicographically ≈ chronological within
 * the hour, so that walk is ordered without sorting.
 *
 * <p>⚠️ M1 writes tag {@code N} for the filter. The membership filter is ADR-0003
 * and lands in M2; the SLOT is here from the first key so that adding it later
 * is not a key-grammar change. // SKELETON: filter is N until M2
 */
public record SegmentKey(
        String prefix, long timestampMillis, String podShortId, long sequence, int headerLen) {

    private static final DateTimeFormatter PATH =
            // ⚠️ withLocale(ROOT) is BELT-AND-BRACES here, not a fixed defect.
            // A first version of this comment claimed ofPattern() takes its
            // DecimalStyle from the default locale; that is FALSE and was
            // measured false on JDK 25 -- toFormatter() hardcodes
            // DecimalStyle.STANDARD, so under ar-EG-u-nu-arab this pattern still
            // renders 2021/01/01/00. The locale selects TEXT (month names, era),
            // never digits, so ROOT only starts mattering if the pattern ever
            // gains a text field such as MMM. The real digit sinks in this file
            // are the %019d and h%d conversions below, which String.format DOES
            // localize. Recording the difference because the mirror-image error
            // -- auditing DateTimeFormatter and missing a decimal
            // String.format -- is exactly the bug this file just paid for.
            // ZoneOffset.UTC is pre-existing and unrelated to locale.
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
                    .withZone(ZoneOffset.UTC)
                    .withLocale(java.util.Locale.ROOT);

    /** ⚠️ S3, GCS and Azure all cap a key at 1024 bytes. */
    public static final int MAX_KEY_BYTES = 1024;

    public SegmentKey {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(podShortId, "podShortId");
        if (podShortId.isBlank()) {
            throw new IllegalArgumentException("podShortId is never blank");
        }
        if (headerLen < 0) {
            throw new IllegalArgumentException("headerLen is never negative: " + headerLen);
        }
        if (timestampMillis < 0) {
            throw new IllegalArgumentException("timestampMillis is never negative");
        }
    }

    /** The full object key. */
    public String key() {
        // ⚠️ Zero-padded to 19 digits -- the width of Long.MAX_VALUE -- so the
        // millisecond stamp sorts lexicographically for ANY value a long can
        // hold. Unpadded, "9999" sorts after "10000" and a recovery walk visits
        // the hour out of order, silently skipping objects on a start-after
        // resume. ⚠️ 13 was tried first and is WRONG: it pads today's 13-digit
        // values, so every realistic fixture passes, while a 14-digit timestamp
        // (from 2286) is not padded at all and sorts before every key written
        // before it. Six extra bytes against a 1024-byte budget whose fixed part
        // is ~90.
        // ⚠️ Locale.ROOT: String.formatted() uses the default locale, so under
        // a locale with non-ASCII digits every key would render with digits no
        // other process could parse -- and object keys outlive the process.
        String rendered = String.format(java.util.Locale.ROOT,
                "%s/data/%s/%019d-%s-%016x-h%d-N.bseg",
                prefix, PATH.format(Instant.ofEpochMilli(timestampMillis)),
                timestampMillis, podShortId, sequence, headerLen);
        if (rendered.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalStateException("segment key exceeds " + MAX_KEY_BYTES + " bytes");
        }
        return rendered;
    }

    /** The prefix a recovery LIST walks for one hour. */
    public static String hourPrefix(String prefix, long timestampMillis) {
        return String.format(java.util.Locale.ROOT, "%s/data/%s/", prefix,
                PATH.format(Instant.ofEpochMilli(timestampMillis)));
    }

    /**
     * The header length a reader can extract from a key without reading the
     * object, so its first GET covers preamble and directory exactly.
     */
    public static int headerLenOf(String key) {
        int h = key.lastIndexOf("-h");
        int dash = key.indexOf('-', h + 2);
        if (h < 0 || dash < 0) {
            throw new IllegalArgumentException("not a segment key: " + key);
        }
        return Integer.parseInt(key, h + 2, dash, 10);
    }
}
