// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <p>⚠️ M1 wrote the literal tag {@code N} for the filter; M2.6 fills the slot
 * with {@link MembershipFilter#encode()} of whatever the writer actually
 * computed (ADR-0003) — the canonical constructor takes it explicitly, and the
 * legacy 5-arg constructor defaults to {@code N} for every existing call site
 * that predates the filter (record IDs, timestamps, header lengths) and does
 * not care what it is.
 */
public record SegmentKey(
        String prefix, long timestampMillis, String podShortId, long sequence, int headerLen,
        String filterEncoded) {

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
        Objects.requireNonNull(filterEncoded, "filterEncoded");
        if (podShortId.isBlank()) {
            throw new IllegalArgumentException("podShortId is never blank");
        }
        // ⚠️ round-1 review (M2.6): without this, a podShortId that happens to
        // contain a hyphen could itself produce a spurious match of
        // HEADER_LEN_MARKER (e.g. "pod-0123456789abcdef-h5" reproduces the
        // exact hazard this task exists to close, just through a different
        // field than the filter). Excluding '-' and '/' from podShortId is
        // what makes "the leftmost match in the tail is always the real one"
        // (see headerLenOf's own javadoc) actually true, not merely assumed.
        // ULIDs (Crockford base32) and most pod-id schemes already satisfy
        // this; it costs nothing a real caller was relying on.
        if (podShortId.indexOf('-') >= 0 || podShortId.indexOf('/') >= 0) {
            throw new IllegalArgumentException(
                    "podShortId may not contain '-' or '/': " + podShortId);
        }
        if (headerLen < 0) {
            throw new IllegalArgumentException("headerLen is never negative: " + headerLen);
        }
        if (timestampMillis < 0) {
            throw new IllegalArgumentException("timestampMillis is never negative");
        }
        // ⚠️ Refused HERE, at construction, rather than only discovered later at
        // key() or by a reader: an unparsable filter string would otherwise
        // become part of a segment's key with no reader ever able to say why.
        try {
            MembershipFilter.decode(filterEncoded);
        } catch (IOException e) {
            throw new IllegalArgumentException("not a valid membership filter: " + filterEncoded, e);
        }
    }

    /**
     * ⚠️ Every pre-M2.6 call site names timestamps, pods, sequences and header
     * lengths, and does not care what the filter is -- this defaults it to
     * {@code N}, matching M1's own hardcoded behaviour, so none of them needed
     * to change for the filter to stop being a literal.
     */
    public SegmentKey(String prefix, long timestampMillis, String podShortId, long sequence,
            int headerLen) {
        this(prefix, timestampMillis, podShortId, sequence, headerLen, new MembershipFilter.None().encode());
    }

    /** The full object key. */
    public String key() {
        String rendered = render(prefix, timestampMillis, podShortId, sequence, headerLen, filterEncoded);
        if (rendered.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalStateException("segment key exceeds " + MAX_KEY_BYTES + " bytes");
        }
        return rendered;
    }

    /**
     * How many bytes are left for the FILTER component of a key built from
     * these other fields, before {@link #MAX_KEY_BYTES} is reached.
     *
     * <p>⚠️ {@code FilterCandidates} must budget against THIS, computed per
     * call from the fields a real key will actually use, never a fixed
     * guess: prefix and podShortId length vary by deployment, and a filter
     * that individually fits some assumed-fixed budget can still leave the
     * WHOLE key over the cap once combined with a longer-than-assumed
     * prefix or pod id. Found at M2.8 (round-1 test review): reproduced
     * with the real, unmodified pre-fix code at a plain ~1,120-distinct-index
     * segment -- nowhere near this project's 10,000-index pathological
     * scale -- where a Bloom filter correctly measured at 897 bytes against
     * a hardcoded 900-byte assumption left {@link #key()} throwing once
     * combined with a realistic prefix and pod id.
     *
     * @return bytes free for {@code filterEncoded}; a non-positive result
     *     means nothing fits and the caller must degrade to {@code N}
     */
    public static int filterBudgetBytes(String prefix, long timestampMillis, String podShortId,
            long sequence, int headerLen) {
        // ⚠️ Renders the SAME template key() renders, with an empty filter
        // slot, so this can never drift from what key() will actually do --
        // a second, hand-derived formula for "the fixed part's length"
        // would be exactly the kind of assumption this method exists to
        // replace.
        String withoutFilter = render(prefix, timestampMillis, podShortId, sequence, headerLen, "");
        int fixedBytes = withoutFilter.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return MAX_KEY_BYTES - fixedBytes;
    }

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
    private static String render(String prefix, long timestampMillis, String podShortId,
            long sequence, int headerLen, String filterEncoded) {
        return String.format(java.util.Locale.ROOT,
                "%s/data/%s/%019d-%s-%016x-h%d-%s.bseg",
                prefix, PATH.format(Instant.ofEpochMilli(timestampMillis)),
                timestampMillis, podShortId, sequence, headerLen, filterEncoded);
    }

    /** The prefix a recovery LIST walks for one hour. */
    public static String hourPrefix(String prefix, long timestampMillis) {
        return String.format(java.util.Locale.ROOT, "%s/data/%s/", prefix,
                PATH.format(Instant.ofEpochMilli(timestampMillis)));
    }

    /**
     * ⚠️ MATCHED FROM A BOUNDED WINDOW, not {@code lastIndexOf("-h")} over the
     * whole key. Base64url's alphabet legally contains {@code -} (design doc
     * 02 §7), so once the filter component can be anything other than the
     * literal {@code N}, a filter payload containing the two characters
     * {@code -h} would make a naive last-match search find THAT occurrence
     * instead of the real header-length marker.
     *
     * <p>⚠️ TWO GUARDS, not one, are what make "the leftmost match in the
     * TAIL is always the real one" true rather than merely likely:
     * <ol>
     *   <li>{@link #headerLenOf} searches only the substring AFTER the key's
     *       LAST {@code /} — this excludes {@code prefix} and the date-path
     *       digits entirely. None of {@code timestampMillis}, {@code
     *       podShortId} (once restricted, below), {@code sequence}, {@code
     *       headerLen} or {@link MembershipFilter#encode()}'s base64url
     *       payload can ever contain {@code /}, so the tail is exactly
     *       {@code <ts>-<podShortId>-<seq>-h<headerLen>-<filter>};
     *   <li>the canonical constructor refuses a {@code podShortId} containing
     *       {@code -}. Round-1 review (M2.6) found that WITHOUT this, a
     *       podShortId could itself produce a spurious match inside the tail
     *       (e.g. {@code "pod-0123456789abcdef-h5"} reproduces the exact
     *       hazard this task exists to close, through a different field than
     *       the filter) — the marker being anchored on the 16-hex-digit
     *       {@code sequence} field alone was not sufficient by itself.
     * </ol>
     * With both, the tail's only unconstrained content is the filter
     * payload itself, which comes AFTER the real marker, never before it.
     */
    private static final Pattern HEADER_LEN_MARKER = Pattern.compile("-[0-9a-f]{16}-h(\\d+)-");

    /**
     * The header length a reader can extract from a key without reading the
     * object, so its first GET covers preamble and directory exactly.
     */
    public static int headerLenOf(String key) {
        int tailStart = key.lastIndexOf('/') + 1;
        Matcher m = HEADER_LEN_MARKER.matcher(key).region(tailStart, key.length());
        if (!m.find()) {
            throw new IllegalArgumentException("not a segment key: " + key);
        }
        return Integer.parseInt(m.group(1));
    }
}
