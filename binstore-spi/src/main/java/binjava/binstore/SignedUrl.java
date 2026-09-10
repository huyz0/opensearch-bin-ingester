// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.time.Instant;

/**
 * A short-lived URL that reads one object without credentials (M5.10).
 *
 * <p>⚠️ IT IS A TYPE SO THAT IT CANNOT BE LOGGED BY ACCIDENT. security.md rule
 * 4 says a signed URL never reaches a log, a trace or an error message, and a
 * returned {@code String} makes that a rule every future caller has to
 * remember -- non-negotiable 9 puts "remember to" at the weakest rung there is.
 * {@link #toString} redacts, so the common ways a secret escapes -- string
 * concatenation into a log line, an exception message, a record's generated
 * {@code toString} that contains one -- all yield the redaction instead.
 *
 * <p>⚠️ {@link #url()} IS THE WAY OUT, and it is NOT awkward -- it is a record
 * accessor shorter to type than {@code toString()}. An earlier version of this
 * comment claimed otherwise, which asserted a safeguard that does not exist.
 * What the type buys is that the ACCIDENTAL paths redact; a caller reaching for
 * {@code url()} has chosen to handle the secret.
 *
 * <p>⚠️ TWO LEAKS THIS TYPE DOES NOT CLOSE, and both are outside
 * {@code toString}. A serializer reading record ACCESSORS emits the component
 * -- and the component is named {@code url}, so it appears under that name.
 * ⚠️ THIS CLASSPATH HAS NO SUCH SERIALIZER: no Jackson, JSON-B or Gson, and
 * every logger is {@code System.Logger}, which formats through
 * {@code toString}. An earlier draft named {@code XContentBuilder} in the
 * PLUGIN module as a live leak; that was wrong in a way worth recording,
 * because this type cannot reach {@code plugin} at all (see below), so the
 * obligation it pointed at was aimed at the wrong module. What M5.14 actually
 * inherits is stated below: the value crossing to the consumer is a String.
 * And AssertJ's opt-in field-printing assertions print components into a
 * CI-visible failure message.
 *
 * <p>⚠️ IT ALSO CANNOT REACH THE CONSUMER. This type lives in
 * {@code binstore-spi}, which ADR-0023 deliberately keeps out of {@code client}
 * and {@code plugin}, so the value crossing to the consumer at M5.14 is a
 * String again and this guarantee does not travel with it.
 */
public record SignedUrl(String url, Instant expiresAt) {

    public SignedUrl {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("a signed URL needs a url");
        }
        if (expiresAt == null) {
            throw new IllegalArgumentException("a signed URL needs an expiry -- ADR-0041 "
                    + "requires a short TTL, and one that is absent is not short");
        }
    }

    /**
     * ⚠️ REDACTED, ON PURPOSE. The expiry is safe to show and is what an
     * operator actually needs when a fetch fails: whether the grant had run
     * out.
     */
    @Override
    public String toString() {
        return "SignedUrl[redacted, expiresAt=" + expiresAt + "]";
    }
}
