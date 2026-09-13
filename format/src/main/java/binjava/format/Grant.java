// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.time.Instant;

/**
 * A short-lived URL a CONSUMER fetches one segment with (ADR-0041, M5.44).
 *
 * <p>⚠️ IT IS A SECOND TYPE FOR ONE CONCEPT, AND THE MODULE RULE IS WHY.
 * {@code binjava.binstore.SignedUrl} is the store side of the same idea, and it
 * cannot be this one: architecture.md rule 1 says {@code format} depends on
 * NOTHING, and ADR-0023 keeps {@code binstore-spi} out of {@code client} and
 * {@code plugin}. {@code SignedUrl}'s own javadoc already records the
 * consequence -- "the value crossing to the consumer at M5.14 is a String again
 * and this guarantee does not travel with it" -- so what this record restores is
 * the guarantee, not the value.
 *
 * <p>⚠️ WHICH GUARANTEE: {@link #toString} REDACTS. security.md rule 4 says a
 * signed URL never reaches a log, a trace or an error message, and a bare
 * {@code String} makes that a rule every future caller has to remember --
 * non-negotiable 9 puts "remember to" at the weakest rung there is. The
 * accidental paths -- concatenation into a log line, an exception message, a
 * record's generated {@code toString} that contains one -- all yield the
 * redaction. {@link #url()} is the way out, and a caller reaching for it has
 * chosen to handle the secret.
 *
 * <p>⚠️ THE EXPIRY IS SAFE TO SHOW and is what an operator needs when a fetch
 * fails: whether the grant had run out.
 *
 * <p>⚠️ AND IT IS TRUNCATED TO MILLISECONDS, so {@link #expiresAt()} may differ
 * from the {@code Instant} handed in. {@code SubscriptionEvent} encodes it as
 * epoch millis, and once that event's {@code equals} compares the grant, a
 * value carrying micros -- which is what {@code clock.instant().plus(ttl)}
 * returns on Linux -- would not survive a round trip. Truncating here makes the
 * in-memory value the one that travels. ⚠️ M5.45d's adapter from
 * {@code binjava.binstore.SignedUrl} is where that difference will first be
 * compared for equality.
 */
public record Grant(String url, Instant expiresAt) {

    public Grant {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("a grant needs a url");
        }
        if (expiresAt == null) {
            throw new IllegalArgumentException("a grant needs an expiry -- ADR-0041 requires a "
                    + "short TTL, and one that is absent is not short");
        }
        // ⚠️ MILLISECOND PRECISION, TRUNCATED HERE RATHER THAN LOST ON THE
        // WIRE. The event encodes this as epoch millis, so an `Instant` with
        // micros or nanos -- which is what `clock.instant().plus(ttl)` returns
        // on Linux -- would not survive `decode(encode(e)).equals(e)` once
        // `SubscriptionEvent.equals` compares the grant. Review measured it:
        // `Instant.ofEpochSecond(1000, 123456789)` came back as `1000.123Z`
        // and the round trip was false. Truncating at construction makes the
        // in-memory value the one that travels, so the property is total
        // rather than true-for-the-fixtures-we-happened-to-write.
        expiresAt = expiresAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        // ⚠️ AND BOUNDED, because `Instant.MAX.toEpochMilli()` throws
        // `ArithmeticException: long overflow` -- an encode that dies on a
        // value the constructor accepted.
        long millis;
        try {
            millis = expiresAt.toEpochMilli();
        } catch (ArithmeticException tooFarOut) {
            throw new IllegalArgumentException(
                    "a grant expiry must fit epoch milliseconds, got " + expiresAt);
        }
        if (millis < 0) {
            throw new IllegalArgumentException(
                    "a grant expiry before the epoch has already run out: " + expiresAt);
        }
    }

    @Override
    public String toString() {
        return "Grant[redacted, expiresAt=" + expiresAt + "]";
    }
}
