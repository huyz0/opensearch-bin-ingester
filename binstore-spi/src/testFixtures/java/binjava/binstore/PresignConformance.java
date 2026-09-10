// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * What EVERY backend owes the {@code direct} fetch mode (M5.10, ADR-0041).
 *
 * <p>⚠️ IT PINS BOTH ANSWERS, not just the capable one. A backend may honestly
 * lack presigning -- neither shipping backend has it -- and the contract is
 * then that {@link Capabilities#presignedUrls()} says so and
 * {@link BinStore#presign} refuses. The failure this catches is the pair
 * DISAGREEING: a backend advertising the capability and throwing anyway would
 * pass a startup check and fail at first use, which is the whole thing
 * {@link Capabilities#requirePresignedUrls()} exists to prevent.
 *
 * <p>⚠️ ONE HALF OF THE CONTRACT IS MECHANICALLY UNCHECKABLE HERE, and saying
 * so is better than a green suite implying otherwise. {@link BinStore#presign}
 * requires that signing issue no request PER SIGNATURE, and the case this
 * excludes -- a backend doing a {@code stat} or an {@code iam.signBlob} round
 * trip inside its own implementation -- is invisible from outside: review
 * MEASURED a stand-in doing {@code stat(key)} before signing surviving both
 * this suite and a {@code CountingBinStore} wrapped around it, because a meter
 * ABOVE a backend cannot see the backend's internal traffic. That half rests on
 * the contract being read, and on M5.37.
 *
 * <p>⚠️ SITS IN THE CHAIN so every backend runs it without opting in.
 * {@code MultipartConformance -> ConditionalWriteConformance -> this ->
 * BinStoreConformance}, and both shipping backends extend the last.
 */
public abstract class PresignConformance extends ConditionalWriteConformance {

    /** The capability and the method agree, whichever way the backend answers. */
    @Test
    void theCAPABILITYAndTheMETHODAgree() throws Exception {
        try (BinStore store = newStore()) {
            store.put("presign/agree", Body.ofBytes(bytes("x")));

            if (store.capabilities().presignedUrls()) {
                // ⚠️ BOUNDS, NOT RESTATEMENTS. An earlier version asserted
                // `isNotBlank()` and `isNotNull()` here, which review MEASURED
                // as tautologies: `SignedUrl`'s constructor already rejects
                // both, so a `presign` ignoring its `ttl` and answering
                // `Instant.EPOCH` or `Instant.MAX` would have passed.
                // ⚠️ THE WALL CLOCK, AND IT IS TIGHT RATHER THAN LOOSE.
                // `presign(key, ttl)` offers no clock seam, so this reads the
                // clock either side of the call -- and because the assertion
                // below brackets the mint to `[before.plus(ttl),
                // after.plus(ttl)]`, the tolerance is the DURATION OF THE CALL,
                // microseconds for a local signer. Review MEASURED it: a
                // stand-in expiring at `plusSeconds(1)` is killed, and so is
                // one at `plusMillis(1)`. An earlier version of this comment
                // said it caught "orders of magnitude, not one off by a
                // second"; that was written before the lower bound was
                // tightened and is false against the bound now here.
                // ⚠️ AND NON-NEGOTIABLE 7 IS NOT THE ARGUMENT AGAINST A CLOCK
                // SEAM -- it is the rule that argues FOR one, and an earlier
                // version of this comment cited it inverted. The real argument
                // is that a store backend is an ADAPTER rather than business
                // logic, and none of S3 SigV4, an Azure user-delegation SAS or
                // GCS V4 would take an injected clock: each signs against the
                // clock its own SDK reads.
                Instant before = Instant.now();
                Duration ttl = Duration.ofMinutes(5);
                SignedUrl signed = store.presign("presign/agree", ttl);
                Instant after = Instant.now();

                // ⚠️ THE LOWER BOUND IS `before.plus(ttl)`, NOT `before`. Review
                // MEASURED that a `presign` returning `Instant.now()` -- the
                // TTL ignored entirely, a grant expiring at the instant it was
                // minted -- passed the weaker form, and that `lastTtl` does not
                // save it: recording the argument says nothing about its
                // effect. The two clock reads bracket the mint, so the tighter
                // bound costs no slack.
                assertThat(signed.expiresAt())
                        .as("the grant must live for the TTL the caller asked for, and no longer")
                        .isAfterOrEqualTo(before.plus(ttl))
                        .isBeforeOrEqualTo(after.plus(ttl));
                assertThat(signed.url())
                        .as("and it must name the key it was asked for")
                        .contains("presign/agree");
                store.capabilities().requirePresignedUrls();

                assertThatThrownBy(() -> store.presign("presign/agree", Duration.ZERO))
                        .as("a TTL that is not positive is not short-lived, it is unusable")
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> store.presign("presign/agree", Duration.ofMinutes(-1)))
                        .as("and negative as well as zero -- review measured that dropping the "
                                + "negative half survived a suite probing only ZERO")
                        .isInstanceOf(IllegalArgumentException.class);
            } else {
                assertThatThrownBy(() -> store.presign("presign/agree", Duration.ofMinutes(5)))
                        .as("a backend that says it cannot presign must refuse, not return "
                                + "something unusable")
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> store.capabilities().requirePresignedUrls())
                        .as("and it must be refusable at STARTUP, which is the point of the "
                                + "capability")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("refusing to start");
            }
        }
    }

}
