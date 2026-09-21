// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The v3 event: a {@code direct} grant, and the range it is scoped to (M5.44).
 *
 * <p>⚠️ ADR-0041 ASSIGNED BOTH TO M5.14 BY NAME and M5.14 shipped the shape
 * carrying NEITHER, so {@code FetchMode.DIRECT} was an encodable mode with
 * nothing able to convey the URL a consumer would fetch with. ADR-0042 then
 * dropped research doc 04 §2c's {@code byteStart}/{@code byteLen} deliberately
 * and named this row as the one deciding whether closing security.md rule 3's
 * "where possible one range" means bringing them back.
 *
 * <p>⚠️ **IT DOES NOT, AND AN EARLIER DRAFT OF THIS PARAGRAPH ENDED "It does."**
 * The fields came back, but ADR-0044 records that rule 3's range half is still
 * OPEN: the grant is scoped to one key and to NO range -- not because a range
 * cannot be NAMED, which ADR-0044 sets out that two can be, but because no
 * reader in the tree ISSUES one, so there is no range for a signature to cover.
 * ⚠️ An earlier draft of this paragraph said "no range is expressible", which
 * is the claim ADR-0044 itself had to retire in five places. **M5.66** owns
 * building the reader that issues one, and owns whether these fields stay at
 * all.
 *
 * <p>⚠️ THE ASSERTIONS BELOW ARE UNTOUCHED BY THAT, and review measured why
 * they are worth keeping: forcing {@code decode} to return {@code RANGE_ABSENT}
 * reds three cases here and in the golden test, so the fields are UNUSED by any
 * reader in the tree but they are NOT unpinned.
 */
class SubscriptionEventGrantTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666"), 7);
    private static final Grant GRANT =
            new Grant("https://store.example/x?sig=abc", Instant.ofEpochMilli(1_757_764_800_000L));

    private static SubscriptionEvent event(Grant grant, long byteStart, long byteLen) {
        return new SubscriptionEvent("sess", 1L, 2L, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0], grant, byteStart, byteLen);
    }

    /**
     * Encode, decode, equals -- the property the skill's checklist names.
     *
     * <p>⚠️ IT IS NOT REDUNDANT WITH THE GOLDEN FILES. They pin the BYTES
     * against drift; this pins that the two directions agree, which a change
     * that broke both symmetrically would still satisfy.
     */
    @Test
    void aV3EventSurvivesAROUNDTrip() throws Exception {
        for (SubscriptionEvent original : java.util.List.of(
                event(GRANT, 4_096L, 65_536L),
                event(GRANT, 0L, 1L),
                event(GRANT, SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT))) {
            assertThat(SubscriptionEvent.decode(original.encode()))
                    .as("round trip of %s", original)
                    .isEqualTo(original);
        }
    }

    /**
     * A range starting at byte ZERO survives, which the sentinel choice is for.
     *
     * <p>⚠️ {@code byteStart = 0} IS A REAL RANGE -- the segment's first byte --
     * so an encoding that used 0 to mean ABSENT would lose it. The presence
     * byte is what keeps the two distinguishable, and this case is what would
     * fail if someone replaced it with a sentinel.
     */
    @Test
    void aRangeStartingAtBYTEZEROIsNotMistakenForABSENT() throws Exception {
        SubscriptionEvent atZero = event(GRANT, 0L, 4_096L);

        SubscriptionEvent back = SubscriptionEvent.decode(atZero.encode());

        assertThat(back.byteStart()).as("byte zero, not absent").isZero();
        assertThat(back.byteLen()).isEqualTo(4_096L);
        assertThat(back).isEqualTo(atZero);
    }

    @Test
    void aRANGEWithoutAGrantIsRefused() {
        assertThatThrownBy(() -> event(null, 4_096L, 65_536L))
                .as("encode writes the range inside the grant, so this would not round trip")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nothing to scope");
    }

    @Test
    void aHALFRangeIsRefusedInBothDirections() {
        assertThatThrownBy(() -> event(GRANT, 4_096L, SubscriptionEvent.RANGE_ABSENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both a start and a length");
        assertThatThrownBy(() -> event(GRANT, SubscriptionEvent.RANGE_ABSENT, 65_536L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both a start and a length");
        assertThatThrownBy(() -> event(GRANT, 4_096L, 0L))
                .as("a zero-length range grants nothing")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("grants nothing");
    }

    /**
     * A grant without a session epoch is refused where it is BUILT.
     *
     * <p>⚠️ AN EARLIER VERSION OF THIS CASE ASSERTED THE WIRE REFUSAL AND WAS
     * NAMED FOR IT, which pinned a writer defect as reader correctness. Review
     * measured what that allowed: {@code encode} stamped VERSION_3, wrote 0
     * into the epoch slot, and {@code decode} threw -- an event this class
     * produces and cannot read back, with the ingester recording a successful
     * push every consumer rejects.
     *
     * <p>⚠️ ADR-0042's AMENDMENT ASSIGNED THIS DECISION TO M5.44 BY NAME: the
     * epoch slot is NOT special-cased for v3, so a v3 body always carries the
     * epoch and 0 there is the sentinel rather than a value. The constructor is
     * where that becomes true.
     */
    @Test
    void aGrantWithoutASessionEPOCHIsRefusedWhereItIsBUILT() {
        assertThatThrownBy(() -> new SubscriptionEvent("sess", 1L,
                SubscriptionEvent.SESSION_EPOCH_ABSENT, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0], GRANT,
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT))
                .as("refused at construction, so encode can never emit a body decode refuses")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a grant needs a session epoch");
    }

    /**
     * A grant's expiry survives the wire even with sub-millisecond precision.
     *
     * <p>⚠️ REVIEW MEASURED THE ROUND TRIP FALSE for
     * {@code Instant.ofEpochSecond(1000, 123456789)}: the event encodes epoch
     * MILLIS, and once {@code equals} compares the grant the nanos came back
     * missing. It is not a fixture curiosity -- {@code clock.instant()} carries
     * microseconds on Linux, so a real presigner's
     * {@code instant().plus(ttl)} hits it on nearly every grant. Every case
     * here used {@code ofEpochMilli}, so nothing saw it.
     */
    @Test
    void aSUBMILLISECONDExpirySurvivesTheWire() throws Exception {
        Grant precise = new Grant("https://store.example/x?sig=abc",
                Instant.ofEpochSecond(1_000L, 123_456_789L));
        SubscriptionEvent event = new SubscriptionEvent("sess", 1L, 2L, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0], precise,
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT);

        assertThat(SubscriptionEvent.decode(event.encode()))
                .as("truncated at construction, so the in-memory value is the one that travels")
                .isEqualTo(event);
        assertThat(precise.expiresAt().getNano() % 1_000_000)
                .as("and the truncation happened where it can be seen, not on the wire")
                .isZero();
    }

    /** An expiry too far out to be epoch millis is refused rather than overflowing encode. */
    @Test
    void anExpiryTooFARForEpochMillisIsRefused() {
        assertThatThrownBy(() -> new Grant("https://store.example/x", Instant.MAX))
                .as("Instant.MAX.toEpochMilli() throws, so encode would die on a value "
                        + "the constructor had accepted")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch milliseconds");
    }

    @Test
    void aNEGATIVEByteStartIsRefused() {
        assertThatThrownBy(() -> event(GRANT, -7L, 5L))
                .as("it constructed and encoded and then decode refused it, so the constructor's "
                        + "own claim to keep the round trip total was false")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a range");
    }

    /** A range-present flag that is neither 0 nor 1 is refused, not read as absent. */
    @Test
    void aRangePresentFLAGThatIsNeitherZeroNorOneIsREFUSED() throws Exception {
        byte[] withRange = event(GRANT, 4_096L, 65_536L).encode();
        // ⚠️ THE FLAG IS THE BYTE BEFORE THE TWO RANGE VARINTS, which for this
        // fixture are `8020` (4096) and `808004` (65536) -- five bytes, so the
        // flag is six from the end.
        int flag = withRange.length - 6;
        assertThat(withRange[flag]).as("the fixture's flag really is here").isEqualTo((byte) 1);
        withRange[flag] = 2;

        assertThatThrownBy(() -> SubscriptionEvent.decode(withRange))
                .as("a 2 falling through to `== 1` decodes as a WHOLE-OBJECT grant, silently "
                        + "downgrading a range-scoped one")
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("range-present flag");
    }

    /**
     * A negative expiry on the wire is an IOException too, not an IAE.
     *
     * <p>⚠️ THE SAME DEFECT AS THE BLANK URL, AND THE COMMIT CLAIMED BOTH WERE
     * FIXED WHILE ONE HAD A CASE. Review measured the guard relaxed to
     * {@code if (false)} leaving all 277 tests green, after which
     * {@code new Grant(...)} throws
     * {@code IllegalArgumentException: a grant expiry before the epoch has
     * already run out} from OUTSIDE the try that exists so a caller decoding
     * untrusted bytes need not catch two kinds of thing.
     */
    @Test
    void aNEGATIVEExpiryOnTheWireIsAnIOExceptionToo() throws Exception {
        byte[] good = event(GRANT, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT).encode();
        // ⚠️ THE EXPIRY IS THE SIX VARINT BYTES BEFORE THE PRESENCE BYTE, which
        // is last for a whole-object grant. A ten-byte varint of all-ones reads
        // back as -1, which is the shape a hostile or corrupt sender produces.
        int flagAt = good.length - 1;
        assertThat(good[flagAt]).as("the presence byte is last for a whole-object grant")
                .isEqualTo((byte) 0);
        byte[] hostile = new byte[good.length + 4];
        System.arraycopy(good, 0, hostile, 0, flagAt - 6);
        for (int i = 0; i < 9; i++) {
            hostile[flagAt - 6 + i] = (byte) 0xff;
        }
        hostile[flagAt - 6 + 9] = 0x01;
        hostile[hostile.length - 1] = 0;

        assertThatThrownBy(() -> SubscriptionEvent.decode(hostile))
                .as("a decoder reading untrusted bytes reports one kind of thing")
                .isInstanceOf(java.io.IOException.class);
    }

    /**
     * A grant expiring before the epoch is refused where it is BUILT.
     *
     * <p>⚠️ IT IS THE ONE BOUND OF {@code Grant}'s FOUR WITH NO CASE, and it is
     * load-bearing for encode-decode totality: a pre-epoch expiry encodes as a
     * ten-byte varint that {@code decode} then refuses, which is exactly the
     * asymmetry {@code aNEGATIVEByteStartIsRefused} pins for its own field.
     */
    @Test
    void aPreEPOCHExpiryIsRefused() {
        assertThatThrownBy(() -> new Grant("https://store.example/x",
                Instant.ofEpochMilli(-1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already run out");
    }

    /**
     * {@code toString} carries the grant's EXPIRY and never its URL.
     *
     * <p>⚠️ THE REDACTION IS A COMPOSITION, NOT A PROPERTY OF ONE TYPE, and
     * this commit asserted it in a javadoc while nothing constrained it. Review
     * measured {@code + ", grant=" + grant} replaced by
     * {@code grant.url()} leaving the whole suite green -- a presigned URL with
     * its signature in every log line printing an event, security.md rule 4
     * defeated at the one place the class's own javadoc calls "the line an
     * operator prints while debugging a delivery".
     *
     * <p>⚠️ {@code SubscriptionEventTest.toStringCarriesNODocumentPayload} owns
     * rule 4 for this type and pins the INLINE payload; the commit that added a
     * SECOND secret to the same method did not extend it. This is that
     * extension, kept beside the grant's other cases.
     */
    @Test
    void toStringCarriesTheGrantsEXPIRYAndNEVERItsURL() {
        SubscriptionEvent event = new SubscriptionEvent("sess", 1L, 2L, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/x?sig=TOPSECRETSIGNATURE",
                        Instant.ofEpochMilli(1_757_764_800_000L)),
                4_096L, 65_536L);

        assertThat(event.toString())
                .as("the signature is the thing rule 4 forbids")
                .doesNotContain("TOPSECRETSIGNATURE")
                .doesNotContain("https://")
                .contains("redacted");
        assertThat(event.toString())
                .as("and the expiry IS what an operator needs when a direct fetch fails")
                .contains("2025-09-13T12:00:00Z");
        assertThat(event.toString())
                .as("the range prints too, because it is not a secret and it is the "
                        + "run's physical extent -- NOT because a ranged GET asked for it, "
                        + "which an earlier version of this message said and ADR-0044 "
                        + "retired: no reader in the tree issues a ranged GET")
                .contains("byteStart=4096")
                .contains("byteLen=65536");

        // ⚠️ BOTH GRANT SHAPES, because one is a coverage gap rather than a
        // contract. Review measured a leak conditional on the OTHER shape
        // surviving -- `byteStart != RANGE_ABSENT ? grant : grant.url()` --
        // and the whole-object grant is not hypothetical: RANGE_ABSENT is what
        // the nine-argument delegating constructor supplies, ADR-0043 keeps the
        // grant scoped to the key, and two cases in this file build it.
        SubscriptionEvent wholeObject = new SubscriptionEvent("sess", 1L, 2L, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/x?sig=TOPSECRETSIGNATURE",
                        Instant.ofEpochMilli(1_757_764_800_000L)),
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT);

        assertThat(wholeObject.toString())
                .as("a whole-object grant redacts too, or the redaction holds only for the "
                        + "shape somebody happened to test")
                .doesNotContain("TOPSECRETSIGNATURE")
                .doesNotContain("https://")
                .contains("redacted");
    }

    /**
     * An expiry of exactly the epoch round-trips, which pins the guard's edge.
     *
     * <p>⚠️ REVIEW MEASURED {@code expiresAtMillis < 0} RELAXED TO {@code <= 0}
     * SURVIVING: {@code Grant} accepts millis 0, so the mutant makes
     * {@code decode} refuse a grant the constructor accepts -- the same
     * encode-decode asymmetry this commit already closed twice, for the absent
     * session epoch and for a negative {@code byteStart}. Degenerate as a
     * value, identical as a defect.
     */
    @Test
    void anExpiryOfEXACTLYTheEpochRoundTrips() throws Exception {
        SubscriptionEvent atEpoch = new SubscriptionEvent("sess", 1L, 2L, KEY, "seg/x", 10L, 5,
                FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/x", Instant.EPOCH),
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT);

        assertThat(SubscriptionEvent.decode(atEpoch.encode()))
                .as("accepted by the constructor, so it must be accepted by the reader")
                .isEqualTo(atEpoch);
    }

    /** A grant with no url on the wire is an IOException, not an IllegalArgumentException. */
    @Test
    void aBLANKUrlOnTheWireIsAnIOExceptionLikeEveryOtherMalformedField() throws Exception {
        byte[] good = event(GRANT, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT).encode();
        int urlLen = GRANT.url().length();
        int urlLenAt = good.length - 1 - 1 - 6 - urlLen;
        assertThat(good[urlLenAt]).as("the url's length prefix is where it is expected")
                .isEqualTo((byte) urlLen);
        byte[] blanked = new byte[good.length - urlLen];
        System.arraycopy(good, 0, blanked, 0, urlLenAt);
        blanked[urlLenAt] = 0;
        System.arraycopy(good, urlLenAt + 1 + urlLen, blanked, urlLenAt + 1,
                good.length - urlLenAt - 1 - urlLen);

        assertThatThrownBy(() -> SubscriptionEvent.decode(blanked))
                .as("a decoder reading untrusted bytes must not escape with a second kind of "
                        + "thing -- `Grant`'s own guard throws IllegalArgumentException")
                .isInstanceOf(java.io.IOException.class);
    }

    /** An unknown version still STOPS, and v4 is the next one nobody has written. */
    @Test
    void aVERSIONFOUREventIsREFUSEDNotSkipped() throws Exception {
        byte[] v3 = event(GRANT, 4_096L, 65_536L).encode();
        v3[7] = 4;

        assertThatThrownBy(() -> SubscriptionEvent.decode(v3))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("unsupported subscription event version: 4");
    }
}
