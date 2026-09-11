// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Bodies this codec's own {@code encode} can never produce (M5.14).
 *
 * <p>⚠️ ROUND-1 REVIEW MEASURED THE WHOLE DECODE-SIDE GUARD SET UNEXERCISED,
 * and the reason is structural rather than an oversight: every other test in
 * this milestone builds an event, encodes it and decodes it back, so every
 * input the decoder ever sees was produced by the encoder. An unknown
 * {@code via} name silently becoming {@code INLINE}, and all four
 * {@code > Integer.MAX_VALUE} guards deleted, passed the entire suite.
 *
 * <p>⚠️ THAT IS THE OPPOSITE OF WHAT A DECODER IS FOR. Its whole job is bytes
 * that did NOT come from our encoder -- an older peer, a newer one, a truncated
 * frame, a hostile sender. So this file writes the bytes by hand.
 *
 * <p>⚠️ AND ONE OF THESE GUARDS HAS A MEASURED HISTORY. Without the record-count
 * bound, {@code 0x1_0000_0001} decodes to {@code 1}: a count that survives a
 * narrowing cast as a different, plausible number. {@code Cursor}'s own javadoc
 * records that exact class of defect being measured twice in this repository.
 */
class SubscriptionEventMalformedTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");

    /**
     * Builds an event body field by field, so any one field can be made
     * impossible.
     *
     * <p>⚠️ IT DELIBERATELY DOES NOT REUSE {@code SubscriptionEvent.encode}.
     * Sharing the writer is what made the guards unreachable in the first
     * place: a body that only the production encoder can produce is a body the
     * production decoder always accepts.
     */
    private static final class BodyWriter {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        /**
         * ⚠️ DEFAULTS TO THE VERSION THIS BUILD WRITES. Round-1 review of
         * M5.15a found every hand-written body here still emitting
         * {@code VERSION_1} after the version bump -- so all eleven guard tests
         * exercised only the shape the encoder no longer produces, and three
         * mutations guarded by {@code version == VERSION_1 &&} passed. That is
         * verbatim the structural failure this file's own javadoc records
         * review measuring at M5.14, reintroduced by the bump.
         */
        BodyWriter() {
            this(SubscriptionEvent.VERSION_2);
        }

        BodyWriter(int version) {
            out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
                    .putInt(SubscriptionEvent.MAGIC).putInt(version).array());
        }

        BodyWriter str(String v) {
            byte[] utf8 = v.getBytes(StandardCharsets.UTF_8);
            SegmentWriter.putUvarint(out, utf8.length);
            out.writeBytes(utf8);
            return this;
        }

        /** A length prefix that does not match the bytes that follow. */
        BodyWriter strWithLength(long declared, String actual) {
            SegmentWriter.putUvarint(out, declared);
            out.writeBytes(actual.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        BodyWriter uvarint(long v) {
            SegmentWriter.putUvarint(out, v);
            return this;
        }

        BodyWriter uuid(UUID v) {
            out.writeBytes(ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
                    .putLong(v.getMostSignificantBits())
                    .putLong(v.getLeastSignificantBits()).array());
            return this;
        }

        BodyWriter raw(byte... bytes) {
            out.writeBytes(bytes);
            return this;
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    /**
     * A v2 body that is valid in every field, so each test alters exactly one.
     *
     * <p>⚠️ IT ENDS WITH THE SESSION EPOCH, because a v2 body carries one --
     * and a v2 body carrying {@code SESSION_EPOCH_ABSENT} is refused, so 3 here
     * is a live value rather than a filler.
     */
    private static BodyWriter valid() {
        return v1Fields(new BodyWriter()).uvarint(3L);
    }

    /** The fields both versions share, in order. */
    private static BodyWriter v1Fields(BodyWriter w) {
        return w.str("sess-abc")
                .uvarint(42L)
                .uuid(INDEX)
                .uvarint(7L)
                .str("seg/key")
                .uvarint(1_000L)
                .uvarint(128L)
                .str("PROXY")
                .uvarint(0L);
    }

    /** The same body at VERSION_1: no session epoch at all. */
    private static BodyWriter validV1() {
        return v1Fields(new BodyWriter(SubscriptionEvent.VERSION_1));
    }

    @Test
    void theHandWrittenValidBodyDECODES() throws Exception {
        // ⚠️ THE ANTI-VACUITY CHECK. Every other test here asserts a REFUSAL,
        // and a body writer that produced garbage would make all of them pass
        // for the wrong reason. This one proves the writer agrees with the
        // production decoder before any of them alters a field.
        // ⚠️ EVERY COMPONENT, because round-2 review measured four of eight
        // letting a TRANSPOSITION of `firstOffset` and `recordCount` in
        // `valid()` through -- and a body writer that has drifted from the
        // production layout makes every refusal test below pass for the wrong
        // reason.
        SubscriptionEvent decoded = SubscriptionEvent.decode(valid().bytes());
        assertThat(decoded.session()).isEqualTo("sess-abc");
        assertThat(decoded.sequencerEpoch()).isEqualTo(42L);
        assertThat(decoded.key()).isEqualTo(new RunKey(INDEX, 7));
        assertThat(decoded.segmentKey()).isEqualTo("seg/key");
        assertThat(decoded.firstOffset()).isEqualTo(1_000L);
        assertThat(decoded.recordCount()).isEqualTo(128);
        assertThat(decoded.via()).isEqualTo(FetchMode.PROXY);
        assertThat(decoded.inline()).isEmpty();
        assertThat(decoded.sessionEpoch()).isEqualTo(3L);
    }

    /**
     * An unknown {@code via} name is REFUSED, not defaulted.
     *
     * <p>⚠️ ADR-0042 ARGUES `via` IS WRITTEN BY NAME SO A RENAME "fails loudly",
     * and round-1 review measured that claim checked by nothing: a decoder
     * mapping an unknown name to {@code INLINE} passed the whole suite. Under
     * it, a v2 peer sending a fourth mode would have every consumer treat the
     * event as carrying inline bytes it does not have.
     */
    @Test
    void anUNKNOWNViaNameIsREFUSED() {
        byte[] body = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("HYBRID")
                .uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown fetch mode")
                .hasMessageContaining("HYBRID");
    }

    /**
     * A record count that does not fit an {@code int} is REFUSED, not
     * truncated.
     *
     * <p>⚠️ {@code 0x1_0000_0001} NARROWS TO 1, which is the whole danger: not a
     * crash, but a plausible value. A consumer would accept a push claiming one
     * record where the sender meant four billion, and the offsets either side
     * would silently disagree. {@code Cursor}'s javadoc records this class of
     * defect being measured twice here.
     */
    @Test
    void aRecordCountThatDoesNotFitAnIntIsREFUSED() {
        byte[] body = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L)
                .uvarint(0x1_0000_0001L)
                .str("PROXY").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("record count")
                .hasMessageContaining("4294967297");
    }

    /** A partition that does not fit an {@code int} is REFUSED. */
    @Test
    void aPartitionThatDoesNotFitAnIntIsREFUSED() {
        byte[] body = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX)
                .uvarint(0x1_0000_0005L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("PROXY").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition");
    }

    /**
     * A declared inline length past the end of the event is REFUSED, and it
     * never becomes an allocation.
     *
     * <p>⚠️ THIS IS THE LENGTH-AS-ALLOCATION HAZARD {@code Cursor} exists to
     * bound, and its guard is the one being relied on -- so this asserts the
     * reliance actually holds rather than assuming it.
     */
    @Test
    void anInlineLengthPastTheEndIsREFUSED() {
        byte[] body = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("INLINE")
                // ⚠️ 0x1_0000_0005, NOT 0x7FFF_FFFF. Round-2 review found the
                // earlier value IS Integer.MAX_VALUE, so `> Integer.MAX_VALUE`
                // was false, `Cursor.bytes` threw first, and this test passed
                // without ever reaching the guard it names. This value narrows
                // to a small positive int, so without the guard it would PARSE.
                .uvarint(0x1_0000_0005L)
                .raw((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                // ⚠️ ON THE MESSAGE, because that is what distinguishes THIS
                // guard from Cursor's bounds check firing first.
                .hasMessageContaining("inline length")
                .hasMessageContaining("4294967301");
    }

    /** A declared string length past the end is REFUSED. */
    @Test
    void aStringLengthPastTheEndIsREFUSED() {
        // ⚠️ SAME OFF-BY-ONE AS ABOVE, and round-2 review proved the test was
        // vacuous by rewriting the prefix to a COMPLETELY CORRECT `5` and
        // watching it stay green. It also measured what the guard buys: with it
        // deleted, a body declaring a session length of 4294967304 followed by
        // eight bytes returns a fully valid event with no error at all.
        byte[] body = new BodyWriter().strWithLength(0x1_0000_0008L, "sess-abc").bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("string length")
                .hasMessageContaining("4294967304");
    }

    /**
     * A NEGATIVE length -- a ten-byte varint with bit 63 set -- is REFUSED.
     *
     * <p>⚠️ ROUND-2 REVIEW MEASURED THIS DECODING SILENTLY. `Cursor.uvarint`
     * returns a long, so bit 63 makes it negative, `> Integer.MAX_VALUE` is
     * false, and the cast keeps the low 32 bits: `0x8000000000000064` became a
     * record count of 100 and `0x8000000000000003` became partition 3. A
     * corrupt or mis-framed event then delivers one stream's records under
     * another stream's partition and reports a clean stream.
     *
     * <p>⚠️ `Checkpoint` AND `MembershipFilter` IN THIS PACKAGE BOTH WRITE
     * `< 0 ||` for exactly this, and this class named them as its precedent
     * while checking only the upper bound.
     */
    @Test
    void aNEGATIVELengthFromABitSixtyThreeVarintIsREFUSED() {
        byte[] count = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L)
                .uvarint(0x8000_0000_0000_0064L)
                .str("PROXY").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(count))
                .as("it would otherwise narrow to a plausible 100")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("record count");

        byte[] partition = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX)
                .uvarint(0x8000_0000_0000_0003L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("PROXY").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(partition))
                .as("it would otherwise narrow to a plausible partition 3")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition");

        // ⚠️ THE TWO LENGTH FIELDS TOO, and they need their own cases: a
        // POSITIVE over-int length is caught by the upper bound alone, so the
        // `< 0` half of those two guards stayed unexercised until here.
        // `0x8000000000000008` narrows to 8 -- and `Cursor.bytes(8)` would then
        // succeed, so without the guard these bodies PARSE.
        byte[] stringLength = new BodyWriter()
                .strWithLength(0x8000_0000_0000_0008L, "sess-abc").bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(stringLength))
                .as("a negative string length narrows to a length that would read cleanly")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("string length");

        byte[] inlineLength = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("INLINE")
                .uvarint(0x8000_0000_0000_0005L)
                .raw((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(inlineLength))
                .as("and a negative inline length narrows to exactly the bytes that follow")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("inline length");

        // ⚠️ THE FIFTH VARINT FIELD, added to the wire by M5.15a and not to
        // this enumeration. Round-2 review measured the decode-side guard
        // deletable -- behaviour-preserving, because the value then reaches the
        // compact constructor which refuses it too, so only the MESSAGE
        // changes. Pinned anyway: a caller decoding untrusted bytes should be
        // told which field was wrong, and a guard nothing exercises is a guard
        // nothing keeps.
        byte[] negativeSessionEpoch = v1Fields(new BodyWriter(SubscriptionEvent.VERSION_2))
                .uvarint(0x8000_0000_0000_0007L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(negativeSessionEpoch))
                .as("a bit-63 session epoch is not a request order")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("session epoch")
                // ⚠️ AND THE DECODER REPORTS IT, not the constructor. My first
                // attempt asserted only "session epoch" and the mutation
                // SURVIVED: with the decode guard deleted the value reaches the
                // compact constructor, which refuses it too and whose message
                // also contains that phrase, so the bytes are rejected either
                // way and nothing distinguished the two paths. The wrapped form
                // is prefixed "malformed subscription event"; its absence is
                // what says the field was validated where it was read.
                .hasMessageNotContaining("malformed");
    }

    /**
     * A body violating the {@code via}/{@code inline} pairing arrives as a
     * PARSE failure, not an {@code IllegalArgumentException}.
     *
     * <p>⚠️ THIS IS THE PATH THE PAIRING TEST CLAIMED AND DID NOT TAKE. Round-1
     * review measured `SubscriptionEvent`'s
     * {@code catch (IllegalArgumentException)} unreachable: both assertions
     * there call the CONSTRUCTOR. A caller decoding untrusted bytes must not
     * have to catch two kinds of thing, and only a hand-written body can prove
     * it does not.
     */
    @Test
    void aBodyViolatingTheVIAPairingIsAPARSEFailure() {
        byte[] inlineWithNoBytes = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("INLINE").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(inlineWithNoBytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("malformed subscription event")
                .hasMessageContaining("via=INLINE carries the bytes");

        byte[] proxyWithBytes = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("PROXY").uvarint(2L).raw((byte) 1, (byte) 2).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(proxyWithBytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not carry bytes");
    }

    /** A blank session arriving over the wire is a parse failure too. */
    @Test
    void aBlankSessionOverTheWireIsAPARSEFailure() {
        byte[] body = new BodyWriter()
                .str("").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("PROXY").uvarint(0L).uvarint(3L).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("blank session");
    }

    /**
     * TRAILING BYTES are refused.
     *
     * <p>⚠️ {@code ChainEntry}, {@code Checkpoint} and {@code SegmentReader} all
     * end their decoders with this check, and round-1 review found it missing
     * here while this class's javadoc named {@code ChainEntry} as its
     * precedent. Bytes after a complete event mean the sender and this reader
     * disagree about the shape -- a v2 field, or two events run together by a
     * framing bug -- and silently accepting the prefix is how a consumer
     * processes half a stream and reports success.
     */
    @Test
    void TRAILINGBytesAreREFUSED() {
        byte[] body = valid().raw((byte) 0xFF, (byte) 0xFE).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing bytes");

        // ⚠️ AND ON AN INLINE BODY, which is the branch that matters. Round-3
        // review MEASURED `if (inline.length == 0 && !c.atEnd())` passing all
        // 28 tests, because every trailing-bytes body here had an EMPTY inline
        // payload -- while `inline` is the DEFAULT mode (M5.11 criterion 4), so
        // the guard was pinned on the path least likely to be taken.
        byte[] withPayload = new BodyWriter()
                .str("sess-abc").uvarint(42L).uuid(INDEX).uvarint(7L)
                .str("seg/key").uvarint(1_000L).uvarint(128L)
                .str("INLINE").uvarint(3L).raw((byte) 1, (byte) 2, (byte) 3).uvarint(3L)
                .raw((byte) 0xFF, (byte) 0xFE).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(withPayload))
                .as("a v2 field appended to an inline push must not be read as a complete event")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing bytes");

        // ⚠️ AND ON A v1 BODY, because the guard became version-sensitive the
        // moment `BodyWriter` started defaulting to v2. Round-2 review measured
        // `version == VERSION_2 && !c.atEnd()` surviving all 260 format tests:
        // a body labelled VERSION_1 with bytes past the v1 shape -- a writer
        // that appended the epoch without bumping the version, a future v3
        // field, or two v1 events run together by a framing bug -- decoded as a
        // complete v1 event with ABSENT, dropping the extra and reporting a
        // clean stream. Verbatim what the guard's own comment forbids.
        byte[] v1WithTrailing = validV1().raw((byte) 0xFF, (byte) 0xFE).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(v1WithTrailing))
                .as("the refusal is not a property of the newer version")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing bytes");
    }

    /**
     * The varint width transitions round-trip.
     *
     * <p>⚠️ ROUND-1 REVIEW SHOWED MY EXTREMES TEST NAMED THE WRONG BOUNDARY:
     * {@code Long.MAX_VALUE} is a NINE-byte uvarint, not ten -- a ten-byte one
     * needs bit 63 set, which the constructor rejects. The transitions that
     * actually change the encoding width are 127/128 and 16383/16384, and
     * neither was covered.
     *
     * <p>⚠️ AND THIS IS DEFENCE IN DEPTH RATHER THAN PROOF, said plainly
     * because the distinction cost a red record to discover: the varint ENCODER
     * is {@code SegmentWriter}'s and is exercised by every chain format, so the
     * only mutations of THIS class that kill this test are ones that also kill
     * the plain round trips. What it adds is that the boundary VALUES are
     * carried through this event's own field order at all -- worth having, not
     * worth calling a varint-width proof.
     */
    @Test
    void theVarintWIDTHTransitionsRoundTrip() throws Exception {
        long[] boundaries = {126L, 127L, 128L, 129L, 16_382L, 16_383L, 16_384L, 16_385L};
        for (long boundary : boundaries) {
            SubscriptionEvent event = new SubscriptionEvent("s", boundary, 1L, new RunKey(INDEX, 1), "k", boundary, 1, FetchMode.PROXY, new byte[0]);
            assertThat(SubscriptionEvent.decode(event.encode()))
                    .as("uvarint boundary %d", boundary)
                    .isEqualTo(event);
        }
    }

    /**
     * A push whose range runs past the last addressable offset is REFUSED.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED {@code lastOffset()} RETURNING
     * -9223372036854775683 for {@code firstOffset = Long.MAX_VALUE - 1} with
     * {@code recordCount = 128}, and my own extremes test stepped around it by
     * using a record count of 1 -- the single value at that offset where the
     * sum still fits. A negative last offset compares as EARLIER than the
     * first, so a consumer sees the range run backwards rather than an error.
     */
    @Test
    void aRangePastTheLastAddressableOffsetIsREFUSED() {
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, new RunKey(INDEX, 1), "k",
                        Long.MAX_VALUE - 1, 128, FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("last addressable offset");

        // ⚠️ THE LAST OFFSET IS `first + count - 1`, so a single record AT
        // Long.MAX_VALUE fits exactly. Round-2 review measured the first guard
        // rejecting it -- an off-by-one that this assertion, written as
        // `MAX_VALUE - 1`, would have frozen into the tree as correct.
        SubscriptionEvent exact = new SubscriptionEvent("s", 1L, 1L, new RunKey(INDEX, 1), "k",
                Long.MAX_VALUE, 1, FetchMode.PROXY, new byte[0]);
        assertThat(exact.lastOffset())
                .as("the largest range that DOES fit is still allowed")
                .isEqualTo(Long.MAX_VALUE);

        // ⚠️ AND THE FIRST VALUE THAT MUST BE REFUSED, one past the boundary.
        // Round-3 review MEASURED the refusal asserted only 127 units past it,
        // so a compound weakening -- `recordCount > 2 &&` bolted onto the
        // guard -- admitted a two-record push at the top of the offset space
        // with `lastOffset()` returning Long.MIN_VALUE.
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, new RunKey(INDEX, 1), "k",
                        Long.MAX_VALUE, 2, FetchMode.PROXY, new byte[0]))
                .as("two records from the last addressable offset is one too many")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("last addressable offset");
    }


    /**
     * A v2 body carrying the ABSENT sentinel is REFUSED, and the sentinel's
     * VALUE is asserted literally.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED BOTH HALVES BROKEN. `SESSION_EPOCH_ABSENT`
     * was pinned only by a comparison with itself, so moving it to 5 passed the
     * whole suite; and the collision it was supposed to prevent already
     * existed -- a v2 event built with epoch 0 encoded, decoded, and read back
     * as ABSENT while three separate javadocs claimed "session epochs number
     * from 1".
     *
     * <p>⚠️ THE FIX MADE THE SENTINEL OUT-OF-BAND: an event with no session
     * epoch IS a v1 event, so no v2 body can carry the value. This asserts the
     * refusal a hand-written body is the only way to reach.
     *
     * <p>⚠️ WHY IT MATTERS BEYOND TIDINESS: M5.15b's registry leaving a
     * {@code long} at Java's default 0 would make every event read as ABSENT
     * and every resume degrade to a commit-log refetch -- forever, with a green
     * suite.
     */
    @Test
    void aV2BodyCarryingTheABSENTSentinelIsREFUSED() {
        assertThat(SubscriptionEvent.SESSION_EPOCH_ABSENT)
                .as("the sentinel's value is 0 and live epochs number from 1")
                .isZero();

        byte[] body = v1Fields(new BodyWriter(SubscriptionEvent.VERSION_2))
                .uvarint(SubscriptionEvent.SESSION_EPOCH_ABSENT).bytes();
        assertThatThrownBy(() -> SubscriptionEvent.decode(body))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("may not carry an absent session epoch");
    }

    /**
     * A v1 body decodes through the hand-written path too, and reads as ABSENT.
     *
     * <p>⚠️ THE GOLDEN FILES COVER THE STORED v1 BYTES; this covers the SHAPE,
     * so a v1 body that never existed as a fixture is still parsed. Together
     * they are the compatibility claim: one pins the bytes we shipped, the
     * other pins that the reader handles the shape at all.
     */
    @Test
    void aV1BodyDecodesAndReadsAsABSENT() throws Exception {
        SubscriptionEvent decoded = SubscriptionEvent.decode(validV1().bytes());
        assertThat(decoded.sessionEpoch()).isEqualTo(SubscriptionEvent.SESSION_EPOCH_ABSENT);
        assertThat(decoded.sequencerEpoch()).isEqualTo(42L);
        assertThat(decoded.via()).isEqualTo(FetchMode.PROXY);

        // ⚠️ AND IT RE-ENCODES TO v1 BYTES, byte-identically. That is stronger
        // than decode-only compatibility: an event read from an old writer and
        // written back is unchanged, which is only true because the VERSION now
        // follows the FIELD rather than being a constant.
        assertThat(decoded.encode()).isEqualTo(validV1().bytes());
    }

    /**
     * The SESSION epoch's varint width transitions round-trip.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED `putUvarint(out, sessionEpoch & 0x7F)`
     * SURVIVING: every session epoch in the tree was 0, 1, 3, 4, 7 or 8, all
     * inside one varint byte. The sibling test varies the SEQUENCER epoch
     * across these same boundaries for exactly this reason, and the new field
     * was spliced into it as a fixed literal.
     *
     * <p>⚠️ AND THE MUTATION IS FINDING 1 BY A SECOND ROUTE: under it a session
     * at request 128 encodes as 0, which is the ABSENT sentinel -- a live epoch
     * silently becoming its own absence.
     */
    @Test
    void theSESSIONEpochVarintWidthTransitionsRoundTrip() throws Exception {
        long[] boundaries = {1L, 126L, 127L, 128L, 129L, 16_382L, 16_383L, 16_384L, 16_385L};
        for (long boundary : boundaries) {
            SubscriptionEvent event = new SubscriptionEvent("s", 42L, boundary,
                    new RunKey(INDEX, 1), "k", 0L, 1, FetchMode.PROXY, new byte[0]);
            SubscriptionEvent back = SubscriptionEvent.decode(event.encode());
            assertThat(back).as("session epoch %d", boundary).isEqualTo(event);
            assertThat(back.sessionEpoch())
                    .as("and it must not collapse to the ABSENT sentinel")
                    .isEqualTo(boundary);
        }
    }
}
