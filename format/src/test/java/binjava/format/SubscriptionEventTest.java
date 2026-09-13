// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The subscription event's shape and codec (M5.14, FR-6).
 *
 * <p>⚠️ THE ROUND TRIP IS THE POINT OF HAVING A CODEC, and it is asserted over
 * the field space rather than on one example: a codec that drops or transposes
 * a field encodes and decodes happily, and a single fixture whose fields are
 * all distinctive is the only thing that catches it. Every value below differs
 * from every other for that reason.
 */
class SubscriptionEventTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");
    private static final RunKey KEY = new RunKey(INDEX, 7);

    private static SubscriptionEvent inlineEvent() {
        return new SubscriptionEvent("sess-abc", 42L, 1L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, FetchMode.INLINE, new byte[] {1, 2, 3, 4, 5});
    }

    private static SubscriptionEvent coordinatesEvent(FetchMode via) {
        return new SubscriptionEvent("sess-abc", 42L, 1L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, via, new byte[0]);
    }

    @Test
    void anINLINEEventRoundTrips() throws Exception {
        SubscriptionEvent original = inlineEvent();
        assertThat(SubscriptionEvent.decode(original.encode())).isEqualTo(original);
    }

    @Test
    void aPROXYAndaDIRECTEventRoundTrip() throws Exception {
        for (FetchMode via : new FetchMode[] {FetchMode.PROXY, FetchMode.DIRECT}) {
            SubscriptionEvent original = coordinatesEvent(via);
            assertThat(SubscriptionEvent.decode(original.encode()))
                    .as("via=%s", via)
                    .isEqualTo(original);
        }
    }

    /**
     * Every field survives, one at a time.
     *
     * <p>⚠️ A ROUND TRIP OF ONE FIXTURE CANNOT SEE A TRANSPOSITION. Two fields
     * of the same type written in the wrong order still decode to a value that
     * equals itself if the fixture happens to give them the same value -- so
     * each is varied ALONE here, against a base where nothing collides.
     */
    @Test
    void EVERYFieldSurvivesTheRoundTripIndividually() throws Exception {
        SubscriptionEvent base = inlineEvent();
        SubscriptionEvent[] variants = {
            new SubscriptionEvent("other-session", base.sequencerEpoch(), 1L, base.key(), base.segmentKey(),
                    base.firstOffset(), base.recordCount(), base.via(), base.inline()),
            new SubscriptionEvent(base.session(), 99L, 1L, base.key(), base.segmentKey(),
                    base.firstOffset(), base.recordCount(), base.via(), base.inline()),
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 1L, new RunKey(UUID.fromString("ffffffff-0000-4000-8000-000000000001"), 3),
                    base.segmentKey(), base.firstOffset(), base.recordCount(), base.via(),
                    base.inline()),
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 1L, base.key(), "seg/other",
                    base.firstOffset(), base.recordCount(), base.via(), base.inline()),
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 1L, base.key(), base.segmentKey(),
                    2_000L, base.recordCount(), base.via(), base.inline()),
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 1L, base.key(), base.segmentKey(),
                    base.firstOffset(), 256, base.via(), base.inline()),
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 1L, base.key(), base.segmentKey(),
                    base.firstOffset(), base.recordCount(), base.via(), new byte[] {9, 8, 7}),
            // ⚠️ THE NINTH COMPONENT. Round-1 review of M5.15a found this set
            // still varying eight while the record had grown to nine -- every
            // variant carried the base's `sessionEpoch`, so the method name
            // stopped being true the moment the field was added.
            new SubscriptionEvent(base.session(), base.sequencerEpoch(), 99L, base.key(),
                    base.segmentKey(), base.firstOffset(), base.recordCount(), base.via(),
                    base.inline()),
        };
        for (SubscriptionEvent variant : variants) {
            assertThat(SubscriptionEvent.decode(variant.encode())).isEqualTo(variant);
            assertThat(variant).isNotEqualTo(base);
        }
    }

    /**
     * {@code via} survives on its own, against a base that differs in NOTHING
     * else.
     *
     * <p>⚠️ IT NEEDS ITS OWN BASE, and getting that wrong is why round 2 found
     * this still open. My first attempt appended a PROXY variant to the INLINE
     * fixture above -- but the pairing invariant forbids PROXY with bytes, so
     * that variant necessarily changed the inline payload TOO, and
     * {@code isNotEqualTo} was satisfied by the array comparison alone while
     * the {@code via} comparison stayed unreachable. Deleting
     * {@code && via == that.via} from {@code equals} still passed.
     *
     * <p>⚠️ PROXY AND DIRECT BOTH CARRY NO BYTES, so they differ in exactly one
     * component and are both legal. That is the only pair in this enum that
     * can isolate the field.
     *
     * <p>⚠️ AND IT MATTERS BEYOND THIS TEST: every round-trip assertion in all
     * three files is an {@code isEqualTo} against that hand-written
     * {@code equals}, so once {@code via} is not compared, a decoder returning
     * a CONSTANT mode passes the DIRECT golden decode too.
     */
    @Test
    void viaSurvivesTheRoundTripONITSOWN() throws Exception {
        SubscriptionEvent proxy = coordinatesEvent(FetchMode.PROXY);
        SubscriptionEvent direct = coordinatesEvent(FetchMode.DIRECT);

        assertThat(proxy).as("one component apart, and that component is `via`")
                .isNotEqualTo(direct);
        assertThat(SubscriptionEvent.decode(proxy.encode())).isEqualTo(proxy);
        assertThat(SubscriptionEvent.decode(direct.encode())).isEqualTo(direct);
        assertThat(SubscriptionEvent.decode(direct.encode())).isNotEqualTo(proxy);
    }

    /**
     * The two epochs move INDEPENDENTLY -- asserted both ways (criterion 12).
     *
     * <p>⚠️ CRITERION 12 SPELLS OUT BOTH DIRECTIONS and this test follows its
     * wording: "a sequencer failover does not invalidate a session, and a
     * session reset does not imply a failover -- asserted both ways". One
     * direction is half the property, and it is the half that looks fine: a
     * codec that wrote one field twice and never read the other would pass a
     * test that only ever varied the first.
     *
     * <p>⚠️ THE FORMAT CAN ONLY CARRY THE DISTINCTION, not enforce the
     * behaviour. That a failover genuinely leaves a session ALIVE is a property
     * of the session registry, which does not exist yet -- M5.15b owns it, and
     * M5.15c owns the reset half. What is provable here is that the two numbers
     * are separate fields that survive independently, which is the thing a
     * later conflation would have to defeat first.
     */
    @Test
    void theTwoEpochsMoveINDEPENDENTLYInBothDirections() throws Exception {
        SubscriptionEvent base = new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg/k",
                1_000L, 128, FetchMode.PROXY, new byte[0]);

        // A SEQUENCER FAILOVER: the term advances, the session is untouched.
        SubscriptionEvent afterFailover = new SubscriptionEvent("sess-abc", 43L, 3L, KEY, "seg/k",
                1_000L, 128, FetchMode.PROXY, new byte[0]);
        assertThat(SubscriptionEvent.decode(afterFailover.encode())).isEqualTo(afterFailover);
        assertThat(afterFailover.sessionEpoch())
                .as("a failover must not disturb the session's own counter")
                .isEqualTo(base.sessionEpoch());
        assertThat(afterFailover).isNotEqualTo(base);

        // A SESSION RESET: the session's counter advances, the term is untouched.
        SubscriptionEvent afterReset = new SubscriptionEvent("sess-abc", 42L, 4L, KEY, "seg/k",
                1_000L, 128, FetchMode.PROXY, new byte[0]);
        assertThat(SubscriptionEvent.decode(afterReset.encode())).isEqualTo(afterReset);
        assertThat(afterReset.sequencerEpoch())
                .as("and a reset must not read as a failover")
                .isEqualTo(base.sequencerEpoch());
        assertThat(afterReset).isNotEqualTo(base);

        // ⚠️ AND THE TWO CHANGES ARE DISTINGUISHABLE AFTER A ROUND TRIP, which
        // is what "different counters" means operationally: an observer holding
        // both ENCODED events can say which happened. Round-1 review found this
        // block asserting `43 != 42` and `3 != 4` through accessors on records
        // built from those literals three lines above -- nothing decoded, so no
        // mutation could red it.
        SubscriptionEvent failoverBack = SubscriptionEvent.decode(afterFailover.encode());
        SubscriptionEvent resetBack = SubscriptionEvent.decode(afterReset.encode());
        assertThat(failoverBack.sequencerEpoch())
                .as("the failover moved the term, on the wire")
                .isEqualTo(43L);
        assertThat(failoverBack.sessionEpoch()).isEqualTo(3L);
        assertThat(resetBack.sequencerEpoch())
                .as("and the reset did not, on the wire")
                .isEqualTo(42L);
        assertThat(resetBack.sessionEpoch()).isEqualTo(4L);
    }

    /**
     * A session epoch survives the round trip on its own.
     *
     * <p>⚠️ ITS OWN BASE, for the reason round 2 of M5.14 taught: a variant that
     * moves two components lets {@code isNotEqualTo} be satisfied by the other
     * one, and the field under test never becomes load-bearing.
     */
    @Test
    void theSessionEpochSurvivesTheRoundTripONITSOWN() throws Exception {
        SubscriptionEvent one = new SubscriptionEvent("s", 1L, 7L, KEY, "k", 0L, 1,
                FetchMode.PROXY, new byte[0]);
        SubscriptionEvent two = new SubscriptionEvent("s", 1L, 8L, KEY, "k", 0L, 1,
                FetchMode.PROXY, new byte[0]);

        assertThat(one).isNotEqualTo(two);
        assertThat(SubscriptionEvent.decode(one.encode())).isEqualTo(one);
        assertThat(SubscriptionEvent.decode(two.encode())).isEqualTo(two);
        assertThat(SubscriptionEvent.decode(two.encode())).isNotEqualTo(one);
    }

    /** A negative session epoch is not a request order. */
    @Test
    void aNEGATIVESessionEpochIsREFUSED() {
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, -1L, KEY, "k", 0L, 1,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a request order");
    }

    /**
     * The extremes round-trip: offset 0, and the largest range that fits.
     *
     * <p>⚠️ AN EARLIER VERSION OF THIS JAVADOC CLAIMED `Long.MAX_VALUE`
     * EXERCISED A TEN-BYTE VARINT. Round-1 review showed that is arithmetically
     * false -- it is NINE bytes, and a ten-byte uvarint needs bit 63 set, which
     * the constructor rejects. The transitions that actually change the
     * encoding width are 127/128 and 16383/16384, and they are covered in
     * {@code SubscriptionEventMalformedTest} rather than here.
     *
     * <p>⚠️ AND THIS TEST ONCE DODGED A REAL DEFECT: with `recordCount = 1` it
     * used the single value at that offset where `firstOffset + recordCount`
     * still fits a long. Review measured 128 records from
     * `Long.MAX_VALUE - 1` giving a NEGATIVE last offset. The constructor now
     * refuses that range, and {@code SubscriptionEventMalformedTest} pins it.
     */
    @Test
    void theEXTREMEOffsetsRoundTrip() throws Exception {
        SubscriptionEvent zero = new SubscriptionEvent("s", 0L, 1L, KEY, "k", 0L, 1,
                FetchMode.PROXY, new byte[0]);
        assertThat(SubscriptionEvent.decode(zero.encode())).isEqualTo(zero);

        SubscriptionEvent huge = new SubscriptionEvent("s", Long.MAX_VALUE, 1L, KEY, "k",
                Long.MAX_VALUE - 1, 1, FetchMode.PROXY, new byte[0]);
        assertThat(SubscriptionEvent.decode(huge.encode())).isEqualTo(huge);
    }

    /**
     * An unknown version REFUSES; it does not skip.
     *
     * <p>⚠️ THE DIRECTION IS THE DECISION, and `wire-format-change` names it as
     * the item people get backwards. A consumer that ignored an event it could
     * not parse would report a clean stream while losing records; one that
     * refuses falls back to the commit log, which is what the log is for.
     */
    @Test
    void anUNKNOWNVersionIsREFUSED() throws Exception {
        byte[] bytes = inlineEvent().encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(4, 99);
        assertThatThrownBy(() -> SubscriptionEvent.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsupported subscription event version: 99");

        // ⚠️ AND BELOW THE RANGE, not only above it. Round-4 review MEASURED
        // `if (version > VERSION_2)` surviving all 260 tests: a body with the
        // correct MAGIC and a version field of ZERO -- which a zeroed or
        // partly-written header produces -- fell through and parsed as the v1
        // shape, returning a well-formed event. The shipped guard is a correct
        // exact-match test; what was missing is anything that would notice a
        // later DOWNWARD widening.
        // ⚠️ THIS IS THE THIRD TIME A ONE-SIDED BOUND HAS TURNED UP IN THIS
        // CODEC: `recordCount` and `partition` were both measured accepting a
        // bit-63 value at M5.14 because their guards checked only the upper
        // end, and `Checkpoint`/`MembershipFilter` were the precedent then too.
        for (int unknown : new int[] {0, -1, Integer.MIN_VALUE}) {
            byte[] below = inlineEvent().encode();
            ByteBuffer.wrap(below).order(ByteOrder.BIG_ENDIAN).putInt(4, unknown);
            assertThatThrownBy(() -> SubscriptionEvent.decode(below))
                    .as("version %d is not one this build writes", unknown)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("unsupported subscription event version");
        }
    }

    /** Foreign bytes are refused on the magic, before anything is parsed. */
    @Test
    void FOREIGNBytesAreRefusedOnTheMagic() throws Exception {
        byte[] bytes = inlineEvent().encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(0, 0xDEADBEEF);
        assertThatThrownBy(() -> SubscriptionEvent.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not a subscription event");

        assertThatThrownBy(() -> SubscriptionEvent.decode(new byte[] {1, 2, 3}))
                .as("and something too short to hold a header is refused as such")
                .isInstanceOf(IOException.class);
    }

    /**
     * A truncated event is an IOException, never a partly-populated one.
     *
     * <p>⚠️ TRUNCATION AT EVERY LENGTH, because a decoder that reads fields in
     * order fails differently depending on where the bytes stop -- and the one
     * outcome that must never happen is a value coming back.
     *
     * <p>⚠️ WHAT IT CONSTRAINS IS THIS CLASS NOT DEFEATING `Cursor`, not
     * `Cursor` itself. Round-1 review measured the distinction: clamping this
     * file's own `getString` to `c.remaining()` survives, because a clamped
     * read simply makes the NEXT field throw. The red recorded for this test is
     * a decoder that CATCHES the failure and returns a partial event -- the
     * lenient-parser defect -- which is the shape it genuinely forbids.
     */
    @Test
    void aTRUNCATEDEventNeverDecodesToAValue() {
        byte[] whole = inlineEvent().encode();
        for (int length = 8; length < whole.length; length++) {
            byte[] cut = java.util.Arrays.copyOf(whole, length);
            assertThatThrownBy(() -> SubscriptionEvent.decode(cut))
                    .as("truncated to %d of %d bytes", length, whole.length)
                    .isInstanceOf(IOException.class);
        }
    }

    /**
     * A truncated subscription frame names the SUBSCRIPTION, not the commit log.
     *
     * <p>⚠️ THE COST IS AN OPERATOR'S TIME, NOT DATA. {@code Cursor} is shared
     * with the chain shapes and its two bounds messages both said "chain entry
     * ends inside …" verbatim, so a short read on the subscription channel
     * produced text naming the commit log — a runbook grepping that string
     * sends someone to investigate commit-log corruption for a fault that is
     * entirely in the subscription channel. Round-3 review of M5.14 measured
     * the strings; M5.46 is the row that fixes them.
     *
     * <p>⚠️ ASSERTED AT EVERY TRUNCATION LENGTH, not at one, because the two
     * messages come from different guards — {@code uvarint()} and
     * {@code bytes(n)} — and which one fires depends on where the bytes stop.
     * A fix that renamed only the varint message would pass a single-length
     * case.
     */
    @Test
    void aTRUNCATEDFrameNamesTheSUBSCRIPTIONAndNeverTheCHAIN() {
        byte[] whole = inlineEvent().encode();
        for (int length = 8; length < whole.length; length++) {
            byte[] cut = java.util.Arrays.copyOf(whole, length);
            assertThatThrownBy(() -> SubscriptionEvent.decode(cut))
                    .as("truncated to %d of %d bytes", length, whole.length)
                    .isInstanceOf(IOException.class)
                    // ⚠️ BOTH HALVES. The negative alone is satisfied by
                    // passing "checkpoint" at the call site -- which review
                    // measured, and which REINSTATES the defect pointing at the
                    // sequencer instead -- or by passing "" , or by hardcoding
                    // the bare "ends inside a varint" back into `Cursor`.
                    .hasMessageContaining("subscription event")
                    .hasMessageNotContaining("chain entry");
        }
    }

    /**
     * The via/inline pairing is enforced, and over the wire it is a PARSE
     * failure.
     *
     * <p>⚠️ A CALLER DECODING UNTRUSTED BYTES MUST NOT CATCH TWO KINDS OF
     * THING. The constructor's invariants are the format's invariants, so a
     * violation arriving from outside is an {@code IOException} rather than an
     * {@code IllegalArgumentException} escaping a method that declares neither.
     */
    @Test
    void theVIAAndINLINEPairingIsEnforcedInBothDirections() {
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, KEY, "k", 0L, 1,
                        FetchMode.INLINE, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("via=INLINE carries the bytes");

        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, KEY, "k", 0L, 1,
                        FetchMode.PROXY, new byte[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not carry bytes");
    }

    /** The constructor refuses what its messages say it refuses. */
    @Test
    void theConstructorREFUSESWhatItsMessagesSay() {
        assertThatThrownBy(() -> new SubscriptionEvent("  ", 1L, 1L, KEY, "k", 0L, 1,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank session");
        assertThatThrownBy(() -> new SubscriptionEvent("s", -1L, 1L, KEY, "k", 0L, 1,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a term");
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, KEY, "k", -1L, 1,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("never negative");
        assertThatThrownBy(() -> new SubscriptionEvent("s", 1L, 1L, KEY, "k", 0L, 0,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a push of nothing");
        assertThatThrownBy(() -> new SubscriptionEvent(null, 1L, 1L, KEY, "k", 0L, 1,
                        FetchMode.PROXY, new byte[0]))
                .isInstanceOf(NullPointerException.class).hasMessage("session");
    }

    /**
     * {@code hashCode} agrees with {@code equals}, including on the array.
     *
     * <p>⚠️ ROUND-3 REVIEW MEASURED IT CONSTRAINED BY NOTHING: replacing the
     * body with the record's generated form -- which hashes {@code byte[]} by
     * IDENTITY -- passed all 28 tests. That is not cosmetic. It breaks the
     * equals/hashCode contract, so two events that ARE equal hash differently,
     * and a decoded event is not found in a {@code HashSet} holding an equal
     * one. A dedup, a {@code Map<SubscriptionEvent, ...>} or a
     * {@code distinct()} on the serving path would then silently double-deliver
     * or drop while {@code equals} still says the two are the same.
     *
     * <p>⚠️ {@code equals} GAINED EIGHT PINS THIS ROUND AND ITS PARTNER GOT
     * NONE, which is how the pair drifts apart.
     */
    @Test
    void hashCodeAGREESWithEqualsIncludingOnTheArray() throws Exception {
        SubscriptionEvent original = inlineEvent();
        SubscriptionEvent roundTripped = SubscriptionEvent.decode(original.encode());

        assertThat(roundTripped).isEqualTo(original);
        assertThat(roundTripped.hashCode())
                .as("equal events must hash equally, or a HashSet loses one of them")
                .isEqualTo(original.hashCode());
        assertThat(java.util.Set.of(original).contains(roundTripped))
                .as("which is the property that actually bites, stated as the thing it breaks")
                .isTrue();
    }

    /**
     * The inline bytes are COPIED in and out.
     *
     * <p>⚠️ A RECORD'S ARRAY COMPONENT IS SHARED BY DEFAULT, so a caller that
     * kept its array could rewrite an event after constructing it, and a caller
     * that reads {@code inline()} could rewrite one it was handed. On the
     * serving path that is another stream's records appearing under this
     * stream's offsets.
     */
    @Test
    void theInlineBytesAreCOPIEDInAndOut() {
        byte[] mine = {1, 2, 3};
        SubscriptionEvent event = new SubscriptionEvent("s", 1L, 1L, KEY, "k", 0L, 1,
                FetchMode.INLINE, mine);
        mine[0] = 99;
        assertThat(event.inline()).as("the constructor copied").containsExactly(1, 2, 3);

        event.inline()[0] = 77;
        assertThat(event.inline()).as("and the accessor copies too").containsExactly(1, 2, 3);
    }

    /**
     * {@code toString} carries no document payload (security.md rule 4).
     *
     * <p>⚠️ AN EVENT IS EXACTLY WHAT SOMEONE PRINTS while debugging a delivery,
     * and its inline field is a document payload -- which rule 4 names beside
     * credentials and signed URLs.
     */
    @Test
    void toStringCarriesNODocumentPayload() {
        byte[] secret = "TOPSECRETDOCUMENTBODY".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // ⚠️ DISTINCT VALUES FOR THE TWO EPOCHS. Round-3 review MEASURED that
        // with both at 1, swapping the two LABELS in `toString` survived --
        // `sequencerEpoch=1` and `sessionEpoch=1` are both present whichever
        // value sits under whichever name. This is the only test in the tree
        // asserting `toString`, so nothing else could catch it, and the line it
        // pins is the one this row's own comment calls "the line that has to
        // answer 'was that a failover or a reset?'". Answering it BACKWARDS is
        // worse than not answering it at all.
        SubscriptionEvent event = new SubscriptionEvent("s", 1L, 2L, KEY, "k", 0L, 1,
                FetchMode.INLINE, secret);
        assertThat(event.toString())
                .as("the payload is the thing rule 4 forbids")
                .doesNotContain("TOPSECRET")
                .contains("21 bytes");
        // ⚠️ EVERY OTHER COMPONENT, because round-1 review measured this half
        // checking three of eight: dropping `key`, `segmentKey`, `firstOffset`,
        // `recordCount` and `via` from `toString` passed. That is the
        // ADR-0041 property in reverse -- a refusal that names nothing is rule
        // 4 obeyed by making the system undebuggable, which is the version
        // someone quietly breaks back.
        assertThat(event.toString())
                .as("and everything a reader needs to identify the delivery is present")
                .contains("session=s")
                .contains("sequencerEpoch=1")
                // ⚠️ AND THE SESSION EPOCH, which is the field that makes the
                // printed line able to answer "failover or reset?" -- the
                // question this row exists to make answerable. Round-1 review
                // found `equals` and `hashCode` had gained it and `toString`
                // had not; adding it to the class without adding it here would
                // have left the omission free to come back.
                .contains("sessionEpoch=2")
                .contains(KEY.toString())
                .contains("segmentKey=k")
                .contains("firstOffset=0")
                .contains("recordCount=1")
                .contains("via=INLINE");
    }

    /** {@code lastOffset} is inclusive, matching {@code SubscriptionHub.Push}. */
    @Test
    void lastOffsetIsINCLUSIVE() {
        assertThat(new SubscriptionEvent("s", 1L, 1L, KEY, "k", 1_000L, 128,
                FetchMode.PROXY, new byte[0]).lastOffset()).isEqualTo(1_127L);
    }
}
