// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * One push, as the subscription protocol puts it on a wire (FR-6, M5.14).
 *
 * <p>⚠️ A SESSION AND A SEQUENCER EPOCH, which is what M5's completion
 * condition asks the push channel to carry. The SESSION identifies a
 * subscription across reconnects, so a consumer that drops and returns can say
 * where it was; the SEQUENCER EPOCH tells it the sequencer changed underneath,
 * which is a different fact from "you missed something" and must not be
 * inferred from an offset gap.
 *
 * <p>⚠️ THERE ARE TWO EPOCHS AND THIS IS THE SEQUENCER'S. M5's SPEC criterion
 * 12 names conflating them as "the obvious defect", and round-1 review caught
 * this record doing exactly that -- shipping the sequencer term while citing
 * research doc 04 §2d, which describes KIP-227's SESSION epoch: a counter that
 * orders concurrent requests within one session and makes retries idempotent.
 * The field is named for which one it is so the second cannot be added by
 * accident on top of it. M5.15 owns resume and keeping them distinct.
 *
 * <p>⚠️ THERE IS NO OLD SHAPE, AND SAYING SO IS PART OF THE OBLIGATION.
 * {@code wire-format-change}'s checklist asks what happens to bytes already in
 * the bucket and requires the read side to ship first, in an earlier commit.
 * Both assume a deployed writer. Nothing has ever serialized a subscription
 * event: {@code SubscriptionTransport} is an in-process seam,
 * {@code ConsumerClient} decodes SEGMENT bytes rather than events, and the
 * production transport (M1.11b) is still unbuilt. So this format has no bytes
 * in any bucket -- the protocol is not persisted at all -- and no old peer
 * speaks it. The exemption is recorded rather than the item skipped, the way
 * M5.10 recorded that a Java interface has no serialized form.
 *
 * <p>⚠️ THE VERSION IS STILL WRITTEN AND AN UNKNOWN ONE STILL REFUSES, because
 * the first real rollout needs the discriminator to already be there. Adding it
 * later is the change that cannot be made compatibly.
 *
 * <p>⚠️ AN UNKNOWN VERSION STOPS; IT DOES NOT SKIP. {@code ChainEntry} decided
 * this for the commit log and the reason carries: a consumer that skipped an
 * event it could not parse would silently lose records and report a clean
 * stream, where one that refuses falls back to the commit log, which is what
 * the log is for. ⚠️ The contrast the skill names is the KEY FILTER, where an
 * unknown tag means READ THE HEADER rather than NO MATCH -- there, skipping
 * drops data; here, continuing does.
 *
 * @param session identifies this subscription across reconnects. ⚠️ Opaque to
 *     the consumer: it is the ingester's to mint and the consumer's to echo
 * @param sequencerEpoch the sequencer TERM this event was produced under.
 *     ⚠️ A CHANGE means the sequencer moved, not that records were lost.
 *     ⚠️ NAMED FOR WHICH EPOCH IT IS, because there are two and M5's SPEC
 *     criterion 12 calls conflating them "the obvious defect". This is the
 *     SEQUENCER's term, from the chain key. Research doc 04 §2d describes a
 *     different counter -- KIP-227's SESSION epoch, which orders concurrent
 *     requests within one session and makes retries idempotent -- and an
 *     earlier draft of this record shipped the sequencer term while citing
 *     that section for it. When the session epoch arrives it is a second
 *     field, not this one
 * @param key which stream advanced
 * @param segmentKey the object the records live in
 * @param firstOffset the offset of the first record in this push
 * @param recordCount how many records this push covers
 * @param via how the consumer gets the bytes -- {@code inline} carries them
 *     here, {@code proxy} and {@code direct} do not
 * @param inline the bytes when {@code via} is {@code INLINE}, empty otherwise.
 *     ⚠️ EMPTY RATHER THAN NULL, so a decoder never has to distinguish
 *     "absent" from "zero-length" -- the case a nullable field gets wrong
 */
public record SubscriptionEvent(String session, long sequencerEpoch, RunKey key, String segmentKey,
        long firstOffset, int recordCount, FetchMode via, byte[] inline) {

    /** ⚠️ Distinct from {@code ChainEntry}'s: a different protocol, a different namespace. */
    public static final int MAGIC = 0x42535542;

    /** The only version that has ever existed. */
    public static final int VERSION_1 = 1;

    public SubscriptionEvent {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(via, "via");
        Objects.requireNonNull(inline, "inline");
        if (session.isBlank()) {
            throw new IllegalArgumentException("a blank session identifies no subscription");
        }
        if (sequencerEpoch < 0) {
            throw new IllegalArgumentException("an epoch of " + sequencerEpoch + " is not a term");
        }
        if (firstOffset < 0) {
            throw new IllegalArgumentException("offsets are never negative");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("a push of nothing is not a push");
        }
        // ⚠️ THE LAST OFFSET MUST EXIST. An event whose range runs past
        // Long.MAX_VALUE describes offsets no consumer can address, and
        // `lastOffset()` would return a NEGATIVE number that compares as
        // earlier than the first -- so a consumer would see the range run
        // backwards rather than see an error.
        // ⚠️ `recordCount - 1`, because the LAST offset is
        // `firstOffset + recordCount - 1`. Round-2 review measured the earlier
        // form rejecting `firstOffset = Long.MAX_VALUE, recordCount = 1`, whose
        // last offset is exactly Long.MAX_VALUE and does not overflow.
        if (firstOffset > Long.MAX_VALUE - (recordCount - 1)) {
            throw new IllegalArgumentException(
                    "a push of " + recordCount + " from " + firstOffset + " runs past the last "
                            + "addressable offset");
        }
        // ⚠️ THE PAIRING IS VALIDATED, not left to the caller. `via=PROXY` with
        // bytes attached would make the consumer's behaviour depend on which
        // field it happened to trust, and `via=INLINE` with none is a delivery
        // that decodes to nothing -- the silent record loss `SubscriptionHub`'s
        // own javadoc already warns about for the empty-array case.
        if (via == FetchMode.INLINE && inline.length == 0) {
            throw new IllegalArgumentException(
                    "via=INLINE carries the bytes, and none are attached");
        }
        if (via != FetchMode.INLINE && inline.length > 0) {
            throw new IllegalArgumentException(
                    "via=" + via + " does not carry bytes, but " + inline.length + " are attached");
        }
        inline = inline.clone();
    }

    /** ⚠️ Defensive, matching the constructor: a record's array is otherwise shared. */
    @Override
    public byte[] inline() {
        return inline.clone();
    }

    /**
     * The last offset this push covers.
     *
     * <p>⚠️ THE SUM IS BOUNDED AT CONSTRUCTION, not here, so this cannot
     * overflow. Round-1 review MEASURED the unguarded version returning
     * -9223372036854775683 for {@code firstOffset = Long.MAX_VALUE - 1} with
     * {@code recordCount = 128} -- and my own extremes test stepped around it
     * by using a record count of 1, the single value at that offset where the
     * sum still fits.
     */
    public long lastOffset() {
        return firstOffset + recordCount - 1;
    }

    /**
     * ⚠️ GENERATED equals COMPARES ARRAYS BY IDENTITY, so a round-trip would
     * never be equal to its original. The round-trip property is the point of
     * having a codec at all, so this is overridden rather than worked around in
     * every test that needs it.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SubscriptionEvent that)) {
            return false;
        }
        return sequencerEpoch == that.sequencerEpoch && firstOffset == that.firstOffset
                && recordCount == that.recordCount
                && session.equals(that.session) && key.equals(that.key)
                && segmentKey.equals(that.segmentKey) && via == that.via
                && java.util.Arrays.equals(inline, that.inline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(session, sequencerEpoch, key, segmentKey, firstOffset, recordCount, via)
                * 31 + java.util.Arrays.hashCode(inline);
    }

    /**
     * ⚠️ THE INLINE BYTES ARE NOT PRINTED, and neither is their content
     * summarised beyond a length. security.md rule 4 forbids document payloads
     * in a log or an error message, and an event is exactly the object someone
     * prints while debugging a delivery.
     */
    @Override
    public String toString() {
        return "SubscriptionEvent[session=" + session + ", sequencerEpoch=" + sequencerEpoch + ", key=" + key
                + ", segmentKey=" + segmentKey + ", firstOffset=" + firstOffset
                + ", recordCount=" + recordCount + ", via=" + via
                + ", inline=" + inline.length + " bytes]";
    }

    /** The 8-byte header every version shares: magic, then version. */
    static byte[] header(int version) {
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(version);
        return head.array();
    }

    /** This event's bytes, at {@link #VERSION_1}. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(header(VERSION_1));
        putString(out, session);
        SegmentWriter.putUvarint(out, sequencerEpoch);
        // ⚠️ THE UUID AS ITS TWO LONGS, not as text. A `UUID.toString` costs 36
        // bytes where 16 do, on an event sent per stream per flush -- and the
        // textual form invites a parser that accepts any string shape.
        putLongBE(out, key.indexId().getMostSignificantBits());
        putLongBE(out, key.indexId().getLeastSignificantBits());
        SegmentWriter.putUvarint(out, key.partitionId());
        putString(out, segmentKey);
        SegmentWriter.putUvarint(out, firstOffset);
        SegmentWriter.putUvarint(out, recordCount);
        // ⚠️ THE ORDINAL IS NOT WRITTEN. Reordering the enum would silently
        // change every encoded event; the NAME survives a reorder and fails
        // loudly on a rename, which is the direction that can be fixed.
        putString(out, via.name());
        SegmentWriter.putUvarint(out, inline.length);
        out.writeBytes(inline);
        return out.toByteArray();
    }

    /**
     * Reads one event.
     *
     * @throws IOException on a foreign magic, an unknown version, or bytes that
     *     do not parse -- never a partially-populated event
     */
    public static SubscriptionEvent decode(byte[] bytes) throws IOException {
        // ⚠️ A TRUNCATED FRAME REPORTS ITSELF AS A CHAIN ENTRY, and round-3
        // review measured the strings: `Cursor` has exactly two messages, "chain
        // entry ends inside a varint" and "...inside a field", and this class is
        // its fourth caller and the first that is not a chain shape. An operator
        // -- or a runbook grepping that text -- would investigate commit-log
        // corruption for a fault entirely in the subscription channel.
        // ⚠️ NOT FIXED HERE ON PURPOSE: `Cursor` is shared by three chain
        // formats, so re-wording it is their change as much as this one's, and
        // wrapping every read to re-message it would bury the guards this class
        // just spent three rounds getting right. M5.46 owns it.
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length < 8) {
            throw new IOException("a subscription event is at least 8 bytes, got " + bytes.length);
        }
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int magic = b.getInt(0);
        if (magic != MAGIC) {
            throw new IOException("not a subscription event: magic " + Integer.toHexString(magic));
        }
        int version = b.getInt(4);
        if (version != VERSION_1) {
            // ⚠️ REFUSED, NOT SKIPPED -- see the class javadoc. A consumer that
            // ignored an event it could not read would report a clean stream
            // while losing records.
            throw new IOException("unsupported subscription event version: " + version);
        }
        Cursor c = new Cursor(bytes, 8);
        String session = getString(c);
        long sequencerEpoch = c.uvarint();
        long indexHi = getLongBE(c);
        long indexLo = getLongBE(c);
        long partition = c.uvarint();
        String segmentKey = getString(c);
        long firstOffset = c.uvarint();
        long recordCount = c.uvarint();
        String via = getString(c);
        // ⚠️ `Cursor.bytes` DOES THE BOUNDS CHECK, and its own comment records
        // why the obvious form is wrong: `i + n` overflows int for a large
        // length field and lets through the very allocation the guard exists to
        // prevent. Reimplementing that check here would reintroduce the bug the
        // class already fixed once.
        long inlineLength = c.uvarint();
        if (inlineLength < 0 || inlineLength > Integer.MAX_VALUE) {
            throw new IOException("inline length " + inlineLength + " does not fit an array");
        }
        byte[] inline = c.bytes((int) inlineLength);
        // ⚠️ TRAILING BYTES ARE A REFUSAL, matching every other decoder here:
        // `ChainEntry`, `Checkpoint` and `SegmentReader` all end with this
        // check. Bytes after a complete event mean the sender and this reader
        // disagree about the shape -- a v2 field this build cannot see, or a
        // framing bug that has run two events together -- and accepting the
        // prefix silently is how a consumer processes half a stream and reports
        // success. Round-1 review found it missing while the class javadoc
        // named `ChainEntry` as its precedent.
        if (!c.atEnd()) {
            throw new IOException("trailing bytes after a complete subscription event");
        }

        FetchMode mode;
        try {
            mode = FetchMode.valueOf(via);
        } catch (IllegalArgumentException unknownMode) {
            throw new IOException("unknown fetch mode on a subscription event: " + via);
        }
        // ⚠️ BOTH BOUNDS. `Cursor.uvarint` returns a long, so a ten-byte
        // varint with bit 63 set returns a NEGATIVE one -- for which
        // `> Integer.MAX_VALUE` is false, and the cast below then keeps the low
        // 32 bits. Round-2 review MEASURED `0x8000000000000064` decoding with
        // no error to a record count of 100. `Checkpoint` and
        // `MembershipFilter` in this package both write `< 0 ||` for exactly
        // this, and this class names them as its precedent.
        if (recordCount < 0 || recordCount > Integer.MAX_VALUE) {
            throw new IOException("record count " + recordCount + " does not fit a push");
        }
        // ⚠️ BOTH BOUNDS, same reason: measured `0x8000000000000003` decoding
        // to partition 3, which delivers one stream's records under another
        // stream's partition and reports a clean stream.
        if (partition < 0 || partition > Integer.MAX_VALUE) {
            throw new IOException("partition " + partition + " does not fit a RunKey");
        }
        try {
            return new SubscriptionEvent(session, sequencerEpoch,
                    new RunKey(new java.util.UUID(indexHi, indexLo), (int) partition), segmentKey,
                    firstOffset, (int) recordCount, mode, inline);
        } catch (IllegalArgumentException malformed) {
            // ⚠️ The constructor's invariants are the FORMAT's invariants, so a
            // violation arriving over the wire is a parse failure rather than a
            // programming error -- a caller decoding untrusted bytes must not
            // have to catch two kinds of thing.
            throw new IOException("malformed subscription event: " + malformed.getMessage(),
                    malformed);
        }
    }

    private static void putLongBE(ByteArrayOutputStream out, long value) {
        out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array());
    }

    private static long getLongBE(Cursor c) throws IOException {
        return ByteBuffer.wrap(c.bytes(8)).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static void putString(ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    private static String getString(Cursor c) throws IOException {
        long length = c.uvarint();
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IOException("string length " + length + " does not fit an array");
        }
        return new String(c.bytes((int) length), StandardCharsets.UTF_8);
    }
}
