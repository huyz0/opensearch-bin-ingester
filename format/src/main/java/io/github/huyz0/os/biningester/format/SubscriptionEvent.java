// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * One live push or catch-up event, as the subscription protocol puts it on a
 * wire (FR-6, M5.14; M9.41).
 *
 * <p>⚠️ A SESSION AND A SEQUENCER EPOCH, which is what M5's completion
 * condition asks the push channel to carry. The SESSION identifies a
 * subscription across reconnects, so a consumer that drops and returns can say
 * where it was; the SEQUENCER EPOCH tells it the sequencer changed underneath,
 * which is a different fact from "you missed something" and must not be
 * inferred from an offset gap.
 *
 * <p>⚠️ BOTH EPOCHS ARE HERE NOW, AND THEY ARE DIFFERENT COUNTERS.
 * {@code sequencerEpoch} is the chain key's TERM; {@code sessionEpoch} is
 * KIP-227's, ordering concurrent requests within one session. SPEC criterion 12
 * requires the distinction ASSERTED BOTH WAYS: a sequencer failover does not
 * invalidate a session, and a session reset does not imply a failover.
 *
 * <p>⚠️ M5.14 SHIPPED ONLY THE FIRST AND MIS-CITED IT as research doc 04 §2d's
 * -- which is the session epoch -- and round-1 review caught it. Naming the
 * field for which epoch it was is what made adding the second an addition
 * rather than a reinterpretation. ⚠️ THE FORMAT CAN ONLY CARRY THE
 * DISTINCTION; whether a failover actually leaves a session alive is
 * behavioural and needs the session registry, which is M5.15b's.
 *
 * <p>⚠️ THERE IS AN OLD SHAPE NOW, AND THERE DID NOT USED TO BE. M5.14 shipped
 * v1 and recorded honestly that "bytes already in the bucket", "ship the read
 * side first" and "golden files for the old AND new shape" had NO SUBJECT --
 * nothing had ever serialized a subscription event. M5.15a added
 * {@link #sessionEpoch}, so v1 is a real previous shape with real stored bytes
 * ({@code subscription-event-*-v1.bin}) and this decoder reads both.
 *
 * <p>⚠️ THIS PARAGRAPH SURVIVED THREE ROUNDS OF REVIEW SAYING THE OPPOSITE, and
 * how is worth recording. Round one found it; my fix was a SILENT NO-OP,
 * because I searched for "rather than the event" where the file said "rather
 * than events" and {@code String.replace} returns the string unchanged. Round
 * two found it again; that fix applied and was then REVERTED by a
 * mutation-testing restore from a snapshot taken before it. Round three found
 * it a third time. What a reader does with the stale version is concrete: told
 * no bytes in this shape exist, they delete the
 * {@code version == VERSION_2 ? c.uvarint() : SESSION_EPOCH_ABSENT} branch or
 * regenerate the v1 fixtures, and every v1 event becomes unreadable.
 *
 * <p>⚠️ "SHIP THE READ SIDE FIRST, IN AN EARLIER COMMIT" IS STILL EXEMPT, and
 * that half has not changed: the reader that understands both shapes and the
 * writer that emits v2 land together, and no peer has ever spoken either shape
 * across a process, so there is nobody to lag behind.
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
 * @param sessionEpoch KIP-227's counter: it orders concurrent requests WITHIN
 *     one session and makes a retry idempotent. ⚠️ NOT THE SEQUENCER'S TERM,
 *     and SPEC criterion 12 calls conflating the two "the obvious defect" --
 *     a sequencer failover must not invalidate a session, and a session reset
 *     must not imply a failover. They move independently and neither can be
 *     derived from the other. ⚠️ Numbered from 1 by whatever mints them, which
 *     is M5.15b's. {@link #SESSION_EPOCH_ABSENT} means the event carries no
 *     session epoch -- USUALLY a v1 writer that had no such field, and possibly
 *     a current writer that failed to set one, because {@code encode} emits v1
 *     for an absent epoch. ⚠️ DO NOT READ IT AS "old peer": see
 *     {@link #encode()} for why the two are indistinguishable on the wire
 * @param sequencerEpoch the sequencer TERM this event was produced under.
 *     ⚠️ A CHANGE means the sequencer moved, not that records were lost.
 *     ⚠️ NAMED FOR WHICH EPOCH IT IS, because there are two and M5's SPEC
 *     criterion 12 calls conflating them "the obvious defect". This is the
 *     SEQUENCER's term, from the chain key. Research doc 04 §2d describes a
 *     different counter -- KIP-227's SESSION epoch, which orders concurrent
 *     requests within one session and makes retries idempotent -- and an
 *     earlier draft of this record shipped the sequencer term while citing
 *     that section for it. ⚠️ It ARRIVED in M5.15a as a SECOND field --
 *     {@code sessionEpoch}, documented above -- rather than as a
 *     reinterpretation of this one, which is what naming this field for its own
 *     epoch bought
 * @param key which stream advanced
 * @param segmentKey the object the records live in
 * @param firstOffset the offset of the first record in this push
 * @param recordCount how many records this push covers
 * @param via how the consumer gets the bytes -- {@code inline} carries them
 *     here, {@code proxy} and {@code direct} do not
 * @param inline the bytes when {@code via} is {@code INLINE}, empty otherwise.
 *     ⚠️ EMPTY RATHER THAN NULL, so a decoder never has to distinguish
 *     "absent" from "zero-length" -- the case a nullable field gets wrong
 * @param grant the signed URL a {@code direct} consumer fetches with, or
 *     {@code null}. ⚠️ THE CORRESPONDENCE WITH {@code via} IS NOT CHECKED HERE,
 *     and an earlier draft of this line stated it flatly as though it were:
 *     the compact constructor refuses a grant without a session epoch and a
 *     range without a grant, and nothing refuses a grant on an {@code INLINE}
 *     event. {@code SubscriptionHub.Push} does enforce it one layer up, with a
 *     case pinning each direction
 * @param byteStart the run's first byte within the segment, or
 *     {@link #RANGE_ABSENT}. ⚠️ **NO READER IN THE TREE CONSUMES THIS**, and
 *     ADR-0044 keeps it anyway with the reason written down: a bounded read is
 *     TWO expressible requests -- the header range, which is what
 *     {@code SegmentKey}'s {@code h<headerLen>} is for, then this run's slice
 *     -- and what is missing is only a {@code SegmentReader} entry point taking
 *     a directory and a slice separately. ⚠️ A SLICE-ONLY GET would not do,
 *     because {@code u32 codecFlags} lives per run in the DIRECTORY and DATA
 *     carries no codec id; fetching the header first is what supplies it.
 *     ⚠️ THIS FIELD DOES NOT MAKE THAT READ POSSIBLE -- {@code RunEntry}
 *     carries the same extent in the DIRECTORY, which a bounded reader has
 *     already fetched. What it buys is ONE ROUND TRIP: the slice GET can be
 *     issued WITHOUT waiting for the header. The reader that would use it is
 *     **M5.66**, which also owns whether one RTT is worth the price below. ⚠️ ADR-0043 said these buy
 *     "coalescing without a directory read", which needs a ranged GET and is
 *     therefore FALSE -- ADR-0044 replaced that justification rather than
 *     repairing it, and priced carrying them at **5.4% of the subscription
 *     stream** -- ADR-0044 derives that number once and this line deliberately
 *     does not re-derive it, because three review rounds went on copies of it
 *     disagreeing. ⚠️ It is MODELLED at the 8 MiB / ~1,600-run segment, not
 *     measured; what is measured is the golden pair, 128 bytes with a range
 *     against 123 without. ⚠️ NOT to be confused with {@code RunEntry}'s fields of the
 *     same name, which are the segment DIRECTORY's own coordinates and are read
 *     on every decode
 * @param byteLen the run's length in bytes, or {@link #RANGE_ABSENT}. See
 *     {@code byteStart} -- the two are absent together or present together
 * @param chainSequence the commit-chain sequence in v4, or
 *     {@link #CHAIN_SEQUENCE_ABSENT} for v1-v3 and unsequenced fixtures
 */
public record SubscriptionEvent(String session, long sequencerEpoch, long sessionEpoch,
        RunKey key, String segmentKey, long firstOffset, int recordCount, FetchMode via,
        byte[] inline, Grant grant, long byteStart, long byteLen, long chainSequence) {

    /** What a v1-v3 frame carries when the commit-chain sequence is absent. */
    public static final long CHAIN_SEQUENCE_ABSENT = -1L;

    /** Existing call sites describe legacy-shaped events until they supply a chain sequence. */
    public SubscriptionEvent(String session, long sequencerEpoch, long sessionEpoch,
            RunKey key, String segmentKey, long firstOffset, int recordCount, FetchMode via,
            byte[] inline, Grant grant, long byteStart, long byteLen) {
        this(session, sequencerEpoch, sessionEpoch, key, segmentKey, firstOffset, recordCount,
                via, inline, grant, byteStart, byteLen, CHAIN_SEQUENCE_ABSENT);
    }

    /**
     * An event with no grant and no chain sequence, which retains the v1/v2
     * constructor shape used by existing call sites.
     *
     * <p>⚠️ IT EXISTS SO THE GRANT COST NO CALL SITES. A record may declare
     * constructors beside its canonical one, so the thirty-eight existing
     * {@code new SubscriptionEvent(...)} sites keep compiling rather than being
     * swept for three values every one of them would have written the same --
     * and sweeping them is how a fixture silently stops testing what it did,
     * which M5.43's review measured happening to a case two hundred lines from
     * anything its diff touched.
     */
    public SubscriptionEvent(String session, long sequencerEpoch, long sessionEpoch,
            RunKey key, String segmentKey, long firstOffset, int recordCount, FetchMode via,
            byte[] inline) {
        this(session, sequencerEpoch, sessionEpoch, key, segmentKey, firstOffset, recordCount,
                via, inline, null, RANGE_ABSENT, RANGE_ABSENT, CHAIN_SEQUENCE_ABSENT);
    }

    /**
     * No byte range, which is every event without a grant and any grant scoped
     * to a whole object.
     *
     * <p>⚠️ {@code -1}, NOT {@code 0}, because {@code byteStart = 0} is the
     * first byte of a real segment and {@code byteLen = 0} is an empty range --
     * both legitimate values a sentinel must not collide with.
     * {@link #SESSION_EPOCH_ABSENT} could use {@code 0} because epoch zero is
     * not a live session; a byte offset has no such spare value.
     */
    public static final long RANGE_ABSENT = -1L;

    /** ⚠️ Distinct from {@code ChainEntry}'s: a different protocol, a different namespace. */
    public static final int MAGIC = 0x42535542;

    /**
     * The first shape: no session epoch.
     *
     * <p>⚠️ STILL DECODABLE, and that is the point of having written a version
     * byte before there was anything to discriminate. M5.14 recorded "golden
     * files for the old AND new shape" as having no subject because nothing had
     * ever been serialized; two v1 golden files are now in the tree, so the
     * item has one and is met rather than waived.
     */
    public static final int VERSION_1 = 1;

    /** Adds {@link #sessionEpoch}. Written by {@link #encode}. */
    public static final int VERSION_2 = 2;

    /**
     * The third shape: a {@code direct} grant, and optionally its byte range.
     *
     * <p>⚠️ A GRANT IMPLIES A SESSION EPOCH, so a v3 body is a v2 body plus the
     * grant rather than a v1 body plus two things. That keeps {@code decode}
     * linear and each golden file one field from its predecessor.
     */
    public static final int VERSION_3 = 3;

    /** Adds an explicit grant-presence byte, an optional grant, and the required chain sequence. */
    public static final int VERSION_4 = 4;

    /**
     * What a v1 event's session epoch reads as: there was no such field.
     *
    ABSENT IS NOT ZERO-THE-NUMBER. Collapsing the two would
     * make a v1 event indistinguishable from a session at its very first
     * request, and a consumer resuming on it would claim a position it was
     * never given.
     *
     * <p>⚠️ "A LIVE SESSION NEVER USES THIS VALUE" IS AN INVARIANT THIS CLASS
     * DOES NOT PROVIDE, and an earlier version of this paragraph stated it
     * flatly -- the THIRD home of a claim round 3 removed from two others. What
     * the format guarantees is narrower: no v2 body may carry the sentinel. A
     * WRITER holding zero is not stopped here, because the compact constructor
     * refuses only a negative epoch and {@code encode} turns zero into a
     * well-formed v1 body. **M5.15b owns "a live writer never emits ABSENT"**,
     * and reading this as a guarantee already in place is exactly how a
     * registry defaulting a {@code long} to zero ships.
     *
     * <p>⚠️ A CONSUMER SEEING THIS CANNOT RESUME on that event and falls back
     * to the commit log, which is what the log is for -- the same answer this
     * decoder gives for an unknown version.
     */
    public static final long SESSION_EPOCH_ABSENT = 0L;

    public SubscriptionEvent {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(via, "via");
        Objects.requireNonNull(inline, "inline");
        SubscriptionEventValidation.validate(session, sequencerEpoch, sessionEpoch,
                firstOffset, recordCount, via, inline, grant, byteStart, byteLen, chainSequence);
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
        return sequencerEpoch == that.sequencerEpoch && sessionEpoch == that.sessionEpoch
                && chainSequence == that.chainSequence
                && firstOffset == that.firstOffset
                && recordCount == that.recordCount
                && session.equals(that.session) && key.equals(that.key)
                && segmentKey.equals(that.segmentKey) && via == that.via
                && java.util.Arrays.equals(inline, that.inline)
                // ⚠️ THE GRANT AND THE RANGE ARE PART OF THE VALUE, and this
                // hand-written `equals` is the reason to say so: it enumerates
                // components, so a field added to the record and not to this
                // list is invisible to every `isEqualTo` in the tree -- and the
                // golden files assert exactly that way, so a decoder that
                // dropped the grant would still have matched.
                && Objects.equals(grant, that.grant)
                && byteStart == that.byteStart && byteLen == that.byteLen;
    }

    @Override
    public int hashCode() {
        return Objects.hash(session, sequencerEpoch, sessionEpoch, chainSequence, key, segmentKey,
                firstOffset, recordCount, via, grant, byteStart, byteLen)
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
        return "SubscriptionEvent[session=" + session
                + ", sequencerEpoch=" + sequencerEpoch
                // ⚠️ BOTH EPOCHS, because this is the printed line that has to
                // answer "was that a failover or a reset?" -- the question this
                // whole row exists to make answerable. Round-1 review found
                // `equals` and `hashCode` had gained the field and `toString`
                // had not, so the object could distinguish the two cases while
                // nothing a human reads could.
                + ", sessionEpoch=" + sessionEpoch
                + ", chainSequence=" + chainSequence
                + ", key=" + key
                + ", segmentKey=" + segmentKey + ", firstOffset=" + firstOffset
                + ", recordCount=" + recordCount + ", via=" + via
                + ", inline=" + inline.length + " bytes"
                // ⚠️ THE GRANT PRINTS THROUGH ITS OWN REDACTION, so the URL
                // never reaches this line -- `Grant.toString` is the whole
                // reason that type exists. The EXPIRY does print, because it is
                // what an operator needs when a direct fetch fails.
                // ⚠️ AND OMITTING IT WAS THE DEFECT ROUND 1 CAUGHT ONE COMMIT
                // AGO for `sequencerEpoch`/`sessionEpoch`: the object could
                // distinguish two cases while nothing a human reads could.
                + ", grant=" + grant
                + ", byteStart=" + byteStart + ", byteLen=" + byteLen + "]";
    }

    /** The 8-byte header every version shares: magic, then version. */
    static byte[] header(int version) {
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(version);
        return head.array();
    }

    /**
     * This event's bytes: {@link #VERSION_2} when it carries a session epoch,
     * {@link #VERSION_1} when it does not.
     *
     * <p>⚠️ THE VERSION FOLLOWS THE FIELD, which is what makes
     * {@link #SESSION_EPOCH_ABSENT} out-of-band rather than a magic number
     * inside the v2 range. An event with no session epoch IS a v1 event, so
     * there is no v2 body that can carry the sentinel and no way for a live
     * epoch to be mistaken for its absence.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THE COLLISION: a v2 event built with epoch 0
     * encoded, decoded and read back as ABSENT, while the record javadoc, the
     * ADR and a test all claimed "session epochs number from 1" and nothing
     * enforced it. This rule removes the CONTRADICTION -- no v2 body can say
     * both things at once.
     *
     * <p>⚠️ IT DOES NOT STOP A WRITER FROM HOLDING ZERO, and round-2 review was
     * right to say so. A registry that leaves a {@code long} at Java's default
     * still produces events; they are now stamped {@code VERSION_1},
     * indistinguishable from a legitimately old writer, where before they were
     * a self-contradictory v2 body a peer could refuse. ⚠️ THAT IS A REAL COST
     * OF THIS RULE: the version byte describes the PAYLOAD, not the writer.
     * ⚠️ And once M5.15b mints from 1, nothing legitimate emits v1 -- so a v1
     * event on the wire is a bug this format cannot report. **M5.15b owns "a
     * live writer never emits ABSENT"**; it is a minting invariant, not a codec
     * one.
     *
     * <p>⚠️ SO A v1 EVENT ROUND-TRIPS TO v1 BYTES, which is stronger than the
     * decode-only compatibility this started with: bytes read from an old
     * writer and written back are byte-identical.
     */
    public byte[] encode() {
        boolean carriesGrant = grant != null;
        boolean carriesSessionEpoch = carriesGrant || sessionEpoch != SESSION_EPOCH_ABSENT;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean carriesChainSequence = chainSequence != CHAIN_SEQUENCE_ABSENT;
        int version = carriesChainSequence ? VERSION_4
                : carriesGrant ? VERSION_3
                : carriesSessionEpoch ? VERSION_2 : VERSION_1;
        out.writeBytes(header(version));
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
        // ⚠️ APPENDED, not inserted beside `sequencerEpoch` where it reads
        // better. A v2 body is then a v1 body plus one field, which keeps
        // `decode` linear and makes the relationship between the two golden
        // files legible to anyone diffing them.
        if (carriesSessionEpoch) {
            SegmentWriter.putUvarint(out, sessionEpoch);
        }
        // v3 appends grant fields to v2. v4 instead inserts an explicit grant
        // presence byte here, then appends its required chain sequence, allowing
        // all three fetch modes to carry the same commit cursor.
        if (carriesChainSequence) {
            // v4 gives grant presence an explicit tag so live inline/proxy
            // events can carry the same chain cursor as direct events.
            out.write(carriesGrant ? 1 : 0);
        }
        if (carriesGrant) {
            putString(out, grant.url());
            SegmentWriter.putUvarint(out, grant.expiresAt().toEpochMilli());
            // ⚠️ A PRESENCE BYTE, NOT A SENTINEL VARINT. `byteStart = 0` is the
            // first byte of a real segment and `byteLen = 0` is an empty range,
            // so there is no spare value to mean ABSENT -- encoding
            // `value + 1` would work and would put an off-by-one between the
            // wire and the field, which is the class of bug golden files exist
            // to catch and the class hardest to read in a hex dump.
            boolean carriesRange = byteStart != RANGE_ABSENT;
            out.write(carriesRange ? 1 : 0);
            if (carriesRange) {
                SegmentWriter.putUvarint(out, byteStart);
                SegmentWriter.putUvarint(out, byteLen);
            }
        }
        if (carriesChainSequence) {
            SegmentWriter.putUvarint(out, chainSequence);
        }
        return out.toByteArray();
    }

    /**
     * Reads one event.
     *
     * @throws IOException on a foreign magic, an unknown version, or bytes that
     *     do not parse -- never a partially-populated event
     */
    public static SubscriptionEvent decode(byte[] bytes) throws IOException {
        // ⚠️ A TRUNCATED FRAME NAMES THE SUBSCRIPTION, which M5.46 fixed at
        // the source: `Cursor` takes WHAT IT IS READING and both bounds
        // messages interpolate it, so the call below passes "subscription
        // event" and a short read says so instead of naming the commit log.
        // ⚠️ AN EARLIER VERSION OF THIS COMMENT SAID THE OPPOSITE and that the
        // row was still open, twenty-six lines above the line that closed it --
        // so a reader writing the runbook this fix exists to protect would have
        // written it against "chain entry".
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
        if (version != VERSION_1 && version != VERSION_2 && version != VERSION_3
                && version != VERSION_4) {
            // ⚠️ REFUSED, NOT SKIPPED -- see the class javadoc. A consumer that
            // ignored an event it could not read would report a clean stream
            // while losing records.
            throw new IOException("unsupported subscription event version: " + version);
        }
        Cursor c = new Cursor(bytes, 8, "subscription event");
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
        // ⚠️ THE READ SIDE UNDERSTANDS BOTH SHAPES, which is the half of
        // `wire-format-change`'s "ship the read side first" that is reachable
        // in one commit: a reader built today parses what a v1 writer produced,
        // and a v1 event's session epoch reads as ABSENT rather than as zero.
        long sessionEpoch = version == VERSION_2 || version == VERSION_3 || version == VERSION_4
                ? c.uvarint()
                : SESSION_EPOCH_ABSENT;
        if (sessionEpoch < 0) {
            throw new IOException("session epoch " + sessionEpoch + " is not a request order");
        }
        // ⚠️ A v2 BODY MAY NOT CARRY THE SENTINEL, so the value is out of band
        // for v2: no sender can emit 0 at v2 and leave a reader unable to tell
        // "this session is at request zero" from "this writer had no such
        // field".
        // ⚠️ WHAT THIS DOES NOT MAKE TRUE is "session epochs number from 1" --
        // an earlier version of this comment claimed it did. The compact
        // constructor refuses only a NEGATIVE epoch, and `encode` turns 0 into
        // a well-formed v1 body, so a writer holding zero is not stopped here
        // or anywhere in this class. That is a MINTING invariant and M5.15b
        // owns it.
        if ((version == VERSION_2 || version == VERSION_3 || version == VERSION_4)
                && sessionEpoch == SESSION_EPOCH_ABSENT) {
            throw new IOException("a version " + version + " event may not carry an absent "
                    + "session epoch; an event without one is version 1");
        }
        Grant grant = null;
        long byteStart = RANGE_ABSENT;
        long byteLen = RANGE_ABSENT;
        boolean carriesGrant = version == VERSION_3;
        if (version == VERSION_4) {
            int grantPresent = c.bytes(1)[0] & 0xff;
            if (grantPresent != 0 && grantPresent != 1) {
                throw new IOException("invalid v4 grant presence: " + grantPresent);
            }
            carriesGrant = grantPresent == 1;
        }
        if (carriesGrant) {
            String url = getString(c);
            long expiresAtMillis = c.uvarint();
            // ⚠️ GUARDED HERE, NOT LEFT TO THE RECORD. `Grant`'s own guards are
            // the right ones for a CALLER, and they throw
            // `IllegalArgumentException` -- which is the wrong kind of thing for
            // a decoder reading untrusted bytes to escape with. Review measured
            // it: a crafted body with a zero-length url threw
            // `IllegalArgumentException: a grant needs a url` out of `decode`,
            // past the try below whose own comment says a caller "must not have
            // to catch two kinds of thing".
            if (url.isBlank()) {
                throw new IOException("a version 3 event carries a grant with no url");
            }
            // ⚠️ AND THE EXPIRY IS A VARINT LIKE ANY OTHER: a ten-byte one
            // reads back negative, which `Instant.ofEpochMilli` accepts as a
            // date before 1970 and `Grant` then refuses -- again as the wrong
            // exception type. `recordCount` and `partition` below carry the
            // round-2 measurements for exactly this shape.
            if (expiresAtMillis < 0) {
                throw new IOException("a grant expiry of " + expiresAtMillis
                        + " is not an instant");
            }
            grant = new Grant(url, java.time.Instant.ofEpochMilli(expiresAtMillis));
            int rangePresent = c.bytes(1)[0];
            if (rangePresent != 0 && rangePresent != 1) {
                throw new IOException("a range-present flag is 0 or 1, got " + rangePresent);
            }
            if (rangePresent == 1) {
                byteStart = c.uvarint();
                byteLen = c.uvarint();
                if (byteStart < 0 || byteLen < 0) {
                    throw new IOException("a byte range of (" + byteStart + ", " + byteLen
                            + ") is not a range");
                }
            }
        }
        long chainSequence = version == VERSION_4 ? c.uvarint() : CHAIN_SEQUENCE_ABSENT;
        if (version == VERSION_4 && chainSequence < 0) {
            throw new IOException("chain sequence is not non-negative");
        }
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
            return new SubscriptionEvent(session, sequencerEpoch, sessionEpoch,
                    new RunKey(new java.util.UUID(indexHi, indexLo), (int) partition), segmentKey,
                    firstOffset, (int) recordCount, mode, inline, grant, byteStart, byteLen,
                    chainSequence);
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
