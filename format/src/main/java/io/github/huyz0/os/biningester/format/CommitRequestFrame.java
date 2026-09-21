// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One pod's commit, on its way to the pod that holds the lease (M8.33, FR-12,
 * <a href="../../../../../../docs/internal/product/decisions/0012-peer-mesh-without-gossip.md">ADR-0012</a>).
 *
 * <p>⚠️ THESE BYTES CROSS A PROCESS BOUNDARY, WHICH IS WHY THEY ARE A FORMAT
 * TYPE AND NOT A JSON OBJECT SOMEONE WRITES IN THE TRANSPORT. Every field here
 * is half of an idempotency key or a record count, and a transport that encoded
 * them loosely would make a forwarded commit differ from a local one in ways
 * nothing compares.
 *
 * <p>⚠️ IT CARRIES {@code (podId, incarnationId, flushSeq)} BECAUSE THE PAIR
 * ALONE CANNOT TELL A RESTART FROM A REPLAY (ADR-0036). {@code flushSeq}
 * restarts at 0 on every process start while {@code podId} is stable, so
 * without the incarnation a leaseholder cannot distinguish "this pod is sending
 * its first flush again after a restart" — which must be applied — from "this
 * is the same flush arriving twice" — which must not.
 *
 * <p>⚠️ THE RECORD COUNTS ARE PER STREAM AND THEY ARE THE COMMIT'S SUBSTANCE.
 * A frame that lost one stream's count would have the leaseholder assign a
 * range too short, and every record past it becomes unreachable with no error
 * anywhere — so the map is written whole and a torn one is refused rather than
 * partly applied.
 *
 * <p>⚠️ WHAT COMES BACK IS A {@link CommitDelta}, WHICH ALREADY HAS A FORMAT.
 * Only the request needed one, which is why this is one type rather than two.
 */
public record CommitRequestFrame(String podId, String incarnationId, long flushSeq,
        String segmentKey, Map<RunKey, Integer> recordCounts) {

    /** {@code "BPCR"} — big-endian, so an operator sees it in a hex dump. */
    public static final int MAGIC = 0x42504352;

    /** The only shape that has ever been written. */
    public static final int VERSION_1 = 1;

    public CommitRequestFrame {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(incarnationId, "incarnationId");
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(recordCounts, "recordCounts");
        recordCounts = Map.copyOf(recordCounts);
        requireText(podId, "podId");
        requireText(incarnationId, "incarnationId");
        requireText(segmentKey, "segmentKey");
        if (podId.indexOf('-') >= 0 || podId.indexOf('/') >= 0) {
            // ⚠️ THE SAME GRAMMAR `CommitRequest` ENFORCES, and it is here
            // because the frame is what arrives from ANOTHER process. Without
            // it a peer's `pod-1` decodes cleanly and then throws an unchecked
            // IllegalArgumentException out of the conversion to
            // `CommitRequest`, in the loop serving that peer -- which is the
            // throw this type's `decode` promises cannot happen. ⚠️ AND THE
            // GRAMMAR IS NOT COSMETIC: a segment key is
            // `<seq>-<podId>-<flushSeq>-...`, so a `-` in a podId makes the
            // key ambiguous to parse.
            throw new IllegalArgumentException("podId may not contain '-' or '/': " + podId);
        }
        if (flushSeq < 0) {
            throw new IllegalArgumentException("flushSeq must not be negative: " + flushSeq);
        }
        if (recordCounts.isEmpty()) {
            // ⚠️ A COMMIT OF NOTHING IS NOT A COMMIT. It would take an offset
            // range of length zero from every stream it named, which is a
            // no-op that still consumes a flush sequence -- and a leaseholder
            // that applied it would record a flush the sender never made.
            throw new IllegalArgumentException("a commit with no records commits nothing");
        }
        for (Map.Entry<RunKey, Integer> entry : recordCounts.entrySet()) {
            // ⚠️ NO NULL CHECK: `Map.copyOf` above already refuses a null
            // value, and a second one here reads as a guard that is load-
            // bearing when it can never fire.
            if (entry.getValue() <= 0) {
                // ⚠️ ZERO IS REFUSED, NOT DROPPED. A stream named with no
                // records is a caller that believes it wrote something; taking
                // it out silently is how the sender and the leaseholder come to
                // disagree about which streams a segment holds.
                throw new IllegalArgumentException("record count for " + entry.getKey()
                        + " must be positive, not " + entry.getValue());
            }
        }
    }

    private static void requireText(String value, String what) {
        if (value.isBlank()) {
            throw new IllegalArgumentException(what + " is never blank");
        }
    }

    /** These bytes, as {@link #VERSION_1}. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(VERSION_1);
        out.writeBytes(head.array());
        putString(out, podId);
        putString(out, incarnationId);
        SegmentWriter.putUvarint(out, flushSeq);
        putString(out, segmentKey);
        SegmentWriter.putUvarint(out, recordCounts.size());
        // ⚠️ SORTED, SO ONE REQUEST IS ONE SEQUENCE OF BYTES. `Map.copyOf`
        // gives no iteration order, so an unsorted encode makes the same commit
        // encode differently on two runs -- which breaks a golden file, and
        // worse, makes a retry's bytes differ from the first attempt's for
        // anything that ever compares them.
        recordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    // ⚠️ TWO BIG-ENDIAN LONGS, WHICH IS HOW EVERY OTHER `RunKey`
                    // IN THIS PACKAGE GOES ON A WIRE (`SegmentWriter`'s
                    // directory, `CommitDelta`'s runs). 16 bytes rather than a
                    // 37-byte string, and -- the reason that matters here --
                    // `UUID.fromString` is LENIENT: `0-0-4000-8000-1` and the
                    // canonical form are two different byte sequences that both
                    // decode to one id, which contradicts this method's own
                    // "one commit is one sequence of bytes".
                    ByteBuffer id = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
                    id.putLong(entry.getKey().indexId().getMostSignificantBits());
                    id.putLong(entry.getKey().indexId().getLeastSignificantBits());
                    out.writeBytes(id.array());
                    SegmentWriter.putUvarint(out, entry.getKey().partitionId());
                    SegmentWriter.putUvarint(out, entry.getValue());
                });
        return out.toByteArray();
    }

    /**
     * Reads one frame.
     *
     * <p>⚠️ AN UNKNOWN VERSION STOPS; IT DOES NOT SKIP, for the reason every
     * other format type in this package gives: a reader that took the part it
     * understood would apply a commit whose shape nobody sent.
     */
    public static CommitRequestFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor c = new Cursor(bytes, 0, "commit request");
        int magic = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException("not a commit request frame: magic 0x"
                    + Integer.toHexString(magic));
        }
        int version = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        // ⚠️ != AND NOT >, for the reason ConsumerProgress gives: a version
        // BELOW the known one is what zeroed torn bytes carry.
        if (version != VERSION_1) {
            throw new IOException("commit request version " + version
                    + " is not readable by this build, which knows " + VERSION_1);
        }
        String podId = getString(c);
        String incarnationId = getString(c);
        long flushSeq = c.uvarint();
        String segmentKey = getString(c);
        long count = c.uvarint();
        if (count < 0 || count > c.remaining()) {
            // ⚠️ BOUNDED BY THE BYTES LEFT. ⚠️ AN EARLIER VERSION OF THIS
            // COMMENT CLAIMED IT PREVENTED AN ALLOCATION, and review measured
            // that false: the map below is built with no capacity, so nothing
            // is sized from `count`. What the guard actually buys is that a
            // torn frame is refused HERE, with a message an operator can act
            // on, rather than 20 streams later as a truncation -- and that a
            // count of 2^31 does not spend 2^31 loop iterations failing.
            throw new IOException("commit request claims " + count + " streams with only "
                    + c.remaining() + " bytes left");
        }
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        for (long i = 0; i < count; i++) {
            ByteBuffer id = ByteBuffer.wrap(c.bytes(16)).order(ByteOrder.BIG_ENDIAN);
            UUID indexId = new UUID(id.getLong(), id.getLong());
            int partition = toInt(c.uvarint(), "partition");
            int records = toInt(c.uvarint(), "record count");
            // ⚠️ NO try/catch HERE, AND THAT IS THE POINT OF `toInt` ABOVE.
            // `RunKey` refuses a negative partition and a null id; `toInt`
            // bounds the partition to [0, MAX_INT] and the id is built from
            // sixteen bytes that always exist, so nothing here can throw. An
            // earlier version wrapped this in a catch that review MEASURED
            // unreachable -- a guard that reads as load-bearing and can never
            // fire is the same defect as the null check this file deletes ten
            // lines up.
            RunKey key = new RunKey(indexId, partition);
            if (counts.put(key, records) != null) {
                // ⚠️ TWO COUNTS FOR ONE STREAM IN ONE REQUEST IS UNDECIDABLE,
                // and either answer is wrong: the larger over-assigns offsets
                // for records nobody wrote, the smaller strands the rest.
                throw new IOException("two record counts for one stream: " + key);
            }
        }
        if (!c.atEnd()) {
            throw new IOException("commit request has " + c.remaining()
                    + " trailing bytes after its last stream");
        }
        try {
            return new CommitRequestFrame(podId, incarnationId, flushSeq, segmentKey, counts);
        } catch (IllegalArgumentException refused) {
            // ⚠️ AN IOException, NOT AN IAE: these bytes arrived over a wire,
            // and an unchecked throw out of a decode kills the loop serving the
            // peer -- which on this path stops the fleet committing.
            throw new IOException("commit request is not valid: " + refused.getMessage(), refused);
        }
    }

    private static int toInt(long value, String what) throws IOException {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IOException(what + " " + value + " does not fit an int");
        }
        return (int) value;
    }

    private static void putString(ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    private static String getString(Cursor c) throws IOException {
        long length = c.uvarint();
        if (length < 0 || length > c.remaining()) {
            throw new IOException("commit request claims a " + length
                    + "-byte string with only " + c.remaining() + " bytes left");
        }
        return new String(c.bytes((int) length), StandardCharsets.UTF_8);
    }
}
