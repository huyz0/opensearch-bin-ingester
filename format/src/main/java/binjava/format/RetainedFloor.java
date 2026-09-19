// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * The lowest offset of one stream still in the bucket, as the subscription puts
 * it on a wire (M8.6, M7.18, ADR-0056).
 *
 * <p>⚠️ **A FRAME OF ITS OWN, NOT A FIELD ON {@link SubscriptionEvent}.** An
 * event exists only when a segment is pushed, and the floor matters most on
 * RESUME -- when a stream is often quiet. A floor that could only ride on a
 * push would not arrive at the moment it is needed.
 *
 * <p>⚠️ **ONLY A CONSUMER THAT ASKS RECEIVES ONE**, and that is what makes a
 * new frame type deployable at all: this decoder, like every other in the
 * package, refuses a magic it does not know, so an old plugin handed this frame
 * would refuse the whole poll answer it arrived in. See ADR-0056.
 *
 * <p>⚠️ **THE STREAM IS TWO BIG-ENDIAN LONGS AND A PARTITION**, the way every
 * {@link RunKey} in the package travels -- 16 bytes and a varint rather than a
 * UUID's 37-character text, which {@code UUID.fromString} parses leniently
 * enough that two byte sequences decode to one id (ADR-0053).
 *
 * @param key the stream
 * @param oldestRetainedOffset the lowest offset of {@code key} whose segment
 *     garbage collection has not deleted. ⚠️ **A POSITION BELOW IT IS GONE**,
 *     and the consumer's refusal of one is what M7.16 built and what this frame
 *     finally lets fire
 */
public record RetainedFloor(RunKey key, long oldestRetainedOffset) {

    /** {@code BPRF}. */
    public static final int MAGIC = 0x42505246;

    public static final int VERSION_1 = 1;

    public RetainedFloor {
        Objects.requireNonNull(key, "key");
        if (oldestRetainedOffset < 0) {
            throw new IllegalArgumentException(
                    "oldestRetainedOffset is never negative: " + oldestRetainedOffset);
        }
    }

    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(VERSION_1);
        head.putLong(key.indexId().getMostSignificantBits());
        head.putLong(key.indexId().getLeastSignificantBits());
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, key.partitionId());
        SegmentWriter.putUvarint(out, oldestRetainedOffset);
        return out.toByteArray();
    }

    /**
     * Whether {@code frame} begins with this type's magic.
     *
     * <p>⚠️ **FOR A READER DISPATCHING A MIXED ANSWER**, which the poll answer
     * now is: a floor frame and event frames share one stream. Asking the
     * magic, rather than trying one decoder and catching its refusal, keeps a
     * torn event from being mistaken for a floor.
     */
    public static boolean isRetainedFloor(byte[] frame) {
        Objects.requireNonNull(frame, "frame");
        return frame.length >= 4
                && ByteBuffer.wrap(frame, 0, 4).order(ByteOrder.BIG_ENDIAN).getInt() == MAGIC;
    }

    public static RetainedFloor decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor c = new Cursor(bytes, 0, "retained floor");
        int magic = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException("not a retained floor frame: magic 0x"
                    + Integer.toHexString(magic));
        }
        int version = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        // ⚠️ `!=` RATHER THAN `>`: a version BELOW the known one is what zeroed
        // torn bytes carry (ADR-0053).
        if (version != VERSION_1) {
            throw new IOException("retained floor version " + version
                    + " is not readable by this build, which knows " + VERSION_1
                    + " -- refusing rather than guessing at a boundary below which a consumer "
                    + "is told its data is gone");
        }
        ByteBuffer id = ByteBuffer.wrap(c.bytes(16)).order(ByteOrder.BIG_ENDIAN);
        long most = id.getLong();
        long least = id.getLong();
        long partition = c.uvarint();
        if (partition < 0 || partition > Integer.MAX_VALUE) {
            throw new IOException("retained floor partition " + partition + " does not fit an int");
        }
        long floor = c.uvarint();
        if (floor < 0) {
            // ⚠️ A uvarint past 2^63 decodes negative in a long. A negative
            // floor refuses nothing and would look like "unknown".
            throw new IOException("retained floor " + floor + " does not fit a long");
        }
        if (!c.atEnd()) {
            throw new IOException("retained floor has " + c.remaining()
                    + " trailing bytes after its offset");
        }
        return new RetainedFloor(new RunKey(new java.util.UUID(most, least), (int) partition),
                floor);
    }
}
