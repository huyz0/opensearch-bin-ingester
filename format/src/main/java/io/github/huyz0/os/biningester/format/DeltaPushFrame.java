// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * One durable delta, pushed to a ready pod of the sender's own AZ (M10.17,
 * ADR-0075).
 *
 * <p>⚠️ **SAME AZ ONLY.** A whole delta is ~150 KiB at scale, 7.7× the
 * cross-AZ crossover; across AZs only a {@link DeltaHintFrame} travels.
 *
 * <p>⚠️ **THE DELTA IS CARRIED AS {@link CommitDelta}'s OWN BYTES**, the shape
 * the chain stores, so a pushed delta and a stored one cannot drift apart.
 *
 * <p>Layout, big-endian: {@code MAGIC u32 "BDPF", VERSION u32 = 1, epoch i64,
 * delta length uvarint, CommitDelta bytes}.
 */
public record DeltaPushFrame(long epoch, CommitDelta delta) {

    public static final int MAGIC = 0x42445046;
    public static final int VERSION_1 = 1;

    /**
     * The cap a forwarded commit's reply already carries a delta under
     * ({@code HttpSequencerTransport.MAX_REPLY_BYTES}, M4.7): a delta batches
     * many pods' commits, so it is legitimately larger than any one request.
     * ⚠️ A larger delta is durable and unpushable: only catch-up reaches it
     * (catch-up served off the leaseholder is owned by M11).
     */
    public static final int MAX_DELTA_BYTES = 8 << 20;

    public DeltaPushFrame {
        if (epoch < 1) {
            throw new IllegalArgumentException("a pushed delta's epoch is positive: " + epoch);
        }
        Objects.requireNonNull(delta, "delta");
    }

    public byte[] encode() {
        byte[] body = delta.encode();
        if (body.length > MAX_DELTA_BYTES) {
            throw new IllegalStateException("pushed delta exceeds " + MAX_DELTA_BYTES + " bytes");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(body.length + 24);
        out.writeBytes(ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putInt(VERSION_1).putLong(epoch).array());
        SegmentWriter.putUvarint(out, body.length);
        out.writeBytes(body);
        return out.toByteArray();
    }

    public static DeltaPushFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor cursor = new Cursor(bytes, 0, "delta push");
        if (ByteBuffer.wrap(cursor.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt() != MAGIC) {
            throw new IOException("not a delta push");
        }
        int version = ByteBuffer.wrap(cursor.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (version != VERSION_1) {
            throw new IOException("unsupported delta push version: " + version);
        }
        long epoch = ByteBuffer.wrap(cursor.bytes(8)).order(ByteOrder.BIG_ENDIAN).getLong();
        long length = cursor.uvarint();
        if (length <= 0 || length > MAX_DELTA_BYTES || length > cursor.remaining()) {
            throw new IOException("invalid delta push body length: " + length);
        }
        byte[] body = cursor.bytes((int) length);
        if (cursor.remaining() != 0) {
            throw new IOException("trailing bytes in delta push");
        }
        // ⚠️ THE DELTA'S OWN CONSTRUCTORS THROW UNCHECKED on a damaged field;
        // this parses network input, so every refusal is an IOException.
        try {
            return new DeltaPushFrame(epoch, CommitDelta.decode(body));
        } catch (IllegalArgumentException | IllegalStateException malformed) {
            throw new IOException("invalid delta push", malformed);
        }
    }
}
