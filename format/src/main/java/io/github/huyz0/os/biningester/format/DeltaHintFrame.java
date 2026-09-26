// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * The cross-AZ half of delta delivery: the leaseholder tells one relay pod per
 * remote AZ which delta is durable, and the relay reads it from the store
 * (M10.17, ADR-0075).
 *
 * <p>⚠️ **A KEY, NOT THE DELTA.** {@code (epoch, sequence)} names the chain
 * object, and the relay derives its key from its own prefix, so the bytes on
 * the expensive link do not grow with the delta. The hint is also harmless
 * when wrong: a hint for an object that does not exist reads nothing and
 * publishes nothing.
 *
 * <p>Layout, big-endian, fixed 24 bytes: {@code MAGIC u32 "BDHF", VERSION u32
 * = 1, epoch i64, sequence i64}.
 */
public record DeltaHintFrame(long epoch, long sequence) {

    public static final int MAGIC = 0x42444846;
    public static final int VERSION_1 = 1;
    public static final int BYTES = 24;

    public DeltaHintFrame {
        if (epoch < 1) {
            throw new IllegalArgumentException("a hinted delta's epoch is positive: " + epoch);
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("a sequence is never negative: " + sequence);
        }
    }

    public byte[] encode() {
        return ByteBuffer.allocate(BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putInt(VERSION_1).putLong(epoch).putLong(sequence).array();
    }

    public static DeltaHintFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != BYTES) {
            throw new IOException("a delta hint is " + BYTES + " bytes, not " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (buffer.getInt() != MAGIC) {
            throw new IOException("not a delta hint");
        }
        int version = buffer.getInt();
        if (version != VERSION_1) {
            throw new IOException("unsupported delta hint version: " + version);
        }
        try {
            return new DeltaHintFrame(buffer.getLong(), buffer.getLong());
        } catch (IllegalArgumentException malformed) {
            throw new IOException("invalid delta hint", malformed);
        }
    }
}
