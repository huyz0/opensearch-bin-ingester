// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** A best-effort peer hint that one segment is durable (M8.56, FR-10, NFR-4). */
public record DurableSegmentSignalFrame(String writerPodId, String writerAz,
        String segmentKey) {

    /** {@code BPDS}, visible in a packet dump. */
    public static final int MAGIC = 0x42504453;
    public static final int VERSION_1 = 1;
    public static final int MAX_POD_ID_BYTES = 256;
    public static final int MAX_AZ_BYTES = 256;
    public static final int MAX_SEGMENT_KEY_BYTES = SegmentKey.MAX_KEY_BYTES;
    public static final int MAX_FRAME_BYTES = 1550;

    public DurableSegmentSignalFrame {
        requireText(writerPodId, "writerPodId");
        requireText(writerAz, "writerAz");
        requireText(segmentKey, "segmentKey");
        if (utf8(writerPodId).length > MAX_POD_ID_BYTES
                || utf8(writerAz).length > MAX_AZ_BYTES
                || utf8(segmentKey).length > MAX_SEGMENT_KEY_BYTES) {
            throw new IllegalArgumentException("durable signal field exceeds its byte bound");
        }
    }

    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putInt(VERSION_1);
        out.writeBytes(header.array());
        put(out, writerPodId);
        put(out, writerAz);
        put(out, segmentKey);
        byte[] frame = out.toByteArray();
        if (frame.length > MAX_FRAME_BYTES) {
            throw new IllegalStateException("durable segment signal exceeds "
                    + MAX_FRAME_BYTES + " bytes");
        }
        return frame;
    }

    public static DurableSegmentSignalFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length > MAX_FRAME_BYTES) {
            throw new IOException("durable segment signal exceeds " + MAX_FRAME_BYTES + " bytes");
        }
        Cursor cursor = new Cursor(bytes, 0, "durable segment signal");
        int magic = ByteBuffer.wrap(cursor.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException("not a durable segment signal");
        }
        int version = ByteBuffer.wrap(cursor.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (version != VERSION_1) {
            throw new IOException("unsupported durable segment signal version: " + version);
        }
        try {
            String pod = get(cursor, MAX_POD_ID_BYTES);
            String az = get(cursor, MAX_AZ_BYTES);
            String key = get(cursor, MAX_SEGMENT_KEY_BYTES);
            if (cursor.remaining() != 0) {
                throw new IOException("trailing bytes in durable segment signal");
            }
            return new DurableSegmentSignalFrame(pod, az, key);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("invalid durable segment signal", malformed);
        }
    }

    private static String get(Cursor cursor, int maxBytes) throws IOException {
        long length = cursor.uvarint();
        if (length == 0 || length > maxBytes || length > cursor.remaining()) {
            throw new IOException("invalid durable signal field length: " + length);
        }
        byte[] encoded = cursor.bytes((int) length);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(encoded, utf8(value))) {
            throw new IOException("invalid UTF-8 in durable segment signal");
        }
        return value;
    }

    private static void put(ByteArrayOutputStream out, String value) {
        byte[] encoded = utf8(value);
        SegmentWriter.putUvarint(out, encoded.length);
        out.writeBytes(encoded);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is never blank");
        }
    }
}
