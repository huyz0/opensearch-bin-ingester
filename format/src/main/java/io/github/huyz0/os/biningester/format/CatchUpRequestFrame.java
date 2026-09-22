// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Versioned node-scoped catch-up request (ADR-0065, M8.24c). */
public record CatchUpRequestFrame(UUID requestId, List<Stream> streams) {

    /** {@code BCUP}; request, event and end frames share this protocol magic. */
    public static final int MAGIC = 0x42435550;
    public static final int VERSION_1 = 1;
    public static final int FRAME_KIND = CatchUpFrameCodec.REQUEST_KIND;
    public static final int MAX_STREAMS = 1024;

    public CatchUpRequestFrame {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(streams, "streams");
        if (streams.isEmpty()) {
            throw new IllegalArgumentException("a catch-up request needs a stream");
        }
        if (streams.size() > MAX_STREAMS) {
            throw new IllegalArgumentException("a catch-up request exceeds the maximum of "
                    + MAX_STREAMS + " streams");
        }
        streams = List.copyOf(streams);
    }

    public byte[] encode() {
        ByteArrayOutputStream out = CatchUpFrameCodec.header(MAGIC, VERSION_1, FRAME_KIND);
        CatchUpFrameCodec.putUuid(out, requestId);
        SegmentWriter.putUvarint(out, streams.size());
        for (Stream stream : streams) {
            CatchUpFrameCodec.putUuid(out, stream.key().indexId());
            SegmentWriter.putUvarint(out, stream.key().partitionId());
            SegmentWriter.putUvarint(out, stream.batchStart());
        }
        return out.toByteArray();
    }

    public static CatchUpRequestFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor cursor = new Cursor(bytes, 0, "catch-up request");
        CatchUpFrameCodec.requireVersion(cursor, MAGIC, VERSION_1, FRAME_KIND, "catch-up request");
        UUID requestId = CatchUpFrameCodec.uuid(cursor);
        long count = cursor.uvarint();
        if (count <= 0 || count > MAX_STREAMS || count > cursor.remaining()) {
            if (count > MAX_STREAMS) {
                throw new IOException("catch-up request exceeds the maximum of "
                        + MAX_STREAMS + " streams: " + count);
            }
            throw new IOException("catch-up request claims " + count
                    + " streams with only " + cursor.remaining() + " bytes left");
        }
        List<Stream> streams = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            UUID indexId = CatchUpFrameCodec.uuid(cursor);
            long partition = cursor.uvarint();
            long batchStart = cursor.uvarint();
            if (partition < 0 || partition > Integer.MAX_VALUE) {
                throw new IOException("catch-up partition does not fit an int: " + partition);
            }
            if (batchStart < 0) {
                throw new IOException("catch-up batch_start does not fit a long: " + batchStart);
            }
            try {
                streams.add(new Stream(new RunKey(indexId, (int) partition), batchStart));
            } catch (IllegalArgumentException refused) {
                throw new IOException("catch-up stream is invalid: " + refused.getMessage(), refused);
            }
        }
        CatchUpFrameCodec.finish(cursor, "catch-up request");
        try {
            return new CatchUpRequestFrame(requestId, streams);
        } catch (IllegalArgumentException refused) {
            throw new IOException("catch-up request is invalid: " + refused.getMessage(), refused);
        }
    }

    public record Stream(RunKey key, long batchStart) {
        public Stream {
            Objects.requireNonNull(key, "key");
            if (batchStart < 0) {
                throw new IllegalArgumentException("batch_start is never negative: " + batchStart);
            }
        }
    }
}
