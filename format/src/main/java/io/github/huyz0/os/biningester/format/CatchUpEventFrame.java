// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/** Versioned catch-up delivery carrying the existing subscription event. */
public record CatchUpEventFrame(UUID requestId, SubscriptionEvent event) {

    public static final int FRAME_KIND = CatchUpFrameCodec.EVENT_KIND;

    public CatchUpEventFrame {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(event, "event");
    }

    public byte[] encode() {
        ByteArrayOutputStream out = CatchUpFrameCodec.header(
                CatchUpRequestFrame.MAGIC, CatchUpRequestFrame.VERSION_1, FRAME_KIND);
        CatchUpFrameCodec.putUuid(out, requestId);
        byte[] eventBytes = event.encode();
        SegmentWriter.putUvarint(out, eventBytes.length);
        out.writeBytes(eventBytes);
        return out.toByteArray();
    }

    public static CatchUpEventFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor cursor = new Cursor(bytes, 0, "catch-up event");
        CatchUpFrameCodec.requireVersion(cursor, CatchUpRequestFrame.MAGIC,
                CatchUpRequestFrame.VERSION_1, FRAME_KIND, "catch-up event");
        UUID requestId = CatchUpFrameCodec.uuid(cursor);
        long length = cursor.uvarint();
        if (length < 0 || length > cursor.remaining()) {
            throw new IOException("catch-up event claims " + length
                    + " bytes with only " + cursor.remaining() + " bytes left");
        }
        SubscriptionEvent event = SubscriptionEvent.decode(cursor.bytes((int) length));
        CatchUpFrameCodec.finish(cursor, "catch-up event");
        return new CatchUpEventFrame(requestId, event);
    }
}
