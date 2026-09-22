// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/** Versioned marker that a catch-up request has delivered its complete range. */
public record CatchUpEndFrame(UUID requestId) {

    public static final int FRAME_KIND = CatchUpFrameCodec.END_KIND;

    public CatchUpEndFrame {
        Objects.requireNonNull(requestId, "requestId");
    }

    public byte[] encode() {
        ByteArrayOutputStream out = CatchUpFrameCodec.header(
                CatchUpRequestFrame.MAGIC, CatchUpRequestFrame.VERSION_1, FRAME_KIND);
        CatchUpFrameCodec.putUuid(out, requestId);
        return out.toByteArray();
    }

    public static CatchUpEndFrame decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor cursor = new Cursor(bytes, 0, "catch-up end");
        CatchUpFrameCodec.requireVersion(cursor, CatchUpRequestFrame.MAGIC,
                CatchUpRequestFrame.VERSION_1, FRAME_KIND, "catch-up end");
        UUID requestId = CatchUpFrameCodec.uuid(cursor);
        CatchUpFrameCodec.finish(cursor, "catch-up end");
        return new CatchUpEndFrame(requestId);
    }
}
