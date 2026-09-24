// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/** Shared bounds-checked primitives for the versioned catch-up frames. */
final class CatchUpFrameCodec {

    static final int REQUEST_KIND = 1;
    static final int EVENT_KIND = 2;
    static final int END_KIND = 3;

    private CatchUpFrameCodec() {
    }

    static ByteArrayOutputStream header(int magic, int version, int kind) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        bytes.putInt(magic).putInt(version);
        out.writeBytes(bytes.array());
        out.write(kind);
        return out;
    }

    static void putUuid(ByteArrayOutputStream out, UUID id) {
        ByteBuffer bytes = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        out.writeBytes(bytes.array());
    }

    static UUID uuid(Cursor cursor) throws IOException {
        ByteBuffer bytes = ByteBuffer.wrap(cursor.bytes(16)).order(ByteOrder.BIG_ENDIAN);
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    static int requireVersion(Cursor cursor, int magic, int version, int kind, String what)
            throws IOException {
        return requireVersion(cursor, magic, new int[] {version}, kind, what);
    }

    static int requireVersion(Cursor cursor, int magic, int[] versions, int kind, String what)
            throws IOException {
        int actualMagic = intValue(cursor.bytes(4));
        if (actualMagic != magic) {
            throw new IOException("not a " + what + " frame: magic 0x"
                    + Integer.toHexString(actualMagic));
        }
        int actualVersion = intValue(cursor.bytes(4));
        if (java.util.Arrays.stream(versions).noneMatch(version -> version == actualVersion)) {
            throw new IOException(what + " version " + actualVersion
                    + " is not readable by this build, which knows "
                    + java.util.Arrays.toString(versions));
        }
        int actualKind = cursor.bytes(1)[0] & 0xFF;
        if (actualKind != kind) {
            throw new IOException(what + " has unexpected frame kind " + actualKind
                    + ", expected " + kind);
        }
        return actualVersion;
    }

    static void finish(Cursor cursor, String what) throws IOException {
        if (!cursor.atEnd()) {
            throw new IOException(what + " has " + cursor.remaining()
                    + " trailing bytes after its body");
        }
    }

    static int intValue(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }
}
