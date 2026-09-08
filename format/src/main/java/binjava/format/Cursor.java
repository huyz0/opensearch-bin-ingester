// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.IOException;

/**
 * A bounds-checked read cursor over a chain entry's bytes.
 *
 * <p>⚠️ BOUNDS-CHECKED BEFORE ALLOCATING: the commit log is untrusted input
 * too. A length field read out of a corrupt object must not become an array
 * size.
 *
 * <p>⚠️ Shared by all three chain shapes rather than copied into each. It was
 * private to {@code CommitDelta} until M4.5 added two more shapes that need
 * exactly the same primitives; a second copy is how two shapes come to disagree
 * about what a malformed varint is.
 */
final class Cursor {

    private final byte[] a;
    private int i;

    Cursor(byte[] a, int from) {
        this.a = a;
        this.i = from;
    }

    boolean atEnd() {
        return i == a.length;
    }

    /**
     * Bytes not yet consumed — an upper bound on any COUNT the remaining object
     * could possibly justify.
     *
     * <p>⚠️ EXISTS SO A COUNT CANNOT BECOME AN ALLOCATION SIZE, which is the
     * invariant this class's own header claims and which repeated fields broke.
     * A torn object claiming {@code 0x7FFFFFFF} segments — or {@code
     * 0x7FFFFFFF} runs inside an honest segment — sized an {@code ArrayList}
     * before reading a byte, and recovery died with an {@code OutOfMemoryError}
     * past its own {@code throws IOException}. Both were measured.
     *
     * <p>⚠️ THE LENGTH-PREFIXED CASE WAS ALREADY CLOSED and the REPEAT-COUNT
     * case was not, in EITHER of its two places. The run count predates the
     * batched layout and is reachable from the v0 path every bucket already
     * holds; it is bounded here rather than left behind a comment claiming the
     * class was closed. ⚠️ Whoever adds the next repeated field bounds it too —
     * this method does not do it for them.
     */
    int remaining() {
        return a.length - i;
    }

    long uvarint() throws IOException {
        long value = 0;
        int shift = 0;
        while (true) {
            if (i >= a.length) {
                throw new IOException("chain entry ends inside a varint");
            }
            int b = a[i++] & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 63) {
                throw new IOException("varint longer than 64 bits");
            }
        }
    }

    byte[] bytes(int n) throws IOException {
        // ⚠️ `a.length - i` rather than `i + n`, which OVERFLOWS int for a
        // large length field and lets the very allocation this guard exists to
        // prevent through as an OutOfMemoryError.
        if (n < 0 || n > a.length - i) {
            throw new IOException("chain entry ends inside a field");
        }
        byte[] out = new byte[n];
        System.arraycopy(a, i, out, 0, n);
        i += n;
        return out;
    }
}
