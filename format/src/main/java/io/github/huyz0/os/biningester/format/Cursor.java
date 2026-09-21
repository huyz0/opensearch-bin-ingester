// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.IOException;

/**
 * A bounds-checked read cursor over one framed object's bytes.
 *
 * <p>⚠️ BOUNDS-CHECKED BEFORE ALLOCATING: the commit log is untrusted input
 * too. A length field read out of a corrupt object must not become an array
 * size.
 *
 * <p>⚠️ Shared rather than copied into each caller. It was private to
 * {@code CommitDelta} until M4.5 added two more chain shapes that need exactly
 * the same primitives; a second copy is how two formats come to disagree about
 * what a malformed varint is.
 *
 * <p>⚠️ ITS CALLERS ARE NOT ALL CHAIN SHAPES, which is why {@link #what}
 * exists: {@code Checkpoint} has constructed one since M4.0 and
 * {@code SubscriptionEvent} since M5.14. An earlier version of this paragraph
 * said "shared by all three chain shapes" and then that a non-chain caller
 * arrived "since M5.14" — stale in the first half and off by a milestone and a
 * caller in the second, which the same commit conceded by passing
 * {@code "checkpoint"}. Both bounds
 * messages used to say "chain entry ends inside …" verbatim whoever was
 * reading, so a short read on the SUBSCRIPTION channel produced text naming the
 * COMMIT LOG. ⚠️ THE COST IS AN OPERATOR'S TIME, NOT DATA: a runbook grepping
 * that string sends someone to investigate commit-log corruption for a fault
 * entirely elsewhere. Round-3 review of M5.14 measured the strings and M5.46
 * fixed them here rather than by wrapping every read at the boundary, which
 * would have buried the four bounds guards that took three rounds to get right.
 */
final class Cursor {

    private final byte[] a;
    private final String what;
    private int i;

    /**
     * @param what what is being read, named in every bounds failure — "chain
     *     entry", "subscription event", "checkpoint". It is a CONSTRUCTOR
     *     argument rather than a field on the exception so that a caller cannot
     *     forget it at one of the four guards and get another format's name.
     */
    Cursor(byte[] a, int from, String what) {
        this.a = a;
        this.i = from;
        this.what = what;
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
                throw new IOException(what + " ends inside a varint");
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
            throw new IOException(what + " ends inside a field");
        }
        byte[] out = new byte[n];
        System.arraycopy(a, i, out, 0, n);
        i += n;
        return out;
    }
}
