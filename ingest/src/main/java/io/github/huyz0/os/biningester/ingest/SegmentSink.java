// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;

/**
 * Where {@link SegmentProxy} writes one consumer's copy of a segment.
 *
 * <p>⚠️ IT TAKES A SLICE OF A SHARED BUFFER, NOT AN ARRAY OF ITS OWN, and that
 * signature is the whole design. A {@code byte[] chunk} parameter would let the
 * proxy hand each consumer a fresh array, which is buffer-then-forward one
 * chunk at a time; {@code (buffer, offset, length)} makes the sharing explicit
 * and lets one read serve K consumers from one array.
 *
 * <p>⚠️ "FROM ONE ARRAY", NOT "WITH NO COPY AT ALL" -- an earlier draft claimed
 * the stronger thing and round-2 review MEASURED that nothing enforces it: an
 * implementation holding one shared scratch array and doing a
 * {@code System.arraycopy} into it per consumer per chunk satisfies every
 * assertion, because every consumer still sees exactly one array and the same
 * one. What the signature buys, and what {@code SegmentProxyTest} pins, is that
 * memory stays O(chunk) rather than O(K x chunk); it does not buy zero copying,
 * and saying so is cheaper than an assertion nobody can write.
 *
 * <p>⚠️ SO THE BYTES ARE ONLY VALID FOR THE DURATION OF THE CALL. A sink that
 * keeps the array reference sees it overwritten by the next read. Any sink that
 * needs to retain bytes copies what it needs, and pays for that copy itself --
 * which is where the cost belongs, because it is the sink's choice rather than
 * the serving path's.
 *
 * <p>⚠️ AND "THE DURATION OF THE CALL" IS NOT A PROMISE THE DEADLINE OVERLOAD
 * CAN KEEP (M5.58a). {@code SegmentProxy.streamTo}'s three-argument form runs
 * each {@code write} on its own virtual thread and DROPS a sink that overruns
 * its deadline -- by interrupting it and walking away, which is the stall that
 * form exists to remove. A dropped sink's task may therefore still be inside
 * {@code write}, holding the shared buffer, when the next read refills it. So
 * what such a sink sees is a range that is neither its own chunk nor a whole
 * one: not a truncated prefix, which the caller can at least detect, but bytes
 * from two different places in the segment spliced at an arbitrary offset.
 *
 * <p>⚠️ THE SINK CANNOT DEFEND AGAINST IT AND IS NOT ASKED TO. A sink that
 * copies on entry still races the refill for the bytes it is copying. What
 * bounds the damage is the CALLER: the three-argument form names the sinks it
 * dropped, and a caller that completes only the ones it did not drop never
 * presents those bytes as a segment. That is why the return shape names the
 * drops rather than counting the survivors, and why nothing in this tree wires
 * that form until its caller marks them ({@code SegmentServingPath.Tracking},
 * M5.58b).
 */
@FunctionalInterface
public interface SegmentSink {

    /**
     * Accepts {@code length} bytes starting at {@code offset} of
     * {@code buffer}.
     *
     * @throws IOException if this consumer cannot take the bytes; the proxy
     *     drops THIS consumer and keeps serving the rest
     */
    void write(byte[] buffer, int offset, int length) throws IOException;
}
