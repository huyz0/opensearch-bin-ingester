// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.binstore.CrossAzBytes;
import binjava.format.FetchMode;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * One poll answer's bytes: how a frame is written, and which of M9 criterion
 * 6's peer sockets its cost belongs to (M9.2, NFR-5).
 *
 * <p>⚠️ **A SEPARATE FILE BECAUSE IT IS PURE AND {@link SubscriptionService}
 * IS NOT.** Reaching this arithmetic through {@code SubscriptionService} meant standing up a
 * {@code WebServer} and publishing, every such case published {@code INLINE},
 * and review MEASURED three mutations surviving because of it. Here every arm
 * is one call away. ⚠️ It also keeps that class under code-structure.md rule
 * 1's cap, which it had just crossed.
 *
 * <p>⚠️ **NOTHING HERE TOUCHES A SOCKET, A CLOCK OR A STORE.** It takes an
 * {@link OutputStream} the caller owns and a counter the composition root
 * built.
 */
final class PollAnswer {

    private PollAnswer() {
    }

    /**
     * One frame of a poll answer: the mode it was published in, and the bytes
     * it added to the body.
     */
    record AnswerFrame(FetchMode via, long bytes) {
    }

    /**
     * Attributes one poll answer's bytes across M9 criterion 6's transports.
     *
     * <p>⚠️ **PURE, AND PACKAGE-PRIVATE SO IT CAN BE ASSERTED WITHOUT A
     * SERVER.** Review MEASURED why: with this arithmetic inline in the poll,
     * the only way to reach it was a live {@code WebServer} publish, every
     * such case published {@code INLINE}, and three mutations survived —
     * {@code PROXY} attributed to the poll, {@code DIRECT} attributed to a
     * proxy read, and the overhead call deleted outright (every case built
     * its answer from {@code RetainedFloors.unknown()}, so the overhead was
     * always 0 and the call returned early having constrained nothing). The
     * same reason {@code SubscriptionService.waitFor(String)} is extracted.
     *
     * @param overheadBytes the answer's own bytes — the retained-floor frame
     *     and the framing — which belong to the socket rather than to a push
     *     that did not send them
     */
    static void countAnswer(CrossAzBytes crossAz, String consumerAz, long overheadBytes,
            java.util.List<AnswerFrame> frames) {
        crossAz.sent(CrossAzBytes.Transport.CONSUMER_POLL, consumerAz, overheadBytes);
        for (AnswerFrame frame : frames) {
            crossAz.sent(transportOf(frame.via()), consumerAz, frame.bytes());
        }
    }

    /**
     * Which peer socket of M9's criterion 6 a frame's bytes belong to.
     *
     * <p>⚠️ **{@code DIRECT} IS NOT A PEER BYTE PATH AND IS NOT PRETENDED TO
     * BE ONE**: the consumer fetches from the object store with a signed URL,
     * so what leaves this socket is the grant frame and nothing else -- the
     * poll's own bytes. Counting a direct grant as a proxy read would put the
     * segment's size in a column it never travelled through.
     */
    static CrossAzBytes.Transport transportOf(FetchMode via) {
        return switch (via) {
            case INLINE -> CrossAzBytes.Transport.INLINE_PUSH;
            case PROXY -> CrossAzBytes.Transport.PROXY_READ;
            default -> CrossAzBytes.Transport.CONSUMER_POLL;
        };
    }

    /**
     * Writes one length-prefixed frame and ⚠️ **returns the bytes it put in the
     * body**, which is what the cross-AZ split is built from.
     *
     * <p>⚠️ **THE WRITER REPORTS ITS OWN SIZE** rather than the caller
     * measuring the body before and after. Review MEASURED why: the
     * before-and-after form is a subtraction whose mutation to an ADDITION is
     * invisible on the FIRST frame of an answer -- the offset is 0 there -- so
     * the whole attribution rested on a case with two frames existing.
     */
    static long writeFrame(OutputStream out, byte[] frame) throws IOException {
        out.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(frame.length).array());
        out.write(frame);
        return FRAME_PREFIX_BYTES + frame.length;
    }

    /** The big-endian length in front of every frame. */
    static final int FRAME_PREFIX_BYTES = 4;
}
