// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A pod's one receiver of fast frames (ADR-0082 §2; M13.27h): every frame
 * arriving at the peer route is answered here.
 *
 * <p>⚠️ THE HEADER FIRST, SO THE FENCE FIRST: a frame addressed to another
 * incarnation is refused {@code NOT_ROSTERED} unread -- a reused pod name or
 * address never makes a copy count for the wrong pod -- and raises nothing;
 * a frame below the epoch fence is refused {@code LOWER_EPOCH} unread, its
 * answer carrying the fence so the sender learns it is deposed; a higher
 * epoch raises the fence, durably, BEFORE the handler runs (ADR-0081 §3).
 *
 * <p>⚠️ A KIND WITH NO HANDLER IS NOT ANSWERED: the route tells the sender it
 * cannot serve that kind rather than inventing a refusal no reason names.
 * Each kind's handler lands with its task (joins M13.27j, departure M13.27k,
 * the write M13.27m).
 */
public final class FastFrameRouter {

    /** Answers one decoded frame of its kind. */
    @FunctionalInterface
    public interface Handler {
        FastFrame.Body answer(FastFrame.Header header, FastFrame.Body body) throws IOException;
    }

    /** The frame does not decode: the sender's fault, never the store's. */
    public static final class Malformed extends IOException {
        public Malformed(IOException cause) {
            super(cause.getMessage(), cause);
        }
    }

    /**
     * Told the size of every answer sent (M13.27n; M13.27h review round 1, P1):
     * an answer is a cross-AZ byte as much as a request is.
     */
    @FunctionalInterface
    public interface AnswerMeter {
        /**
         * @param asked the frame answered, decoded; null when it was refused
         *     from its header alone
         */
        void answered(FastFrame.Header header, FastFrame.Body asked, int bytes);

        AnswerMeter NONE = (header, asked, bytes) -> { };
    }

    private final String selfUid;
    private final EpochFence fence;
    private final AnswerMeter meter;
    private final Map<Integer, Handler> handlers = new ConcurrentHashMap<>();

    public FastFrameRouter(String selfUid, EpochFence fence) {
        this(selfUid, fence, AnswerMeter.NONE);
    }

    public FastFrameRouter(String selfUid, EpochFence fence, AnswerMeter meter) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.fence = Objects.requireNonNull(fence, "fence");
        this.meter = Objects.requireNonNull(meter, "meter");
    }

    /** Answers every later frame of {@code kind} with {@code handler}. */
    public void handle(int kind, Handler handler) {
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(kind, handler) != null) {
            throw new IllegalStateException("kind " + kind + " already has a handler");
        }
    }

    /**
     * The answer to {@code frame}, encoded; empty when no handler takes its kind.
     *
     * @throws Malformed the frame does not decode
     * @throws IOException the fence could not be made durable, or the handler
     *     failed -- nothing was answered
     */
    public Optional<byte[]> answer(byte[] frame) throws IOException {
        Objects.requireNonNull(frame, "frame");
        FastFrame.Header header;
        try {
            header = FastFrame.header(frame);
        } catch (IOException malformed) {
            throw new Malformed(malformed);
        }
        if (!header.targetUid().equals(selfUid)) {
            return Optional.of(refuse(header, FastFrame.Reason.NOT_ROSTERED,
                    "addressed to " + header.targetUid() + ", not this incarnation"));
        }
        if (!fence.admit(header.epoch())) {
            return Optional.of(refuse(header, FastFrame.Reason.LOWER_EPOCH,
                    "epoch " + header.epoch() + " is below the fence at " + fence.highest()));
        }
        Handler handler = handlers.get(header.kind());
        if (handler == null) {
            return Optional.empty();
        }
        FastFrame.Frame decoded;
        try {
            decoded = FastFrame.decode(frame);
        } catch (IOException malformed) {
            throw new Malformed(malformed);
        }
        FastFrame.Body answer = handler.answer(decoded.header(), decoded.body());
        byte[] encoded = FastFrame.encode(fence.highest(), selfUid, header.senderUid(), answer);
        meter.answered(header, decoded.body(), encoded.length);
        return Optional.of(encoded);
    }

    private byte[] refuse(FastFrame.Header header, FastFrame.Reason reason, String text) {
        byte[] encoded = FastFrame.encode(fence.highest(), selfUid, header.senderUid(),
                new FastFrame.Refused(reason, Optional.empty(), text));
        meter.answered(header, null, encoded.length);
        return encoded;
    }
}
