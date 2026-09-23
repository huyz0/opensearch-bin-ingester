// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.ingest.DurableCatchUpResponder;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/** HTTP adapter for one node-scoped catch-up request (M8.24d, ADR-0065). */
public final class CatchUpService implements HttpService {

    /** POST replay control on the existing per-stream subscription URL. */
    public static final String PATH = HttpSubscriptionTransport.SUBSCRIBE_PREFIX + "{indexUuid}/{partition}";
    static final long MAX_REQUEST_BYTES = 1L << 20;
    static final long MAX_RESPONSE_BYTES = 8L << 20;

    @FunctionalInterface
    public interface Responder {
        List<byte[]> respond(CatchUpRequestFrame request) throws IOException;
    }

    @FunctionalInterface
    public interface StreamingResponder {
        void respond(CatchUpRequestFrame request, FrameSink sink) throws IOException;
    }

    @FunctionalInterface
    public interface FrameSink {
        void write(byte[] frame) throws IOException;
    }

    private final StreamingResponder responder;

    public CatchUpService(Responder responder) {
        Objects.requireNonNull(responder, "responder");
        this.responder = (request, sink) -> {
            for (byte[] frame : Objects.requireNonNull(responder.respond(request),
                    "response frames")) {
                sink.write(frame);
            }
        };
    }

    public CatchUpService(StreamingResponder responder) {
        this.responder = Objects.requireNonNull(responder, "responder");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(PATH, this::catchUp);
    }

    void catchUp(ServerRequest request, ServerResponse response) {
        CatchUpRequestFrame frame;
        try {
            frame = CatchUpRequestFrame.decode(bounded(request));
        } catch (BodyTooLargeException tooLarge) {
            response.status(io.helidon.http.Status.REQUEST_ENTITY_TOO_LARGE_413)
                    .send(tooLarge.getMessage());
            return;
        } catch (IOException | IllegalArgumentException malformed) {
            response.status(io.helidon.http.Status.BAD_REQUEST_400)
                    .send(String.valueOf(malformed.getMessage()));
            return;
        }

        OutputStream[] output = new OutputStream[1];
        try {
            responder.respond(frame, responseFrame -> {
                requireFrameWithinBudget(responseFrame);
                if (output[0] == null) {
                    response.status(io.helidon.http.Status.OK_200);
                    output[0] = response.outputStream();
                }
                PollAnswer.writeFrame(output[0], responseFrame);
                output[0].flush();
            });
            if (output[0] == null) {
                response.status(io.helidon.http.Status.OK_200).send(new byte[0]);
            }
        } catch (DurableCatchUpResponder.ResponseTooLargeException tooLarge) {
            refuseBeforeStreaming(response, output[0], tooLarge.getMessage());
        } catch (IOException unavailable) {
            failBeforeOrDuringStreaming(response, output[0],
                    io.helidon.http.Status.SERVICE_UNAVAILABLE_503,
                    "catch-up replay is temporarily unavailable", unavailable);
        } catch (IllegalArgumentException malformed) {
            failBeforeOrDuringStreaming(response, output[0],
                    io.helidon.http.Status.BAD_REQUEST_400,
                    String.valueOf(malformed.getMessage()), malformed);
        } finally {
            if (output[0] != null) {
                try {
                    output[0].close();
                } catch (IOException ignored) {
                    // An incomplete response has no matching end frame, so the
                    // consumer must leave its replay position unacknowledged.
                }
            }
        }
    }

    static long writeFrameWithinBudget(OutputStream output, long currentBytes,
            byte[] frame) throws IOException {
        Objects.requireNonNull(output, "output");
        if (frame == null || frame.length == 0) {
            throw new IllegalArgumentException("a catch-up response frame is required");
        }
        long framedBytes = (long) PollAnswer.FRAME_PREFIX_BYTES + frame.length;
        if (framedBytes > MAX_RESPONSE_BYTES) {
            throw new BodyTooLargeException("a catch-up frame exceeds "
                    + MAX_RESPONSE_BYTES + " bytes");
        }
        PollAnswer.writeFrame(output, frame);
        return currentBytes + framedBytes;
    }

    private static void requireFrameWithinBudget(byte[] frame)
            throws DurableCatchUpResponder.ResponseTooLargeException {
        if (frame == null || frame.length == 0) {
            throw new IllegalArgumentException("a catch-up response frame is required");
        }
        long framedBytes = (long) PollAnswer.FRAME_PREFIX_BYTES + frame.length;
        if (framedBytes > MAX_RESPONSE_BYTES) {
            throw new DurableCatchUpResponder.ResponseTooLargeException("a catch-up frame exceeds "
                    + MAX_RESPONSE_BYTES + " bytes");
        }
    }

    private static void refuseBeforeStreaming(ServerResponse response, OutputStream output,
            String message) {
        if (output == null) {
            response.status(io.helidon.http.Status.REQUEST_ENTITY_TOO_LARGE_413).send(message);
        }
    }

    private static void failBeforeOrDuringStreaming(ServerResponse response, OutputStream output,
            io.helidon.http.Status status, String message, Exception failure) {
        if (output == null) {
            response.status(status).send(message);
        } else {
            throw new IllegalStateException("catch-up response failed after streaming began",
                    failure);
        }
    }

    private static byte[] bounded(ServerRequest request) throws IOException {
        try (var in = new BoundedStream(request.content().inputStream(), MAX_REQUEST_BYTES)) {
            return in.readAllBytes();
        }
    }
}
