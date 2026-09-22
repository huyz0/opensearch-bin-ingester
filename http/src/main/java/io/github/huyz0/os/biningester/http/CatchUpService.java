// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/** HTTP adapter for one node-scoped catch-up request (M8.24d, ADR-0065). */
public final class CatchUpService implements HttpService {

    public static final String PATH = "/ctl/catch-up";
    static final long MAX_REQUEST_BYTES = 1L << 20;
    static final long MAX_RESPONSE_BYTES = 8L << 20;

    @FunctionalInterface
    public interface Responder {
        List<byte[]> respond(CatchUpRequestFrame request) throws IOException;
    }

    private final Responder responder;

    public CatchUpService(Responder responder) {
        this.responder = Objects.requireNonNull(responder, "responder");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(PATH, this::catchUp);
    }

    private void catchUp(ServerRequest request, ServerResponse response) {
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

        List<byte[]> frames;
        try {
            frames = Objects.requireNonNull(responder.respond(frame), "response frames");
        } catch (IOException unavailable) {
            response.status(io.helidon.http.Status.SERVICE_UNAVAILABLE_503)
                    .send("catch-up replay is temporarily unavailable");
            return;
        } catch (IllegalArgumentException malformed) {
            response.status(io.helidon.http.Status.BAD_REQUEST_400)
                    .send(String.valueOf(malformed.getMessage()));
            return;
        }

        try {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            long responseBytes = 0;
            for (byte[] responseFrame : frames) {
                responseBytes = writeFrameWithinBudget(body, responseBytes, responseFrame);
            }
            response.status(io.helidon.http.Status.OK_200).send(body.toByteArray());
        } catch (BodyTooLargeException tooLarge) {
            response.status(io.helidon.http.Status.REQUEST_ENTITY_TOO_LARGE_413)
                    .send(tooLarge.getMessage());
        } catch (IllegalArgumentException malformed) {
            response.status(io.helidon.http.Status.BAD_REQUEST_400)
                    .send(String.valueOf(malformed.getMessage()));
        } catch (IOException impossible) {
            response.status(io.helidon.http.Status.INTERNAL_SERVER_ERROR_500)
                    .send("could not frame catch-up response");
        }
    }

    static long writeFrameWithinBudget(OutputStream output, long currentBytes,
            byte[] frame) throws IOException {
        Objects.requireNonNull(output, "output");
        if (frame == null || frame.length == 0) {
            throw new IllegalArgumentException("a catch-up response frame is required");
        }
        long framedBytes = (long) PollAnswer.FRAME_PREFIX_BYTES + frame.length;
        if (framedBytes > MAX_RESPONSE_BYTES
                || currentBytes > MAX_RESPONSE_BYTES - framedBytes) {
            throw new BodyTooLargeException("the catch-up response exceeds "
                    + MAX_RESPONSE_BYTES + " bytes");
        }
        PollAnswer.writeFrame(output, frame);
        return currentBytes + framedBytes;
    }

    private static byte[] bounded(ServerRequest request) throws IOException {
        try (var in = new BoundedStream(request.content().inputStream(), MAX_REQUEST_BYTES)) {
            return in.readAllBytes();
        }
    }
}
