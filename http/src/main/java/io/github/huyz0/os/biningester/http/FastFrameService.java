// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * The fast frames' peer route (ADR-0082 §2; M13.27h): one endpoint, every
 * kind, answered by the pod's {@link FastFrameRouter}.
 *
 * <p>⚠️ THE BODY IS BOUNDED WHILE IT IS READ, as {@link CommitService}'s is:
 * a frame's whole entity is materialised only up to the cap, so a
 * {@code Content-Length} of gigabytes cannot become an allocation on the pod
 * every writer sends to (security.md rule 5). Past it: 413, the connection
 * closed (M13.19).
 *
 * <p>⚠️ THE STATUSES SAY WHOSE FAULT IT WAS: 400 a frame that does not decode,
 * 501 a kind this pod has no handler for yet, 500 a fence or handler that
 * failed -- nothing answered. A refusal the protocol names is a 200 carrying a
 * {@code REFUSED} frame, never a status.
 */
public final class FastFrameService implements HttpService {

    /**
     * ⚠️ 64 MiB: a frame carries at most one batch's records (a COMMIT, a
     * REPLICA) and a batch at most one segment's bytes, 8 MiB by default --
     * room for a raised {@code ingest.max-segment-bytes}, and far below a heap.
     */
    public static final long MAX_FRAME_BYTES = 64L << 20;

    private final FastFrameRouter router;
    private final long maxFrameBytes;

    public FastFrameService(FastFrameRouter router) {
        this(router, MAX_FRAME_BYTES);
    }

    FastFrameService(FastFrameRouter router, long maxFrameBytes) {
        this.router = Objects.requireNonNull(router, "router");
        this.maxFrameBytes = maxFrameBytes;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(HttpFastTransport.PATH, this::answer);
    }

    private void answer(ServerRequest request, ServerResponse response) {
        byte[] frame;
        try (var in = new BoundedStream(request.content().inputStream(), maxFrameBytes)) {
            frame = in.readAllBytes();
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413)
                    .header(io.helidon.http.HeaderNames.CONNECTION, "close")
                    .send(tooLarge.getMessage());
            return;
        } catch (IOException unreadable) {
            response.status(Status.BAD_REQUEST_400).send(String.valueOf(unreadable.getMessage()));
            return;
        }
        Optional<byte[]> answer;
        try {
            answer = router.answer(frame);
        } catch (FastFrameRouter.Malformed malformed) {
            response.status(Status.BAD_REQUEST_400).send(String.valueOf(malformed.getMessage()));
            return;
        } catch (IOException | RuntimeException failed) {
            response.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send(String.valueOf(failed.getMessage()));
            return;
        }
        if (answer.isEmpty()) {
            response.status(Status.NOT_IMPLEMENTED_501)
                    .send("this pod answers no fast frame of that kind yet");
            return;
        }
        response.status(Status.OK_200).send(answer.get());
    }
}
