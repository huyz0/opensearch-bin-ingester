// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaPushFrame;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Receives a whole durable delta from a pod of this pod's own AZ -- the
 * leaseholder, or this AZ's relay -- and hands it to this pod's chain
 * publisher (M10.19, ADR-0075).
 *
 * <p>⚠️ **ONLY A READY POD OF THIS AZ MAY PUSH**, identified by its source
 * address in the EndpointSlice membership, the same trust the durable-segment
 * hint route uses. A push from another AZ is refused: across AZs only a
 * 24-byte hint may travel, and accepting a whole delta from there would let
 * NFR-5's largest term back in by a side door.
 *
 * <p>⚠️ **ORDER IS NOT CHECKED HERE.** The chain publisher drops any
 * {@code (epoch, sequence)} not after the last it published, so a duplicate
 * or a stale leaseholder's push is harmless by the time it reaches a consumer.
 */
public final class DeltaPushService implements HttpService {

    /** The route. */
    public static final String PATH = DeltaFanOut.PUSH_PATH;

    /** The frame's fixed header plus the largest length prefix. */
    static final long MAX_BODY_BYTES = DeltaPushFrame.MAX_DELTA_BYTES + 32L;

    /** Takes a pushed delta; {@code ChainPublisher#offer} in production. */
    @FunctionalInterface
    public interface Receiver {
        void accept(long epoch, CommitDelta delta);
    }

    private final EndpointSliceView members;
    private final String selfAz;
    private final Receiver receiver;

    public DeltaPushService(EndpointSliceView members, String selfAz, Receiver receiver) {
        this.members = Objects.requireNonNull(members, "members");
        this.selfAz = Objects.requireNonNull(selfAz, "selfAz");
        this.receiver = Objects.requireNonNull(receiver, "receiver");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(PATH, this::receive);
    }

    private void receive(ServerRequest request, ServerResponse response) {
        if (!authorized(request.remotePeer().host())) {
            response.status(Status.FORBIDDEN_403).send();
            return;
        }
        byte[] body;
        try (var input = new BoundedStream(request.content().inputStream(), MAX_BODY_BYTES)) {
            body = input.readAllBytes();
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send();
            return;
        } catch (IOException | RuntimeException unreadable) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        DeltaPushFrame frame;
        try {
            frame = DeltaPushFrame.decode(body);
        } catch (IOException malformed) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        receiver.accept(frame.epoch(), frame.delta());
        response.status(Status.NO_CONTENT_204).send();
    }

    /** Whether {@code sourceHost} is exactly one ready pod, and of this AZ. */
    boolean authorized(String sourceHost) {
        if (sourceHost == null || sourceHost.isBlank()) {
            return false;
        }
        List<EndpointSliceView.Endpoint> matches = members.readyEndpoints().stream()
                .filter(endpoint -> sourceHost.equals(endpoint.address())).toList();
        return matches.size() == 1 && selfAz.equals(matches.get(0).az());
    }
}
