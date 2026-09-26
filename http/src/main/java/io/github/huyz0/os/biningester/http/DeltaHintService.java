// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Receives the leaseholder's 24-byte hint on this AZ's relay and queues it for
 * the relay's read (M10.20, ADR-0075 decision 5).
 *
 * <p>⚠️ **ONLY THE RELAY ACCEPTS A HINT**: the lowest ready pod id of this AZ,
 * by {@link DeltaFanOut#relayOf}, from the same membership the leaseholder
 * chose it by. Any other pod refuses, so a hint that raced a relay change is
 * dropped rather than read twice in one AZ -- the gap ADR-0075 names.
 *
 * <p>⚠️ **ONLY FROM EXACTLY ONE READY POD.** The hint names a delta; the relay
 * then reads the store, so an unauthenticated source could spend this pod's
 * GETs. A forged hint for a delta that does not exist advances nothing.
 */
public final class DeltaHintService implements HttpService {

    /** The route. */
    public static final String PATH = DeltaFanOut.HINT_PATH;

    private final EndpointSliceView members;
    private final String selfPodId;
    private final String selfAz;
    private final Predicate<DeltaHintFrame> relay;

    /** @param relay {@code DeltaRelay#offer}: false when its queue was full */
    public DeltaHintService(EndpointSliceView members, String selfPodId, String selfAz,
            Predicate<DeltaHintFrame> relay) {
        this.members = Objects.requireNonNull(members, "members");
        this.selfPodId = Objects.requireNonNull(selfPodId, "selfPodId");
        this.selfAz = Objects.requireNonNull(selfAz, "selfAz");
        this.relay = Objects.requireNonNull(relay, "relay");
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
        try (var input = new BoundedStream(request.content().inputStream(),
                DeltaHintFrame.BYTES)) {
            body = input.readAllBytes();
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send();
            return;
        } catch (IOException | RuntimeException unreadable) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        DeltaHintFrame hint;
        try {
            hint = DeltaHintFrame.decode(body);
        } catch (IOException malformed) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        // A full queue is this relay's loss, counted there; the sender's retry
        // would only queue it behind the same backlog.
        relay.test(hint);
        response.status(Status.NO_CONTENT_204).send();
    }

    /** Whether this pod is its AZ's relay and {@code sourceHost} exactly one ready pod. */
    boolean authorized(String sourceHost) {
        if (sourceHost == null || sourceHost.isBlank()) {
            return false;
        }
        List<EndpointSliceView.Endpoint> ready = members.readyEndpoints();
        boolean isRelay = DeltaFanOut.relayOf(ready, selfAz)
                .map(relay -> selfPodId.equals(relay.podId())).orElse(false);
        long sources = ready.stream().filter(e -> sourceHost.equals(e.address())).count();
        return isRelay && sources == 1;
    }
}
