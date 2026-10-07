// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Receives a bounded, source-authenticated durable-segment hint. */
public final class DurableSegmentSignalService implements HttpService {
    public static final String PATH = "/ctl/durable-segment";

    private final EndpointSliceView members;
    private final DurableHandler handler;

    @FunctionalInterface
    public interface DurableHandler {
        void onDurable(String segmentKey, String writerAz) throws IOException;
    }

    public DurableSegmentSignalService(EndpointSliceView members, DurableHandler handler) {
        this(members, handler, PeerBinding.off());
    }

    private final PeerBinding binding;

    /** The same, a hint's writer bound to the client certificate (ADR-0084; M13.52g). */
    public DurableSegmentSignalService(EndpointSliceView members, DurableHandler handler,
            PeerBinding binding) {
        this.members = Objects.requireNonNull(members, "members");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    public boolean accept(String sourceHost, byte[] body) throws IOException {
        return accept(java.util.Optional.empty(), sourceHost, body);
    }

    /** The same, from a client presenting {@code chain} (ADR-0084 decision 8). */
    public boolean accept(java.util.Optional<java.security.cert.Certificate[]> chain,
            String sourceHost, byte[] body) throws IOException {
        DurableSegmentSignalFrame frame = DurableSegmentSignalFrame.decode(body);
        if (!permitted(chain, sourceHost, frame)) {
            return false;
        }
        handler.onDurable(frame.segmentKey(), frame.writerAz());
        return true;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(PATH, this::receive);
    }

    private void receive(io.helidon.webserver.http.ServerRequest request,
            io.helidon.webserver.http.ServerResponse response) {
        byte[] body;
        try {
            body = bounded(request);
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send();
            return;
        } catch (IOException failed) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        DurableSegmentSignalFrame frame;
        try {
            frame = DurableSegmentSignalFrame.decode(body);
        } catch (IOException malformed) {
            response.status(Status.BAD_REQUEST_400).send();
            return;
        }
        if (!permitted(request.remotePeer().tlsCertificates(), request.remotePeer().host(),
                frame)) {
            response.status(Status.FORBIDDEN_403).send();
            return;
        }
        try {
            handler.onDurable(frame.segmentKey(), frame.writerAz());
        } catch (IOException | RuntimeException unavailable) {
            // A cache warm is an optimization; a failed fetch leaves the normal
            // object-store read ladder authoritative and is not retried here.
        }
        response.status(Status.NO_CONTENT_204).send();
    }

    /** The writer is the certificate's pod, and the EndpointSlice's at the source address. */
    private boolean permitted(java.util.Optional<java.security.cert.Certificate[]> chain,
            String sourceHost, DurableSegmentSignalFrame frame) {
        return binding.refusalForPodId(chain, frame.writerPodId()).isEmpty()
                && authorized(sourceHost, frame);
    }

    private boolean authorized(String sourceHost, DurableSegmentSignalFrame frame) {
        if (sourceHost == null || sourceHost.isBlank()) {
            return false;
        }
        List<EndpointSliceView.Endpoint> matches = members.readyEndpoints().stream()
                .filter(endpoint -> endpoint.address().equals(sourceHost)).toList();
        if (matches.size() != 1) {
            return false;
        }
        EndpointSliceView.Endpoint writer = matches.get(0);
        if (!writer.podId().equals(frame.writerPodId())
                || !writer.az().equals(frame.writerAz())) {
            return false;
        }
        try {
            return SegmentKey.podShortIdOf(frame.segmentKey()).equals(frame.writerPodId());
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private static byte[] bounded(io.helidon.webserver.http.ServerRequest request)
            throws IOException {
        try (var input = new BoundedStream(request.content().inputStream(),
                DurableSegmentSignalFrame.MAX_FRAME_BYTES)) {
            return input.readAllBytes();
        }
    }
}
