// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.ingest.AzPeers;
import io.github.huyz0.os.biningester.ingest.Peer;
import io.github.huyz0.os.biningester.ingest.PeerRing;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Sends one best-effort hint to the deterministic cache owner in each remote AZ. */
public final class DurableSegmentSignalSender {
    private static final int MAX_POOLED_CLIENTS = 64;
    private static final Duration TIMEOUT = Duration.ofMillis(100);

    @FunctionalInterface
    public interface PeerPost {
        void post(String endpoint, byte[] body) throws IOException;
    }

    public DurableSegmentSignalSender(CrossAzBytes crossAz, int port, PeerPost post) {
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port is not valid: " + port);
        }
        this.port = port;
        this.post = Objects.requireNonNull(post, "post");
    }

    private final CrossAzBytes crossAz;
    private final int port;
    private final PeerPost post;

    /** Uses pooled Helidon clients for the node's configured HTTP port. */
    public DurableSegmentSignalSender(CrossAzBytes crossAz, int port) {
        this(crossAz, port, new HttpPost());
    }

    public List<String> send(DurableSegmentSignalFrame frame,
            List<EndpointSliceView.Endpoint> endpoints) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(endpoints, "endpoints");
        byte[] body = frame.encode();
        List<Peer> targets = owners(frame, endpoints);
        List<String> attempted = new ArrayList<>(targets.size());
        for (Peer target : targets) {
            attempted.add(target.endpoint());
            // Count bytes that leave this pod, including a best-effort request
            // whose peer is unavailable or still on an older release.
            crossAz.sent(CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL,
                    target.az(), body.length);
            try {
                post.post(target.endpoint(), body);
            } catch (IOException | RuntimeException unavailable) {
                // A lost warm is a cache miss, never a reason to fail a durable write.
            }
        }
        return List.copyOf(attempted);
    }

    private List<Peer> owners(DurableSegmentSignalFrame frame,
            List<EndpointSliceView.Endpoint> endpoints) {
        Map<String, List<Peer>> byAz = new TreeMap<>();
        for (EndpointSliceView.Endpoint endpoint : endpoints) {
            if (endpoint == null || endpoint.az() == null || endpoint.az().isBlank()
                    || endpoint.az().equals(frame.writerAz())) {
                continue;
            }
            String peerUri = peerUri(endpoint.address(), port);
            Peer peer;
            try {
                peer = new Peer(endpoint.podId(), peerUri, endpoint.az());
            } catch (IllegalArgumentException | NullPointerException malformed) {
                continue;
            }
            byAz.computeIfAbsent(peer.az(), ignored -> new ArrayList<>()).add(peer);
        }
        List<Peer> selected = new ArrayList<>(byAz.size());
        byAz.forEach((az, peers) -> PeerRing.ownerOf(frame.segmentKey(),
                new AzPeers(az, peers.stream().distinct().sorted(Comparator
                        .comparing(Peer::podId).thenComparing(Peer::endpoint)).toList()))
                .ifPresent(selected::add));
        return List.copyOf(selected);
    }

    private static String peerUri(String address, int port) {
        Objects.requireNonNull(address, "address");
        String host = address.indexOf(':') >= 0 && !address.startsWith("[")
                ? "[" + address + "]" : address;
        return "http://" + host + ":" + port;
    }

    private static final class HttpPost implements PeerPost {
        private final Map<String, WebClient> clients = new ConcurrentHashMap<>();

        @Override
        public void post(String endpoint, byte[] body) throws IOException {
            try (var response = clientFor(endpoint).post(DurableSegmentSignalService.PATH)
                    .submit(body)) {
                // Any response, including a 404 from an older peer, is a missed
                // hint only. The durable write has already completed.
                response.status().code();
            } catch (RuntimeException unreachable) {
                throw new IOException("durable segment hint was not reachable", unreachable);
            }
        }

        private WebClient clientFor(String endpoint) {
            WebClient existing = clients.get(endpoint);
            if (existing != null) {
                return existing;
            }
            if (clients.size() >= MAX_POOLED_CLIENTS) {
                clients.clear();
            }
            // ⚠️ NO KEEP-ALIVE (M10.36): shared by every flushing thread, and
            // exposed to Helidon 4.3.0's connection-return race (M10.35).
            return clients.computeIfAbsent(endpoint, uri -> WebClient.builder()
                    .baseUri(URI.create(uri)).connectTimeout(TIMEOUT).readTimeout(TIMEOUT)
                    .keepAlive(false).build());
        }
    }
}
