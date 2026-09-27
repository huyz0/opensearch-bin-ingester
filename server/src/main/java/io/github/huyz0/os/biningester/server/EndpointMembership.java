// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.AzPeers;
import io.github.huyz0.os.biningester.ingest.Membership;
import io.github.huyz0.os.biningester.ingest.Peer;

/**
 * This pod and its ready same-AZ peers, read from the live EndpointSlice view,
 * as the durable-segment prefetcher's membership, and the sender of this
 * pod's hints to them (both extracted from {@code Assembly} by M11.1,
 * unchanged).
 */
final class EndpointMembership implements Membership {
    private final ServerConfig config;
    private final EndpointSliceView view;

    EndpointMembership(ServerConfig config, EndpointSliceView view) {
        this.config = config;
        this.view = view;
    }

    @Override
    public Peer self() {
        return new Peer(config.podId(), config.endpoint(), config.az());
    }

    @Override
    public AzPeers localAz() {
        return new AzPeers(config.az(), view.readyEndpoints().stream()
                .filter(endpoint -> config.az().equals(endpoint.az()))
                .map(endpoint -> new Peer(endpoint.podId(), peerUri(endpoint.address(),
                        config.httpPort()), endpoint.az()))
                .toList());
    }

    private static String peerUri(String address, int port) {
        String host = address.indexOf(':') >= 0 && !address.startsWith("[")
                ? "[" + address + "]" : address;
        return "http://" + host + ":" + port;
    }

    /**
     * The sender of this pod's durable-segment hints to those peers, or
     * {@code null} where there is no live view or no port to name (extracted
     * from {@code Assembly} by M11.1, unchanged).
     */
    static DurableSegmentSignalSender signalSender(ServerConfig config,
            EndpointSliceView peerView, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost) {
        return peerView != null && config.httpPort() > 0
                ? signalPost == null
                        ? new DurableSegmentSignalSender(
                                crossAz == null ? CrossAzBytes.untracked() : crossAz,
                                config.httpPort())
                        : new DurableSegmentSignalSender(
                                crossAz == null ? CrossAzBytes.untracked() : crossAz,
                                config.httpPort(), signalPost)
                : null;
    }
}
