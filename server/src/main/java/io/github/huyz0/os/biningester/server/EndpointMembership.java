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
     * from {@code Assembly} by M11.1). ⚠️ ON THE PEER PORT (ADR-0084 decision 2,
     * M13.52c review round 1, P1): {@code /ctl/durable-segment} is served on
     * the peer listener only, and {@code peer.port} is fleet-wide.
     */
    static DurableSegmentSignalSender signalSender(ServerConfig config,
            EndpointSliceView peerView, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost) {
        return signalSender(config, peerView, crossAz, signalPost, () -> { });
    }

    /** The same, each lost hint told to {@code lostHint} -- the node's counter (M13.70). */
    static DurableSegmentSignalSender signalSender(ServerConfig config,
            EndpointSliceView peerView, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost, Runnable lostHint) {
        int peerPort = config.peer().port();
        // ⚠️ NO PLAINTEXT POST UNDER mutual (M13.52d review round 1, P2): it
        // would dial https:// without the pod's certificate, or http:// a
        // listener that speaks TLS -- every hint lost without a word.
        if (signalPost == null && config.peer().mode() == PeerConfig.Mode.MUTUAL) {
            throw new IllegalArgumentException("with " + ServerProperties.PEER_TLS
                    + " = mutual the durable-segment hint needs the pod's certificate");
        }
        return peerView != null && peerPort > 0
                ? signalPost == null
                        // ⚠️ THE COUNTER ON THIS PATH TOO (M13.70 review, P2)
                        ? new DurableSegmentSignalSender(
                                crossAz == null ? CrossAzBytes.untracked() : crossAz,
                                peerPort, DurableSegmentSignalSender.httpPost(
                                        java.util.Optional.empty()), false, lostHint)
                        // ⚠️ https UNDER mutual (ADR-0084; M13.52d), the post
                        // presenting this pod's certificate.
                        : new DurableSegmentSignalSender(
                                crossAz == null ? CrossAzBytes.untracked() : crossAz,
                                peerPort, signalPost,
                                config.peer().mode() == PeerConfig.Mode.MUTUAL, lostHint)
                : null;
    }
}
