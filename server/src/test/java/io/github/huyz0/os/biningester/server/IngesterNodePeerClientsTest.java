// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport.NotTheLeaseholderException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The clients a {@code mutual} node BUILT reach a {@code mutual} peer
 * listener (M13.52d review round 1, P1, T1): the hint's sender, the forward
 * and drain transport, and the departure's -- each dialled here at the node's
 * own peer listener, which requires a certificate from the domain's CA. A
 * client built without the pod's certificate is refused at the handshake.
 */
class IngesterNodePeerClientsTest {

    // ⚠️ THIS POD's incarnation: the node dials itself with its own
    // certificate, and may claim only it (ADR-0084 decision 8; M13.52g).
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("pod1", "uid-pod1", "az-a", "");

    private IngesterNode node;
    private int peerPort;

    private static String fixture(String name) throws Exception {
        return Path.of(IngesterNodePeerClientsTest.class.getResource("/peer-tls/" + name)
                .toURI()).toString();
    }

    @BeforeEach
    void start() throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5 && node == null; attempt++) {
            try (ServerSocket probe = new ServerSocket(0)) {
                peerPort = probe.getLocalPort();
            }
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, "pod1");
            settings.put(ServerProperties.POD_UID, "uid-pod1");
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "memory");
            settings.put(ServerProperties.ENDPOINT, "https://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, "0");
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            // ⚠️ A VIEW, so the node builds the hint's sender; nothing answers.
            settings.put(ServerProperties.MEMBERSHIP_API, "https://127.0.0.1:1");
            settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");
            settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");
            settings.put(ServerProperties.MEMBERSHIP_TOKEN_FILE, "/var/run/token");
            settings.put(ServerProperties.PEER_TLS, "mutual");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.PEER_TLS_CERT, fixture("pod1.pem"));
            settings.put(ServerProperties.PEER_TLS_KEY, fixture("pod1.key"));
            settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
            ServerConfig config = ServerProperties.parse(settings);
            try {
                node = IngesterNode.start(config, Clock.systemUTC(), System::nanoTime,
                        Optional::empty, List.of(), Main.peerTls(config.peer(), message -> { }));
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        if (node == null) {
            throw last;
        }
    }

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void theNODEsHintSenderDialsHttpsWithThePodsCertificate() {
        DurableSegmentSignalSender sender = node.assembly().signalSender;

        List<String> attempted = sender.send(new DurableSegmentSignalFrame("pod1", "az-a",
                        new SegmentKey("bins/cluster-a", 1, "pod1", 1, 48).key()),
                List.of(new EndpointSliceView.Endpoint("pod9", "localhost", "az-b")));

        assertThat(attempted).containsExactly("https://localhost:" + peerPort);
        assertThat(sender.lost()).as("the post was answered, not swallowed").isZero();
    }

    @Test
    void theNODEsSequencerTransportIsAnsweredByAMutualPeer() {
        assertThatCode(() -> {
            try {
                node.sequencerTransport().drain("https://localhost:" + peerPort, "pod1");
            } catch (NotTheLeaseholderException answered) {
                // answered by the peer, which is all this checks
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void theNODEsDepartureTransportIsAnsweredByAMutualPeer() {
        assertThatCode(() -> node.departureTransport().exchange(
                "https://localhost:" + peerPort, FastFrame.encode(0, POD.podUid(), "uid-pod1",
                        new FastFrame.Join(POD, FastFrame.Held.NONE))))
                .doesNotThrowAnyException();
    }
}
