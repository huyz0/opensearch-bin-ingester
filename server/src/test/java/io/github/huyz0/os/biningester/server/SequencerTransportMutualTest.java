// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.http.HttpSequencerTransport;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport.NotTheLeaseholderException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The lease-path transport -- the drain, whose client the forwarded commit
 * shares -- reaches a
 * {@code mutual} peer with the pod's certificate (ADR-0084; M13.52d). A node
 * pair under a fast term forwards on the fast path instead, so this path is
 * checked by itself.
 */
class SequencerTransportMutualTest {

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(SequencerTransportMutualTest.class.getResource("/peer-tls/" + name)
                .toURI()).toString();
    }

    private static ServerConfig mutual(String pod) throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, pod);
        settings.put(ServerProperties.POD_UID, "uid-" + pod);
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "https://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "mutual");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.PEER_TLS_CERT, fixture(pod + ".pem"));
        settings.put(ServerProperties.PEER_TLS_KEY, fixture(pod + ".key"));
        settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        return ServerProperties.parse(settings);
    }

    /** The drain, an answer of either kind counting: the handshake succeeded. */
    private static void drain(HttpSequencerTransport transport, String endpoint)
            throws IOException {
        try {
            transport.drain(endpoint, "pod2");
        } catch (NotTheLeaseholderException answered) {
            // answered by the peer, which is all this checks
        }
    }

    @Test
    void aDRAINWithThePodsCertificateIsAnsweredByAMutualPeer() throws Exception {
        ServerConfig owner = mutual("pod1");
        node = IngesterNode.start(owner, Clock.systemUTC(), System::nanoTime, Optional::empty,
                List.of(), Main.peerTls(owner.peer(), message -> { }));
        try (HttpSequencerTransport transport = new HttpSequencerTransport(
                Duration.ofSeconds(5), CrossAzBytes.untracked(),
                PeerClientTls.of(Main.peerTls(mutual("pod2").peer(), message -> { })))) {

            assertThatCode(() -> drain(transport, "https://localhost:" + node.peerPort()))
                    .doesNotThrowAnyException();
        }
    }
}
