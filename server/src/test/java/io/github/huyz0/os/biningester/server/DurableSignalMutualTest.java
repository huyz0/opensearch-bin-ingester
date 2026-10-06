// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A durable-segment hint reaches a {@code mutual} peer with the pod's
 * certificate, and only with it (ADR-0084; M13.52d): the sender swallows a
 * failed post by design, so the post itself is what this checks.
 */
class DurableSignalMutualTest {

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(DurableSignalMutualTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
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

    private static byte[] hint() {
        return new DurableSegmentSignalFrame("pod2", "az-b",
                new SegmentKey("bins/cluster-a", 1, "pod2", 1, 48).key()).encode();
    }

    @Test
    void aHINTPostedWithThePodsCertificateReachesAMutualPeer() throws Exception {
        ServerConfig owner = mutual("pod1");
        node = IngesterNode.start(owner, Clock.systemUTC(), System::nanoTime, Optional::empty,
                List.of(), Main.peerTls(owner.peer(), message -> { }));
        DurableSegmentSignalSender.PeerPost post = DurableSegmentSignalSender.httpPost(
                PeerClientTls.of(Main.peerTls(mutual("pod2").peer(), message -> { })));

        assertThatCode(() -> post.post("https://localhost:" + node.peerPort(), hint()))
                .as("answered, whatever its status: the handshake succeeded")
                .doesNotThrowAnyException();
    }

    @Test
    void aHINTPostedWithoutAClientCertificateIsRefused() throws Exception {
        ServerConfig owner = mutual("pod1");
        node = IngesterNode.start(owner, Clock.systemUTC(), System::nanoTime, Optional::empty,
                List.of(), Main.peerTls(owner.peer(), message -> { }));
        // Trusting the domain's CA, so the server's certificate is not what
        // fails: the missing client certificate is.
        PeerTls read = Main.peerTls(mutual("pod2").peer(), message -> { }).orElseThrow();
        DurableSegmentSignalSender.PeerPost bare = DurableSegmentSignalSender.httpPost(
                Optional.of(io.helidon.common.tls.Tls.builder()
                        .enabledProtocols(List.of("TLSv1.3")).trust(read.ca())
                        .endpointIdentificationAlgorithm(
                                io.helidon.common.tls.Tls.ENDPOINT_IDENTIFICATION_NONE)
                        .build()));

        assertThatThrownBy(() -> bare.post("https://localhost:" + node.peerPort(), hint()))
                .isInstanceOf(IOException.class);
    }
}
