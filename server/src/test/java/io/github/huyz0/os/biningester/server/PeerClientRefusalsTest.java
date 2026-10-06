// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
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
 * What a pod-to-pod client refuses (M13.52d review round 1, T2, P2): a peer
 * whose certificate the domain's CA did not sign -- the CA being the clients'
 * only server authentication, with no host-name check -- and, under
 * {@code mutual}, a hint sender built without the pod's certificate.
 */
class PeerClientRefusalsTest {

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String fixture(String name) throws Exception {
        return Path.of(PeerClientRefusalsTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
    }

    private static ServerConfig mutual(String cert, String key, String peerPort)
            throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "https://localhost:9443");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "mutual");
        settings.put(ServerProperties.PEER_PORT, peerPort);
        settings.put(ServerProperties.PEER_TLS_CERT, fixture(cert));
        settings.put(ServerProperties.PEER_TLS_KEY, fixture(key));
        settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
        return ServerProperties.parse(settings);
    }

    @Test
    void aPEERPresentingAForeignCertificateIsRefusedByTheClient() throws Exception {
        // ⚠️ THE PEER TRUSTS THE DOMAIN's CA, so it would accept this client:
        // only the client's own trust can refuse it.
        ServerConfig impostor = mutual("foreign.pem", "foreign.key", "0");
        node = IngesterNode.start(impostor, Clock.systemUTC(), System::nanoTime,
                Optional::empty, List.of(), Main.peerTls(impostor.peer(), message -> { }));
        DurableSegmentSignalSender.PeerPost post = DurableSegmentSignalSender.httpPost(
                PeerClientTls.of(Main.peerTls(mutual("pod2.pem", "pod2.key", "0").peer(),
                        message -> { })));

        assertThatThrownBy(() -> post.post("https://localhost:" + node.peerPort(),
                new DurableSegmentSignalFrame("pod2", "az-b",
                        new SegmentKey("bins/cluster-a", 1, "pod2", 1, 48).key()).encode()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void aMUTUALHintSenderWithoutThePodsCertificateIsRefused() throws Exception {
        ServerConfig config = mutual("pod1.pem", "pod1.key", "9443");

        assertThatThrownBy(() -> EndpointMembership.signalSender(config,
                new EndpointSliceView(), CrossAzBytes.untracked(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS);
    }
}
