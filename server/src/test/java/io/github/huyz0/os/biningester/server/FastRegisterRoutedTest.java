// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A node routes a REGISTER to the catalog its term record is fed from
 * (M13.83a review round 1, T1): over its peer listener, as a writer sends
 * it.
 */
class FastRegisterRoutedTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private IngesterNode startOnAFreePort() throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            int peerPort;
            try (ServerSocket probe = new ServerSocket(0);
                    ServerSocket peerProbe = new ServerSocket(0)) {
                port = probe.getLocalPort();
                peerPort = peerProbe.getLocalPort();
            }
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, "pod1");
            settings.put(ServerProperties.POD_UID, "uid-pod1");
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders");
            settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    @Test
    void aREGISTERSentToTheLeaderReachesItsCatalog() throws Exception {
        node = startOnAFreePort();
        long epoch = LocalSequencer.underneath(node.assembly().heldTerm()).orElseThrow().epoch();
        IndexRegistration orders = IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w",
                "orders", 1).withFastSettings(250, true, 1);
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        FastFrame.Body answer = FastFrame.decode(transport.exchange(
                "http://localhost:" + node.peerPort(), FastFrame.encode(epoch, "uid-pod1",
                        "uid-pod1", new FastFrame.Register(orders)))).body();

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(node.assembly().catalog().resolve("orders")).hasValue(orders);
    }

    @Test
    void aREGISTERFromAPodNotRosteredIsRefusedThroughTheRoute() throws Exception {
        // ⚠️ its review round 2, T7: the route passes the frame's own header.
        node = startOnAFreePort();
        long epoch = LocalSequencer.underneath(node.assembly().heldTerm()).orElseThrow().epoch();
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        FastFrame.Body answer = FastFrame.decode(transport.exchange(
                "http://localhost:" + node.peerPort(), FastFrame.encode(epoch, "uid-stranger",
                        "uid-pod1", new FastFrame.Register(IndexRegistration.unsplit(
                                "nVzgup36TLqWp7VBBREj1w", "orders", 1)
                                .withFastSettings(250, true, 1))))).body();

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(node.assembly().catalog().resolve("orders")).isEmpty();
    }
}
