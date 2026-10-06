// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.http.HttpSequencerTransport;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The four pod-to-pod routes on the peer listener only, and every other route
 * where it was (ADR-0084; M13.52c): a route moved too far breaks a consumer, a
 * route left behind is the open door ADR-0084 closes.
 */
class PeerListenerRoutesTest {

    private static final List<String> PEER_ROUTES = List.of(HttpSequencerTransport.PATH,
            HttpSequencerTransport.DRAIN_PATH, "/ctl/durable-segment", "/ctl/fast");

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private IngesterNode start() throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
    }

    private static int post(int port, String path) {
        WebClient client = WebClient.builder().baseUri("http://localhost:" + port).build();
        try (HttpClientResponse response = client.post(path).submit(new byte[] {1, 2, 3})) {
            return response.status().code();
        }
    }

    @Test
    void theFOURPeerRoutesAreServedOnThePeerListenerAndNowhereElse() throws Exception {
        node = start();
        assertThat(node.peerPort()).as("the premise: two listeners")
                .isPositive().isNotEqualTo(node.port());

        for (String route : PEER_ROUTES) {
            assertThat(post(node.peerPort(), route)).as("%s on the peer listener", route)
                    .isNotEqualTo(404);
            assertThat(post(node.port(), route)).as("%s on the producer port", route)
                    .isEqualTo(404);
        }
    }

    @Test
    void theCONSUMERsRoutesStayOnTheProducerPort() throws Exception {
        node = start();

        for (String route : List.of(HttpSubscriptionTransport.REGISTER_PATH,
                HttpSubscriptionTransport.PROGRESS_PATH)) {
            assertThat(post(node.port(), route)).as("%s on the producer port", route)
                    .isNotEqualTo(404);
            assertThat(post(node.peerPort(), route)).as("%s on the peer listener", route)
                    .isEqualTo(404);
        }
        WebClient producer = WebClient.builder().baseUri("http://localhost:" + node.port())
                .build();
        try (HttpClientResponse bulk = producer.post("/logs/_bulk").queryParam("partition", "0")
                .submit("")) {
            assertThat(bulk.status().code()).as("the bulk route").isNotEqualTo(404);
        }
    }
}
