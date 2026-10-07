// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import java.net.ServerSocket;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The node counts every durable-segment hint it loses into its exported
 * counter, {@code biningester_durable_segment_hints_lost_total} (M13.70) --
 * read where an operator reads it, the scrape, by its literal name.
 */
class IngesterNodeHintLostTest {

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void aHINTThatCannotBePostedIsCountedByTheNode() throws Exception {
        int peerPort;
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
        settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        // ⚠️ A VIEW, so the node builds the hint's sender; nothing answers.
        settings.put(ServerProperties.MEMBERSHIP_API, "https://127.0.0.1:1");
        settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");
        settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");
        settings.put(ServerProperties.MEMBERSHIP_TOKEN_FILE, "/var/run/token");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        long before = scraped(node.port());

        node.assembly().signalSender.send(new DurableSegmentSignalFrame("pod1", "az-a",
                        new SegmentKey("bins/cluster-a", 1, "pod1", 1, 48).key()),
                // TEST-NET-1 (RFC 5737): routed nowhere, and no resolver asked
                List.of(new EndpointSliceView.Endpoint("pod9", "192.0.2.1", "az-b")));

        assertThat(scraped(node.port()) - before).isEqualTo(1);
    }

    private static final String NAME = "biningester_durable_segment_hints_lost_total";

    /** The counter as the node's scrape shows it, its TYPE line checked. */
    private static long scraped(int port) throws Exception {
        String exposition = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"
                        + port + "/observe/metrics?scope=application")).GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()).body();
        assertThat(exposition).contains("# TYPE " + NAME + " counter");
        String line = exposition.lines().filter(l -> l.startsWith(NAME + "{")).findFirst()
                .orElseThrow();
        return (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }
}
