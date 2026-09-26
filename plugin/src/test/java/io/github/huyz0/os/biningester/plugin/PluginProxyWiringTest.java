// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.common.settings.Settings;

/**
 * A real plugin node -- built from its settings, as OpenSearch builds it --
 * fetches a {@code proxy} segment from the SAME ingester endpoint it
 * subscribes to (M10.2, FR-6, ADR-0073). Every other M10.2 test builds the
 * node's source by hand, so this is the one that constrains the wiring.
 */
@Timeout(60)
class PluginProxyWiringTest {

    private static final String KEY = "bins/cluster-a/data/2026/09/26/10/seg.bseg";

    private HttpServer server;
    private final List<String> segmentRequests = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/seg", exchange -> {
            String raw = exchange.getRequestURI().getRawQuery();
            segmentRequests.add(raw == null ? "" : URLDecoder.decode(raw, StandardCharsets.UTF_8));
            byte[] body = "the-segment".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        server.start();
        BinStorePlugin.uninstall();
    }

    @AfterEach
    void stop() {
        BinStorePlugin.uninstall();
        server.stop(0);
    }

    @Test
    void aPluginNodeBuiltFromItsSettingsFetchesProxySegmentsFromItsIngester() throws Exception {
        Settings settings = Settings.builder()
                .put("node.name", "wiring-node-" + UUID.randomUUID())
                .put(BinStorePlugin.INGESTER_ENDPOINT.getKey(),
                        "http://127.0.0.1:" + server.getAddress().getPort())
                .build();
        byte[] fetched;
        try (BinStorePlugin plugin = new BinStorePlugin(settings)) {
            assertThat(plugin.subscriptions().proxySource())
                    .as("a node with an ingester endpoint can serve `proxy` deliveries")
                    .isNotNull();
            fetched = plugin.subscriptions().proxySource().fetch(KEY);
        }

        assertThat(new String(fetched, StandardCharsets.UTF_8)).isEqualTo("the-segment");
        assertThat(segmentRequests).hasSize(1).allMatch(q -> q.contains("key=" + KEY));
    }
}
