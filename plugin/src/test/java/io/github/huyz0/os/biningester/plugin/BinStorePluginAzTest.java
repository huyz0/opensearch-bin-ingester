// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.huyz0.os.biningester.format.RunKey;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.common.settings.Settings;

/**
 * The plugin tells the ingester which zone it is in, on the poll and on the
 * proxy fetch, so a deployment's bytes are attributed by zone rather than
 * counted as unknown (M10.4, NFR-5).
 */
@Timeout(60)
class BinStorePluginAzTest {

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String raw = exchange.getRequestURI().getRawQuery();
            requests.add(exchange.getRequestURI().getPath() + "?"
                    + (raw == null ? "" : URLDecoder.decode(raw, StandardCharsets.UTF_8)));
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

    private BinStorePlugin plugin(String az) {
        Settings.Builder settings = Settings.builder()
                .put("node.name", "az-node-" + UUID.randomUUID())
                .put(BinStorePlugin.INGESTER_ENDPOINT.getKey(),
                        "http://127.0.0.1:" + server.getAddress().getPort());
        if (az != null) {
            settings.put(BinStorePlugin.INGESTER_AZ.getKey(), az);
        }
        return new BinStorePlugin(settings.build());
    }

    private void awaitRequest(String pathPrefix) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (requests.stream().noneMatch(r -> r.startsWith(pathPrefix))) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no request to " + pathPrefix + " in " + requests);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void theConfiguredAzReachesPollAndProxyRequests() throws Exception {
        try (BinStorePlugin plugin = plugin("az-b")) {
            NodeSubscriptions node = plugin.subscriptions();
            node.clientFor(new RunKey(UUID.randomUUID(), 0));
            awaitRequest("/sub/");
            assertThat(node.proxySource()).as("the premise: a node with an endpoint proxies")
                    .isNotNull();
            try {
                node.proxySource().fetch("bins/cluster-a/data/seg.bseg");
            } catch (java.io.IOException expected) {
                // the fake answers 503; only the request it saw matters
            }
        }

        assertThat(requests).filteredOn(r -> r.startsWith("/sub/")).isNotEmpty()
                .allMatch(r -> r.contains("az=az-b"));
        assertThat(requests).filteredOn(r -> r.startsWith("/seg")).hasSize(1)
                .allMatch(r -> r.contains("az=az-b"));
    }

    @Test
    void anUnsetAzSendsNone() throws Exception {
        try (BinStorePlugin plugin = plugin(null)) {
            NodeSubscriptions node = plugin.subscriptions();
            node.clientFor(new RunKey(UUID.randomUUID(), 0));
            awaitRequest("/sub/");
            assertThat(node.proxySource()).as("the premise: a node with an endpoint proxies")
                    .isNotNull();
            try {
                node.proxySource().fetch("bins/cluster-a/data/seg.bseg");
            } catch (java.io.IOException expected) {
                // the fake answers 503
            }
        }

        assertThat(requests).isNotEmpty().noneMatch(r -> r.contains("az="));
    }

    @Test
    void theAzIsARegisteredNodeSetting() {
        assertThat(new BinStorePlugin(Settings.EMPTY).getSettings())
                .as("an unregistered setting refuses the node at boot")
                .contains(BinStorePlugin.INGESTER_AZ);
        assertThat(BinStorePlugin.INGESTER_AZ.getKey()).isEqualTo("binstore.ingester.az");
        assertThat(BinStorePlugin.INGESTER_AZ.get(Settings.EMPTY)).isEmpty();
    }
}
