// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.common.settings.Settings;

/**
 * The consumer's fetch path as a REAL node builds it (M8.31, M5.91b, FR-10,
 * NFR-4, M8's criterion 23).
 *
 * <p>⚠️ **BEFORE THIS A REAL NODE BUILT NOTHING.** Only tests called
 * {@link BinStorePlugin#install}, so the plugin a node loaded reflectively
 * held no subscriptions, no transport and no fetcher.
 *
 * <p>⚠️ **THE GETs ARE COUNTED AT AN HTTP SERVER**, behind the production
 * fetcher, rather than at a fake delegate: a per-subscription cache -- the
 * mutation criterion 23 names -- is one GET per shard per segment, correct and
 * green on every read-back, and the count is the only thing that sees it.
 */
@Timeout(60)
class ProductionFetchPathTest {

    private static final int SEGMENTS = 4;

    private HttpServer server;
    private final AtomicInteger gets = new AtomicInteger();
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
        BinStorePlugin.uninstall();
    }

    private String serve() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            gets.incrementAndGet();
            byte[] body = objects.get(exchange.getRequestURI().getPath());
            exchange.sendResponseHeaders(body == null ? 404 : 200, body == null ? -1 : body.length);
            if (body != null) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /** Delivers what the test hands it to the node's ONE listener, which routes by key. */
    private static final class OneListenerTransport implements SubscriptionTransport {
        private volatile Listener listener;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener l) {
            return subscribe(List.of(key), l);
        }

        @Override
        public MultiSubscription subscribe(List<RunKey> keys, Listener l) {
            this.listener = l;
            return new MultiSubscription() {
                @Override
                public void add(RunKey key) {
                }

                @Override
                public void remove(RunKey key) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /** {@code shards} runs over TWO indices, half each. */
    private static List<RunKey> runs(int shards) {
        UUID a = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
        UUID b = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
        List<RunKey> keys = new ArrayList<>();
        for (int i = 0; i < shards; i++) {
            keys.add(new RunKey(i % 2 == 0 ? a : b, i / 2));
        }
        return keys;
    }

    private static byte[] segment(List<RunKey> keys, int n) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord("doc-" + n, OpType.INDEX, OptionalLong.of(1),
                    ("s" + n).getBytes(StandardCharsets.UTF_8)), 1_000L);
        }
        return writer.toByteArray(1_000L);
    }

    /** GETs the node's production fetch path costs for {@code shards} runs of four segments. */
    private int getsFor(int shards) throws Exception {
        String base = serve();
        List<RunKey> keys = runs(shards);
        OneListenerTransport transport = new OneListenerTransport();
        try (NodeSubscriptions node = NodeSubscriptions.fetching(
                new NodeChannel(reconnect -> transport), 64,
                NodeSubscriptions.DEFAULT_SEGMENT_HOLD_BYTES)) {
            List<ConsumerClient> clients = new ArrayList<>();
            for (RunKey key : keys) {
                clients.add(node.clientFor(key));
            }
            for (int s = 0; s < SEGMENTS; s++) {
                String path = "/bucket/seg-" + s;
                objects.put(path, segment(keys, s));
                Grant grant = new Grant(base + path + "?X-Amz-Signature=x",
                        Instant.EPOCH.plusSeconds(60));
                for (RunKey key : keys) {
                    transport.listener.onDelivery(new Delivery(key, "seg-" + s, 1, s,
                            FetchMode.DIRECT, new byte[0], grant));
                }
                for (ConsumerClient client : clients) {
                    assertThat(client.readNext(Duration.ofSeconds(5)))
                            .as("every run reads its record of segment %d", s).isPresent();
                }
            }
        } finally {
            server.stop(0);
        }
        return gets.getAndSet(0);
    }

    @Test
    void GETsScaleWithSEGMENTSAndDoNotMoveWhenTheSHARDCountDOUBLES() throws Exception {
        int atEight = getsFor(8);
        int atSixteen = getsFor(16);

        assertThat(atEight)
                .as("⚠️ ONE GET PER SEGMENT for eight shards over two indices -- a "
                        + "per-subscription cache is eight per segment")
                .isEqualTo(SEGMENTS);
        assertThat(atSixteen)
                .as("⚠️ AND THE SAME AT SIXTEEN: non-negotiable 6")
                .isEqualTo(atEight);
    }

    @Test
    void aREALNodeWithAnINGESTEREndpointBUILDSItsOwnSubscriptionsOverAChannel() {
        NodeSubscriptions built = new BinStorePlugin(Settings.builder()
                .put("node.name", "node-a")
                .put("binstore.ingester.endpoint", "http://127.0.0.1:9")
                .build()).subscriptions();

        try {
            assertThat(built)
                    .as("⚠️ NO FACTORY WAS INSTALLED, as none is in a real node: the plugin "
                            + "builds from the setting or holds nothing")
                    .isNotNull();
            assertThat(built.channel())
                    .as("over a CHANNEL, so the registrar a reconnect reaches is wired (M6.15)")
                    .isNotNull();
            assertThat(built.fetches())
                    .as("and with a fetch path: a direct delivery has something to read it")
                    .isTrue();
        } finally {
            if (built != null) {
                built.close();
            }
        }
    }

    @Test
    void theINGESTEREndpointIsARegisteredNODESetting() {
        assertThat(new BinStorePlugin(Settings.EMPTY).getSettings())
                .as("⚠️ AN UNREGISTERED SETTING REFUSES THE NODE AT BOOT")
                .contains(BinStorePlugin.INGESTER_ENDPOINT);
    }
}
