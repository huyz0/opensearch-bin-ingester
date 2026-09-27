// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.common.settings.Settings;

/**
 * A proxied segment is fetched ONCE PER NODE, by segment key, and the node
 * names its zone on every poll and fetch (M10.3, M10 criterion 4, FR-6,
 * NFR-4, NFR-5, ADR-0073).
 *
 * <p>⚠️ THE ROUTE FETCHES ARE COUNTED AT AN HTTP SERVER standing in for the
 * ingester's {@code /seg} route, behind the production fetcher that
 * {@link NodeSubscriptions#fetching} builds -- not at a fake delegate. A
 * {@code proxy} event carries coordinates only, and every run of one segment
 * gets its OWN delivery, so a fetch per shard subscription is correct, green
 * on every read-back, and one ingester GET per shard per segment: the
 * shards-per-node scaling non-negotiable 6 forbids by name. The count is the
 * only thing that sees it.
 *
 * <p>⚠️ AND EVERY RUN READS ITS OWN RECORD, because a fetch count of one is
 * also what "the first run fetched and the rest decoded nothing" gives.
 */
@Timeout(60)
class NodeSegmentSourceProxyTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000cc");
    private static final String SEGMENT_KEY = "bins/cluster-a/data/2026/09/27/10/seg-16.bseg";
    private static final int SHARDS = 16;

    private HttpServer server;
    private final List<Map<String, String>> routeFetches = new CopyOnWriteArrayList<>();
    private final List<Map<String, String>> polls = new CopyOnWriteArrayList<>();
    private final CountDownLatch polled = new CountDownLatch(1);
    private final Map<String, byte[]> segments = new java.util.concurrent.ConcurrentHashMap<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
        BinStorePlugin.uninstall();
    }

    private static Map<String, String> query(String raw) {
        if (raw == null || raw.isEmpty()) {
            return new HashMap<>();
        }
        return Arrays.stream(raw.split("&"))
                .map(p -> p.split("=", 2))
                .collect(Collectors.toMap(p -> p[0],
                        p -> p.length < 2 ? "" : URLDecoder.decode(p[1], StandardCharsets.UTF_8),
                        (a, b) -> b, HashMap::new));
    }

    /** An ingester stand-in: the segment route, and a subscribe path that records polls. */
    private String startIngester() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        server.createContext(SegmentFetchRoute.PATH, exchange -> {
            Map<String, String> asked = query(exchange.getRequestURI().getRawQuery());
            routeFetches.add(asked);
            byte[] body = segments.get(asked.getOrDefault(SegmentFetchRoute.KEY_PARAM, ""));
            exchange.sendResponseHeaders(body == null ? 404 : 200, body == null ? -1 : body.length);
            if (body != null) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
            exchange.close();
        });
        server.createContext("/", exchange -> {
            polls.add(query(exchange.getRequestURI().getRawQuery()));
            polled.countDown();
            // ⚠️ A REFUSAL, so the transport backs off rather than spinning:
            // the poll's REQUEST is what this test reads.
            exchange.sendResponseHeaders(503, -1);
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

    private static List<RunKey> runs() {
        List<RunKey> keys = new ArrayList<>();
        for (int i = 0; i < SHARDS; i++) {
            keys.add(new RunKey(INDEX, i));
        }
        return keys;
    }

    /** One record per run, its payload naming the run it belongs to. */
    private static byte[] segmentWithARunPerKey(List<RunKey> keys) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord("doc-" + key.partitionId(), OpType.INDEX,
                    OptionalLong.of(1),
                    ("run-" + key.partitionId()).getBytes(StandardCharsets.UTF_8)), 1_000L);
        }
        return writer.toByteArray(1_000L);
    }

    @Test
    void SIXTEENShardSubscriptionsReadingONESegmentKeyIssueONERouteFetch() throws Exception {
        String ingester = startIngester();
        List<RunKey> keys = runs();
        segments.put(SEGMENT_KEY, segmentWithARunPerKey(keys));
        OneListenerTransport transport = new OneListenerTransport();

        List<String> read = new CopyOnWriteArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        try (NodeSubscriptions node = NodeSubscriptions.fetching(
                new NodeChannel(reconnect -> transport), 64,
                NodeSubscriptions.DEFAULT_SEGMENT_HOLD_BYTES, ingester, "az-b")) {
            List<ConsumerClient> clients = new ArrayList<>();
            for (RunKey key : keys) {
                clients.add(node.clientFor(key));
            }
            for (RunKey key : keys) {
                // ⚠️ AN EMPTY SEGMENT, AS OVER HTTP: a proxy event carries
                // coordinates only, and each run gets its own.
                transport.listener.onDelivery(new Delivery(key, SEGMENT_KEY, 1,
                        10L * key.partitionId(), FetchMode.PROXY, new byte[0]));
            }
            // ⚠️ ALL SIXTEEN READ AT ONCE, so the dedup is asked while a fetch
            // of the key is in flight as well as after it has landed.
            CyclicBarrier start = new CyclicBarrier(SHARDS);
            List<Thread> readers = new ArrayList<>();
            for (ConsumerClient client : clients) {
                readers.add(Thread.ofVirtual().start(() -> {
                    try {
                        start.await(10, TimeUnit.SECONDS);
                        ConsumerRecord record = client.readNext(Duration.ofSeconds(10))
                                .orElseThrow();
                        read.add(new String(record.record().payload(), StandardCharsets.UTF_8));
                    } catch (Throwable failed) {
                        failures.add(failed);
                    }
                }));
            }
            for (Thread reader : readers) {
                reader.join(Duration.ofSeconds(30));
            }
        }

        assertThat(failures).as("no run failed to read the proxied segment").isEmpty();
        assertThat(routeFetches)
                .as("⚠️ ONE ROUTE FETCH for %d shard subscriptions of one segment key -- a "
                        + "fetch per subscription is %d ingester GETs per segment, "
                        + "shards-per-node scaling (non-negotiable 6)", SHARDS, SHARDS)
                .hasSize(1);
        assertThat(routeFetches.getFirst())
                .as("by the exact key, naming this node's zone (NFR-5)")
                .containsEntry(SegmentFetchRoute.KEY_PARAM, SEGMENT_KEY)
                .containsEntry(SegmentFetchRoute.AZ_PARAM, "az-b");
        List<String> expected = new ArrayList<>();
        keys.forEach(key -> expected.add("run-" + key.partitionId()));
        assertThat(read)
                .as("EVERY run decodes its OWN record from the one fetch")
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Serves by key and counts what reached it. */
    private static final class CountingKeySource implements SegmentSource {
        private final AtomicInteger byKey = new AtomicInteger();
        private final AtomicInteger byGrant = new AtomicInteger();
        private volatile IOException failNext;

        @Override
        public byte[] fetch(Grant grant) {
            byGrant.incrementAndGet();
            return ("grant:" + grant.url()).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            byKey.incrementAndGet();
            IOException failing = failNext;
            if (failing != null) {
                failNext = null;
                throw failing;
            }
            return ("key:" + segmentKey).getBytes(StandardCharsets.UTF_8);
        }
    }

    @Test
    void aFAILEDProxyFetchIsNOTHeldAndTheNEXTReadFetchesAgain() throws Exception {
        CountingKeySource delegate = new CountingKeySource();
        NodeSegmentSource node = new NodeSegmentSource(delegate, 1L << 20);
        delegate.failNext = new IOException("ingester answered 502");

        assertThatThrownBy(() -> node.fetchSegment("k"))
                .as("⚠️ the failure reaches the run, never an empty segment")
                .isInstanceOf(IOException.class);
        assertThat(node.fetchSegment("k"))
                .as("and nothing was held: the next read fetches and is served")
                .isEqualTo("key:k".getBytes(StandardCharsets.UTF_8));
        assertThat(node.fetchSegment("k")).isEqualTo("key:k".getBytes(StandardCharsets.UTF_8));
        assertThat(delegate.byKey.get())
                .as("the failed fetch and the one that landed; the third was held")
                .isEqualTo(2);
        assertThat(node.bytesHeld())
                .as("the hold is bounded by the same byte ceiling as grants'")
                .isEqualTo("key:k".length());
    }

    @Test
    void TWODistinctSegmentKeysCostTWOFetchesAndEACHGetsItsOWNBytes() throws Exception {
        CountingKeySource delegate = new CountingKeySource();
        NodeSegmentSource node = new NodeSegmentSource(delegate, 1L << 20);

        assertThat(node.fetchSegment("seg-a"))
                .isEqualTo("key:seg-a".getBytes(StandardCharsets.UTF_8));
        assertThat(node.fetchSegment("seg-b"))
                .as("⚠️ THE DEDUP IS BY KEY: a second segment is never answered with the "
                        + "first one's bytes, which would decode another segment's records")
                .isEqualTo("key:seg-b".getBytes(StandardCharsets.UTF_8));
        assertThat(node.fetchSegment("seg-a"))
                .isEqualTo("key:seg-a".getBytes(StandardCharsets.UTF_8));
        assertThat(delegate.byKey.get())
                .as("one route fetch per distinct key; the repeat was held")
                .isEqualTo(2);
    }

    @Test
    void aSegmentKeyAndAGrantURLNeverShareAHeldEntry() throws Exception {
        CountingKeySource delegate = new CountingKeySource();
        NodeSegmentSource node = new NodeSegmentSource(delegate, 1L << 20);
        String same = "https://store.example/seg";

        assertThat(node.fetch(new Grant(same, Instant.EPOCH.plusSeconds(60))))
                .isEqualTo(("grant:" + same).getBytes(StandardCharsets.UTF_8));
        assertThat(node.fetchSegment(same))
                .as("⚠️ a key spelled like a held grant's url is still fetched BY KEY: the "
                        + "two are different sources and must not answer for each other")
                .isEqualTo(("key:" + same).getBytes(StandardCharsets.UTF_8));
        assertThat(delegate.byKey.get()).isEqualTo(1);
    }

    @Test
    void aNodeConfiguredWithAZONENamesItOnPollsAndOnRouteFetches() throws Exception {
        String ingester = startIngester();
        segments.put(SEGMENT_KEY, "bytes".getBytes(StandardCharsets.UTF_8));
        NodeSubscriptions built = new BinStorePlugin(Settings.builder()
                .put("node.name", "node-az-b")
                .put(BinStorePlugin.INGESTER_ENDPOINT.getKey(), ingester)
                .put(BinStorePlugin.NODE_AZ.getKey(), "az-b")
                .build()).subscriptions();
        try {
            assertThat(built).isNotNull();
            built.clientFor(new RunKey(INDEX, 0));
            assertThat(polled.await(20, TimeUnit.SECONDS)).as("the node polled").isTrue();
            assertThat(polls.getFirst())
                    .as("⚠️ THE POLL NAMES THE ZONE, or the ingester counts every push to "
                            + "this node as cross-AZ (NFR-5)")
                    .containsEntry(SegmentFetchRoute.AZ_PARAM, "az-b");

            built.segmentSource().fetchSegment(SEGMENT_KEY);
            assertThat(routeFetches).hasSize(1);
            assertThat(routeFetches.getFirst())
                    .as("and so does the proxy route fetch, from the node's OWN ingester")
                    .containsEntry(SegmentFetchRoute.KEY_PARAM, SEGMENT_KEY)
                    .containsEntry(SegmentFetchRoute.AZ_PARAM, "az-b");
        } finally {
            if (built != null) {
                built.close();
            }
        }
        assertThat(new BinStorePlugin(Settings.EMPTY).getSettings())
                .as("⚠️ AN UNREGISTERED SETTING REFUSES THE NODE AT BOOT")
                .contains(BinStorePlugin.NODE_AZ);
        assertThat(BinStorePlugin.NODE_AZ.getProperties())
                .as("a NODE setting, never an index one")
                .contains(org.opensearch.common.settings.Setting.Property.NodeScope);
    }
}
