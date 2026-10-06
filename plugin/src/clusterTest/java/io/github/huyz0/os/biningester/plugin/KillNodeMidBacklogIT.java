// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.server.IngesterNode;
import io.github.huyz0.os.biningester.server.Main;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.junit.AfterClass;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/** T4 proof that a restarted OpenSearch node gets a fresh consumer subscription. */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST,
        numDataNodes = 2, supportsDedicatedMasters = false)
@ThreadLeakFilters(defaultFilters = true, filters = {KillNodeMidBacklogIT.HelidonTimerFilter.class})
public class KillNodeMidBacklogIT extends OpenSearchIntegTestCase {

    private static final Map<String, AtomicInteger> BUILDS = new ConcurrentHashMap<>();
    private static volatile CountDownLatch CATCH_UP_HELD = new CountDownLatch(1);
    private static volatile CountDownLatch CATCH_UP_RELEASE = new CountDownLatch(1);
    private static final HttpClient PRODUCER = HttpClient.newHttpClient();
    private static final AtomicInteger LIVE_RECORDS_BUFFERED = new AtomicInteger();
    private static volatile boolean HOLD_CATCH_UP;
    private static volatile boolean BUFFER_LIVE;

    static {
        BinStorePlugin.install(node -> {
            BUILDS.computeIfAbsent(node, ignored -> new AtomicInteger()).incrementAndGet();
            String endpoint = TestIngester.endpoint();
            NodeChannel channel = new NodeChannel(onReconnect -> new GatedTransport(
                    new HttpSubscriptionTransport(endpoint, onReconnect,
                            Duration.ofMillis(10), Duration.ofSeconds(1), Duration.ofSeconds(5))));
            return NodeSubscriptions.fetching(channel,
                    BinStorePlugin.QUEUE_CAPACITY, NodeSubscriptions.DEFAULT_SEGMENT_HOLD_BYTES);
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.List.of(BinStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(BinStorePlugin.PROGRESS_INTERVAL.getKey(), "100ms")
                .put(BinStorePlugin.INGESTER_ENDPOINT.getKey(), TestIngester.endpoint())
                .build();
    }

    public void testRestartedNodeRebuildsItsSubscriptionState() throws Exception {
        String node = internalCluster().getNodeNames()[0];
        assertBusy(() -> assertNotNull(BUILDS.get(node)));
        int before = BUILDS.get(node).get();

        internalCluster().restartNode(node);

        assertBusy(() -> assertTrue("a restarted node must not inherit the stopped node's "
                        + "subscription", BUILDS.get(node).get() > before));
    }

    public void testCatchUpRecoversBacklogWhileLiveTailRemainsPending() throws Exception {
        String node = internalCluster().getNodeNames()[0];
        createIndex("logs", Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.routing.allocation.require._name", node)
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        ensureGreen("logs");

        for (int i = 0; i < 10; i++) {
            write("pre-kill-" + i);
        }
        assertBusy(() -> assertEquals(10L, search(QueryBuilders.matchAllQuery())));
        client().admin().indices().prepareFlush("logs").get();
        client().admin().indices().prepareClose("logs").get();

        // The closed index leaves 100 individually flushed segments outstanding at node death.
        var beforeBacklog = TestIngester.node.assembly().storeCounts();
        for (int i = 0; i < 100; i++) {
            write("backlog-" + i);
        }
        var afterBacklog = TestIngester.node.assembly().storeCounts();
        long backlogPuts = afterBacklog.puts() - beforeBacklog.puts();
        assertTrue("the killed node must have at least 100 durable backlog segments; segment "
                        + "PUTs observed=" + backlogPuts,
                backlogPuts >= 100);
        int buildsBeforeRestart = BUILDS.get(node).get();
        internalCluster().restartNode(node);
        assertBusy(() -> assertTrue(BUILDS.get(node).get() > buildsBeforeRestart));

        CATCH_UP_HELD = new CountDownLatch(1);
        CATCH_UP_RELEASE = new CountDownLatch(1);
        LIVE_RECORDS_BUFFERED.set(0);
        BUFFER_LIVE = true;
        HOLD_CATCH_UP = true;
        io.github.huyz0.os.biningester.binstore.StoreCounts beforeCatchUpReads =
                TestIngester.node.assembly().storeCounts();
        client().admin().indices().prepareOpen("logs").get();
        ensureGreen("logs");
        long[] ackedAt = new long[1_000];
        try {
            assertTrue("the completed catch-up response must be held before handoff",
                    CATCH_UP_HELD.await(30, TimeUnit.SECONDS));
            long backlogGets = TestIngester.node.assembly().storeCounts().gets()
                    - beforeCatchUpReads.gets();
            assertTrue("initial catch-up reads at most one GET per pre-kill backlog segment "
                            + "plus the two persisted batch_start boundary segments; backlog="
                            + backlogPuts + " gets=" + backlogGets,
                    backlogGets <= backlogPuts + 2);
            int bufferedBeforeLiveTail = LIVE_RECORDS_BUFFERED.get();
            for (int first = 0; first < ackedAt.length; first += 10) {
                List<String> ids = java.util.stream.IntStream.range(first, first + 10)
                        .mapToObj(i -> "live-" + i).toList();
                writeBatch(ids);
                long acked = System.nanoTime();
                java.util.Arrays.fill(ackedAt, first, first + ids.size(), acked);
            }
            long tailPuts = TestIngester.node.assembly().storeCounts().puts()
                    - afterBacklog.puts();
            assertBusy(() -> assertTrue("at least 1,000 live records must stay queued while "
                            + "catch-up is in progress",
                    LIVE_RECORDS_BUFFERED.get() >= bufferedBeforeLiveTail + 1_000),
                    30, TimeUnit.SECONDS);
            assertTrue("both consumer shards must have a live-tail segment",
                    tailPuts >= 2);
        } finally {
            HOLD_CATCH_UP = false;
            CATCH_UP_RELEASE.countDown();
        }

        assertBusy(() -> {
            assertEquals(1_110L, search(QueryBuilders.matchAllQuery()));
            assertEquals(555L, searchOnShard(0));
            assertEquals(555L, searchOnShard(1));
        },
                90, TimeUnit.SECONDS);
        long observed = System.nanoTime();
        long maxLatency = 0;
        for (long ack : ackedAt) {
            maxLatency = Math.max(maxLatency, observed - ack);
        }
        assertTrue("each post-kill record must be searchable within 3x the 5 s interval ceiling; "
                        + "max observed ns=" + maxLatency,
                maxLatency <= TimeUnit.SECONDS.toNanos(15));

        var counts = TestIngester.node.assembly().storeCounts();
        assertEquals("catch-up never lists", 0L,
                counts.lists() - beforeCatchUpReads.lists());
    }

    private long search(QueryBuilder query) {
        client().admin().indices().prepareRefresh("logs").get();
        SearchResponse response = client().prepareSearch("logs").setQuery(query).setSize(0).get();
        return response.getHits().getTotalHits().value();
    }

    private long searchOnShard(int shard) {
        client().admin().indices().prepareRefresh("logs").get();
        SearchResponse response = client().prepareSearch("logs")
                .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
                .setPreference("_shards:" + shard).get();
        return response.getHits().getTotalHits().value();
    }

    private static void write(String id) throws Exception {
        writeBatch(List.of(id));
    }

    private static void writeBatch(List<String> ids) throws Exception {
        StringBuilder body = new StringBuilder();
        for (String id : ids) {
            body.append("{\"index\":{\"_id\":\"").append(id)
                    .append("\",\"_version\":1}}\n")
                    .append("{\"message\":\"").append(id).append("\"}\n");
        }
        int partition = partitionOf(ids.getFirst());
        HttpResponse<String> response = PRODUCER.send(HttpRequest.newBuilder(
                        URI.create(TestIngester.endpoint() + "/logs/_bulk?partition=" + partition))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals("_bulk must acknowledge its durable record", 202, response.statusCode());
    }

    private static int partitionOf(String id) {
        int record = Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
        return id.startsWith("live-") ? record / 10 % 2 : record % 2;
    }

    @AfterClass
    public static void closeIngester() throws IOException {
        BinStorePlugin.uninstall();
        TestIngester.close();
        PRODUCER.close();
    }

    /** Helidon's shared idle timer is process-global and has no per-client close API. */
    public static final class HelidonTimerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().equals("helidon-idle-connection-timer");
        }
    }

    private static final class GatedTransport implements SubscriptionTransport, AutoCloseable {
        private final HttpSubscriptionTransport delegate;
        private final ConcurrentLinkedQueue<Delivery> bufferedLive = new ConcurrentLinkedQueue<>();
        private volatile Listener liveListener;

        private GatedTransport(HttpSubscriptionTransport delegate) {
            this.delegate = delegate;
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return delegate.subscribe(key, gate(listener));
        }

        @Override
        public MultiSubscription subscribe(List<RunKey> keys, Listener listener) {
            return delegate.subscribe(keys, gate(listener));
        }

        private Listener gate(Listener listener) {
            liveListener = listener;
            return new Listener() {
                @Override
                public void onDelivery(Delivery delivery) {
                    if (BUFFER_LIVE) {
                        bufferedLive.add(delivery);
                        LIVE_RECORDS_BUFFERED.addAndGet(delivery.recordCount());
                    } else {
                        listener.onDelivery(delivery);
                    }
                }

                @Override
                public void onRetainedFloor(RunKey key, long floor) {
                    listener.onRetainedFloor(key, floor);
                }

                @Override
                public boolean wantsRetainedFloor(RunKey key) {
                    return listener.wantsRetainedFloor(key);
                }
            };
        }

        @Override
        public void register(IndexRegistration registration) {
            delegate.register(registration);
        }

        @Override
        public void report(ConsumerProgress progress) {
            delegate.report(progress);
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<SubscriptionEvent> lane) throws IOException {
            CatchUpResult result = delegate.requestCatchUp(request, lane);
            if (HOLD_CATCH_UP) {
                CATCH_UP_HELD.countDown();
                HOLD_CATCH_UP = false;
                try {
                    if (!CATCH_UP_RELEASE.await(60, TimeUnit.SECONDS)) {
                        throw new IOException("test did not release the completed catch-up exchange");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("test catch-up wait interrupted", interrupted);
                }
            }
            if (BUFFER_LIVE) {
                BUFFER_LIVE = false;
                Delivery delivery;
                while ((delivery = bufferedLive.poll()) != null) {
                    liveListener.onDelivery(delivery);
                }
            }
            return result;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class TestIngester {
        private static IngesterNode node;

        static synchronized String endpoint() {
            if (node == null) {
                try {
                    Path temp = Files.createTempDirectory(Path.of("build", "tmp"),
                            "kill-node-mid-backlog-");
                    Path config = temp.resolve("node.properties");
                    Files.writeString(config, String.join("\n",
                            "pod.id=catchupnode",
                            "pod.uid=uid-catchupnode",
                            "peer.tls=off",
                            "peer.port=0",
                            "pod.az=az-a",
                            "trust.domain=cluster-a",
                            "store.prefix=bins/cluster-a",
                            "store.kind=memory",
                            "endpoint=http://127.0.0.1:8080",
                            "http.port=0",
                            "ingest.interval-floor=PT0.01S",
                            "ingest.interval-ceiling=PT5S",
                            "ingest.max-segment-bytes=1",
                            "producer.subject=producer-1",
                            "producer.allowed-indices=logs",
                            ""), StandardCharsets.UTF_8);
                    node = Main.run(config.toString());
                } catch (IOException failed) {
                    throw new IllegalStateException("could not start the test ingester", failed);
                }
            }
            return "http://127.0.0.1:" + node.port();
        }

        static synchronized void close() throws IOException {
            if (node != null) {
                node.close();
                node = null;
            }
        }
    }
}
