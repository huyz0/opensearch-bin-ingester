// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.binstore.backend.LocalFsBinStore;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.IndexRegistration;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IndexCatalog;
import binjava.ingest.IngestConfig;
import binjava.ingest.PendingPool;
import binjava.ingest.RoutedIngest;
import binjava.ingest.SubscriptionHub;
import binjava.security.Principal;
import binjava.sequencer.TestSequencers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * T4 — M6's HEADLINE CRITERION: a producer writes to an ALIAS with a ROUTING
 * VALUE and names no partition, and the document is searchable BY THAT ROUTING
 * VALUE on a cluster of more than one node (M6.9, FR-7, M6 criteria 1 and 11).
 *
 * <p>⚠️ THIS IS THE ONE THAT CANNOT BE FAKED AT A SEAM. {@code _routing} is
 * never set on an ingested document — the ingestion path does not carry it —
 * so a routed search finds the document only if the partition WE computed is
 * the shard OpenSearch's own {@code OperationRouting} resolves that routing
 * value to. Searching WITHOUT the routing value would find it either way and
 * prove nothing, which is why every assertion here carries it.
 *
 * <p>⚠️ AND THE WHOLE LOOP IS REAL. The plugin's {@code IndexRegistrar} pushes
 * each index's shape over the node's own transport; this test's transport hands
 * that registration to the ingester's {@code IndexCatalog}; the producer's
 * routed write is placed by {@code RoutedIngest} from that catalog alone. No
 * step is short-circuited: nothing tells the ingester the shard count except
 * the node that hosts the index.
 *
 * <p>⚠️ THE SHARDS ARE ASSERTED TO BE ON MORE THAN ONE NODE rather than
 * assumed. A fixture whose shards all landed on one node satisfies the search
 * half and none of the rest.
 *
 * <p>⚠️ AND SUBSCRIBER IDENTITIES ARE COUNTED AT THE TRANSPORT, not clients at
 * the plugin: counting {@code clientFor} calls leaves a per-node client count
 * of 1 while one subscriber per shard is opened, which is the M5.62 shape
 * wearing this criterion's clothes.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST,
        numDataNodes = 2, supportsDedicatedMasters = false)
public class RoutedSearchIT extends OpenSearchIntegTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final IndexCatalog CATALOG = new IndexCatalog();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "logs-000001"));
    private static final int SHARDS = 4;

    /** Subscriber identities opened, per node — criterion 11's counter. */
    private static final Map<String, Set<Object>> SUBSCRIBERS = new ConcurrentHashMap<>();

    static {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NodeTransport(node), 1024));
    }

    /**
     * One per NODE, which M6.13 made possible: it bridges the ingester's hub to
     * the consumer's seam, records who subscribed, and carries the node's index
     * registrations to the ingester's catalog.
     */
    private static final class NodeTransport implements SubscriptionTransport {
        private final String nodeName;

        /**
         * ⚠️ ONE SUBSCRIBER OBJECT FOR THE WHOLE NODE, and the identity is the
         * point. {@code SubscriptionHub} groups by {@code Subscriber} IDENTITY
         * (M5.40a), so a node registering ONE object against K streams is
         * handed each segment once, and K separate registrations are K
         * consumers to the hub — which is K copies of the bytes. This is what
         * a production transport has to do, and what
         * {@code SubscriptionTransport.subscribe(List, Listener)}'s own javadoc
         * says its DEFAULT does not do.
         */
        private NodeTransport(String nodeName) {
            this.nodeName = nodeName;
        }

        private static SubscriptionHub.Subscriber subscriberFor(Listener listener) {
            return SubscriptionHub.assembling(
                    push -> listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(),
                            push.segment(), push.grant(), push.sequencerEpoch())));
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            // ⚠️ THE SINGLE-KEY FORM IS NOT WHAT THIS NODE USES, and a fixture
            // that served it from here would be measuring the default's
            // per-key shape rather than the merge. `NodeSubscriptions` opens
            // the multi-key form below.
            throw new UnsupportedOperationException(
                    "this transport registers one subscriber per NODE, not one per stream");
        }

        @Override
        public MultiSubscription subscribe(List<RunKey> keys, Listener listener) {
            SubscriptionHub.Subscriber subscriber = subscriberFor(listener);
            // ⚠️ COUNTED HERE: one subscriber IDENTITY opened per node, which
            // is what criterion 11 asks for. Counting invocations of the
            // single-key form instead collapses to 1 by construction, because
            // the default passes the SAME listener object for every key --
            // review measured two subscriptions on a two-shard node reading as
            // "1 subscriber".
            SUBSCRIBERS.computeIfAbsent(nodeName,
                            n -> java.util.Collections.newSetFromMap(new IdentityHashMap<>()))
                    .add(subscriber);
            Map<RunKey, SubscriptionHub.Subscription> handles = new ConcurrentHashMap<>();
            keys.forEach(key -> handles.put(key, HUB.subscribe(key, subscriber)));
            return new MultiSubscription() {
                @Override
                public void add(RunKey key) {
                    handles.computeIfAbsent(key, k -> HUB.subscribe(k, subscriber));
                }

                @Override
                public void remove(RunKey key) {
                    SubscriptionHub.Subscription handle = handles.remove(key);
                    if (handle != null) {
                        handle.close();
                    }
                }

                @Override
                public void close() {
                    handles.values().forEach(SubscriptionHub.Subscription::close);
                    handles.clear();
                }
            };
        }

        @Override
        public void register(IndexRegistration registration) {
            CATALOG.register(registration);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BinStorePlugin.class);
    }

    public void testARoutedWriteToAnAliasIsSearchableByThatRoutingValue() throws Exception {
        createIndex("logs-000001", Settings.builder()
                .put("index.number_of_shards", SHARDS)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        assertTrue("the alias must be created, or the producer's write names an index the "
                        + "catalog will never resolve",
                client().admin().indices().prepareAliases()
                        .addAlias("logs-000001", "logs").get().isAcknowledged());
        ensureGreen("logs-000001");

        // ⚠️ THE REGISTRATION ARRIVES FROM THE NODE, not from this test. The
        // plugin's listener pushes it over the transport above when the cluster
        // state changes; nothing else tells the ingester this index's shape.
        assertBusy(() -> assertTrue("the node must have pushed the index's shape, with the "
                        + "alias -- until it does, a routed write has nothing to be placed by",
                CATALOG.resolve("logs").isPresent()), 60, TimeUnit.SECONDS);
        IndexRegistration registered = CATALOG.resolve("logs").orElseThrow();
        assertEquals("and it carries the real shard count", SHARDS, registered.numShards());

        assertTrue("PREMISE: the shards are spread over MORE THAN ONE node. A fixture that "
                        + "happened to put them all on one satisfies the search half and "
                        + "nothing else this criterion asks for",
                nodesHoldingShards() > 1);

        String routing = "tenant-42";
        try (DefaultIngest delegate = ingester();
                RoutedIngest ingest = new RoutedIngest(delegate, CATALOG,
                        new PendingPool(Clock.systemUTC(), Duration.ofSeconds(30), 8L << 20),
                        Duration.ofSeconds(30), Clock.systemUTC())) {

            // ⚠️ NO PARTITION. The producer names the ALIAS and a routing
            // value, and nothing else -- which is the mode this milestone
            // exists to deliver.
            ingest.appendRouted(PRINCIPAL, "logs", routing,
                    List.of(document("doc-1", routing))::forEach);

            assertBusy(() -> assertEquals(
                    "the document is searchable BY ITS ROUTING VALUE: OpenSearch sends that "
                            + "search to the one shard its own OperationRouting resolves the "
                            + "value to, and the document is there only if our placement "
                            + "computed the same shard -- a mismatch is a silent miss, not an "
                            + "error",
                    1L, routedHits(routing)), 90, TimeUnit.SECONDS);
        }

        // ⚠️ THIS IS THE DUPLICATE-DELIVERY CHECK, NOT THE PLACEMENT ONE, and
        // saying so matters: an unrouted search fans out to every shard and
        // returns 1 whether the placement was right or wrong, so it cannot
        // prove anything about which shard holds the document -- the routed
        // assertion above is the only thing that can. What it DOES pin is that
        // the record arrived ONCE: a fan-out that counted more than the routed
        // search did would mean the same record was delivered to a second
        // shard as well.
        assertEquals("the record is in the index exactly ONCE -- more than the routed search "
                        + "found would mean it was delivered to a second shard too",
                1L, unroutedHits());

        for (String node : SUBSCRIBERS.keySet()) {
            assertEquals("each node opens ONE subscriber identity however many shards it "
                            + "hosts (criterion 11) -- counted at the TRANSPORT, because "
                            + "counting clients at the plugin reads 1 per node while one "
                            + "subscriber per shard is opened",
                    1, SUBSCRIBERS.get(node).size());
        }
        assertTrue("PREMISE: more than one node actually subscribed, or the loop above "
                        + "asserts nothing", SUBSCRIBERS.size() > 1);
    }

    private DefaultIngest ingester() throws Exception {
        Path root = Files.createDirectory(createTempDir().resolve("routed-store"));
        LocalFsBinStore store = new LocalFsBinStore(root);
        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs-000001").getIndexUUID();
        UUID stream = BinStoreConsumerFactory.indexUuidOf(indexUuid);
        return new DefaultIngest(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                store, "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), HUB, Clock.systemUTC(),
                index -> stream);
    }

    private int nodesHoldingShards() {
        Set<String> nodes = new java.util.HashSet<>();
        for (ShardRouting shard : client().admin().cluster().prepareState().get().getState()
                .routingTable().allShards("logs-000001")) {
            if (shard.assignedToNode()) {
                nodes.add(shard.currentNodeId());
            }
        }
        return nodes.size();
    }

    private long routedHits(String routing) {
        client().admin().indices().prepareRefresh("logs-000001").get();
        return client().prepareSearch("logs-000001")
                .setRouting(routing)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    private long unroutedHits() {
        client().admin().indices().prepareRefresh("logs-000001").get();
        return client().prepareSearch("logs-000001")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    private static SegmentRecord document(String id, String routing) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"tenant\":\"" + routing + "\"}").getBytes(StandardCharsets.UTF_8));
    }
}
