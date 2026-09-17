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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * T4 — a rollover moves NEW records and leaves OLD ones where they are
 * (M6.10, FR-19, ADR-0015 § 4, M6 criterion 6).
 *
 * <p>⚠️ NOTHING IS REPARTITIONED, BECAUSE NOTHING NEEDS TO BE. Stream identity
 * is {@code (indexUUID, partition)} per CONCRETE index, so a rollover is a new
 * index with new streams: the records already committed keep their offsets in
 * the previous index's streams and stay searchable there, and the new index
 * starts empty. A design that keyed streams by the ALIAS would have to move
 * them.
 *
 * <p>⚠️ THE STALENESS WINDOW IS ASSERTED POSITIVELY, not left as an absence.
 * Between the rollover and the plugin's push, the ingester's catalog still
 * resolves the alias to the PREVIOUS concrete index — whose shape is known — so
 * records land THERE. ADR-0015 § 4 calls that normal for time-series rollover
 * and requires it be documented rather than discovered, and ADR-0046 is why it
 * is not the pending pool's business: a rollover nobody has pushed yet is not
 * observable without asking OpenSearch per record.
 *
 * <p>⚠️ THE WINDOW IS MADE DETERMINISTIC BY HOLDING THE PUSH, rather than by
 * racing it. The fixture's transport queues registrations until the case
 * releases them, which is the only way to assert "before the push" without a
 * sleep and without depending on how fast a cluster-state change is applied.
 */
public class AliasRolloverIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    /**
     * ⚠️ A HOLDER, so a case can start with an empty one. The transport is
     * built once, when the node's plugin is installed, so it cannot close over
     * a per-case field -- and the catalog is exactly the state a second case
     * must not inherit.
     */
    private static final java.util.concurrent.atomic.AtomicReference<IndexCatalog> CATALOG =
            new java.util.concurrent.atomic.AtomicReference<>(new IndexCatalog());
    private static final List<IndexRegistration> HELD =
            java.util.Collections.synchronizedList(new ArrayList<>());
    private static final java.util.concurrent.atomic.AtomicBoolean HOLDING =
            new java.util.concurrent.atomic.AtomicBoolean();
    private static final Principal PRINCIPAL = new Principal("cluster-a", "producer-1",
            Set.of("logs", "logs-000001", "logs-000002"));

    static {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NodeTransport(), 1024));
    }

    /** Bridges the hub, and can HOLD registrations so the window is testable. */
    private static final class NodeTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, SubscriptionHub.assembling(
                    push -> listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(),
                            push.segment(), push.grant(), push.sequencerEpoch()))));
        }

        @Override
        public void register(IndexRegistration registration) {
            if (HOLDING.get()) {
                HELD.add(registration);
                return;
            }
            CATALOG.get().register(registration);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    /** ⚠️ The streams a concrete index's records live in, by index UUID. */
    private static final Map<String, UUID> STREAMS = new ConcurrentHashMap<>();

    /**
     * ⚠️ THE STATICS ARE RESET PER CASE. The hub, the catalog and the held
     * registrations outlive a test method -- {@code
     * OpenSearchSingleNodeTestCase} reuses its node across them -- so a second
     * case added here would inherit the first's catalog and its held pushes.
     * Latent with one case; a leak the moment there are two.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // ⚠️ THE HUB IS NOT RESET, and that is deliberate: the node is reused
        // across cases and its shard consumers are already subscribed to THIS
        // hub, so a fresh one would leave them listening to nothing.
        CATALOG.set(new IndexCatalog());
        HELD.clear();
        HOLDING.set(false);
        STREAMS.clear();
    }

    public void testARolloverMovesNewRecordsAndLeavesOldOnesWhereTheyAre() throws Exception {
        createIngestingIndex("logs-000001");
        assertTrue(client().admin().indices().prepareAliases()
                .addAlias("logs-000001", "logs").get().isAcknowledged());
        ensureGreen("logs-000001");
        assertBusy(() -> assertEquals("the node pushes the index's shape, with the alias",
                "logs-000001", CATALOG.get().resolve("logs").orElseThrow().indexName()),
                60, TimeUnit.SECONDS);

        try (DefaultIngest delegate = ingester();
                RoutedIngest ingest = routed(delegate)) {

            ingest.appendRouted(PRINCIPAL, "logs", "tenant-1",
                    List.of(document("before-rollover"))::forEach);
            assertBusy(() -> assertEquals("PREMISE: the ordinary path works before any "
                            + "rollover happens", 1L, hits("logs-000001")), 60,
                    TimeUnit.SECONDS);

            // ⚠️ THE PUSH IS HELD FROM HERE, so "before the plugin has pushed"
            // is a state this case CONTROLS rather than races.
            HOLDING.set(true);
            // ⚠️ THE ROLLOVER CARRIES THE INGESTION SETTINGS, and it has to:
            // OpenSearch builds the new index from the matching TEMPLATE, not
            // from the source index's settings, so a rollover that names
            // neither produces an ordinary index that ingests nothing at all.
            // Measured here first -- the new index was green, empty and never
            // registered, because the node had no BINSTORE index to report.
            // A real deployment puts these in an index template.
            assertTrue(client().admin().indices().prepareRolloverIndex("logs")
                    // ⚠️ A DIFFERENT SHARD COUNT from the previous index, so
                    // "the alias resolves to the index whose shape is KNOWN"
                    // is a claim with teeth: placing by the new registration
                    // during the window would compute a partition out of FOUR
                    // rather than out of two, and with both indices the same
                    // width the two are observationally identical.
                    .settings(ingestionSettings(4))
                    .get().isRolledOver());
            // ⚠️ THE NEW INDEX HAS TO BE ALLOCATED before the node has anything
            // to push about it: the registrar reports the indices this node
            // HOSTS, and a rolled-over index whose shards are still unassigned
            // is not one of them yet. Without this the case measured a
            // registration that was never generated rather than one held.
            ensureGreen("logs-000002");
            createStreamFor("logs-000002");

            ingest.appendRouted(PRINCIPAL, "logs", "tenant-1",
                    List.of(document("during-window"))::forEach);
            assertBusy(() -> assertEquals(
                    "IN THE WINDOW the alias still resolves to the PREVIOUS index, whose "
                            + "shape is known, so the record lands THERE -- ADR-0015 § 4 calls "
                            + "that normal for time-series rollover, and ADR-0046 says why it "
                            + "is not the pending pool's business",
                    2L, hits("logs-000001")), 60, TimeUnit.SECONDS);
            assertEquals("and NOT in the new index, which the ingester has heard nothing "
                            + "about yet. ⚠️ Corroboration, not an independent check: it is "
                            + "trivially true at t=0, and what carries this half is the "
                            + "positive assertion above",
                    0L, hits("logs-000002"));

            // ⚠️ THE PUSH ARRIVES.
            HOLDING.set(false);
            HELD.forEach(CATALOG.get()::register);
            HELD.clear();
            assertBusy(() -> assertEquals("the alias now resolves to the NEW index",
                    "logs-000002", CATALOG.get().resolve("logs").orElseThrow().indexName()),
                    60, TimeUnit.SECONDS);

            ingest.appendRouted(PRINCIPAL, "logs", "tenant-1",
                    List.of(document("after-push"))::forEach);
            assertBusy(() -> assertEquals("AFTER the push, records written to the alias land "
                            + "in the NEW index's streams", 1L, hits("logs-000002")), 60,
                    TimeUnit.SECONDS);
        }

        assertEquals("and the records already committed are STILL in the previous index, "
                        + "exactly as many as before: a rollover moves new records and "
                        + "repartitions nothing, because stream identity is (indexUUID, "
                        + "partition) per CONCRETE index and the new index is a new set of "
                        + "streams",
                2L, hits("logs-000001"));
    }

    private void createIngestingIndex(String name) throws Exception {
        createIndex(name, ingestionSettings(2));
        createStreamFor(name);
    }

    private static Settings ingestionSettings(int shards) {
        return Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build();
    }

    private void createStreamFor(String name) {
        String uuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index(name).getIndexUUID();
        STREAMS.put(name, BinStoreConsumerFactory.indexUuidOf(uuid));
    }

    private RoutedIngest routed(DefaultIngest delegate) {
        return new RoutedIngest(delegate, CATALOG.get(),
                new PendingPool(Clock.systemUTC(), Duration.ofSeconds(30), 8L << 20),
                Duration.ofSeconds(30), Clock.systemUTC());
    }

    private DefaultIngest ingester() throws Exception {
        Path root = Files.createDirectory(createTempDir().resolve("rollover-store"));
        LocalFsBinStore store = new LocalFsBinStore(root);
        return new DefaultIngest(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                store, "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), HUB, Clock.systemUTC(),
                // ⚠️ RESOLVED PER CONCRETE INDEX NAME, which is what makes the
                // two indices two stream sets rather than one.
                index -> STREAMS.get(index));
    }

    private long hits(String index) {
        client().admin().indices().prepareRefresh(index).get();
        return client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    private static SegmentRecord document(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"n\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8));
    }
}
