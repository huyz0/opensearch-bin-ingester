// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.binstore.backend.LocalFsBinStore;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IngestConfig;
import binjava.ingest.SubscriptionHub;
import binjava.security.Principal;
import binjava.sequencer.TestSequencers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.ShardIngestionState;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * T4 — what a poison record does to a shard, under each
 * {@code error_strategy} (M6.8, FR-7, M6 criterion 4).
 *
 * <p>⚠️ BOTH HALVES ARE INVISIBLE AT THE SPI BOUNDARY. This plugin never parses
 * a payload — {@code BinStoreMessage} carries opaque bytes — so the failure
 * happens inside OpenSearch's own processor, and which of "skip it" and "stop
 * the shard" happens is a property of the engine's configuration, not of any
 * seam this project owns. A fake consumer cannot answer it.
 *
 * <p>⚠️ THE POISON IS A MALFORMED DOCUMENT, and it is poison at the layer that
 * matters: the ingester framed it without reading it and the consumer handed it
 * over unchanged, exactly as it would in production. Nothing in this project
 * validates a producer's JSON, and ADR-0020 records why — parsing on the write
 * path would put a JSON reader on the hot path of every record and would reject
 * documents the producer considers valid.
 *
 * <p>⚠️ JUnit 4, because {@code OpenSearchSingleNodeTestCase} runs under
 * RandomizedRunner; {@code check-tdd} does not see these methods (AGENTS.md
 * § Gates records that blind spot), so the reds here were observed by hand:
 * swapping the two indices' strategies makes each case fail.
 */
public class ErrorStrategyIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("dropping", "blocking"));

    static {
        BinStorePlugin.install(new NodeSubscriptions(new HubTransport(), 64));
    }

    private static final class HubTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, SubscriptionHub.assembling(
                    push -> listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(),
                            push.segment(), push.grant(), push.sequencerEpoch()))));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    /**
     * Under {@code DROP}, the poison is skipped and the shard KEEPS INGESTING.
     *
     * <p>⚠️ THE SECOND GOOD RECORD IS THE WHOLE CASE. A shard that indexed the
     * first good record and then stopped satisfies "the good records arrive"
     * for the records BEFORE the poison, which is what a BLOCK-behaving shard
     * looks like from the outside if you only count once.
     */
    public void testUnderDROPThePoisonIsSkippedAndTheShardKeepsIngesting() throws Exception {
        createIngestingIndex("dropping", "DROP");
        try (DefaultIngest ingest = ingesterFor("dropping")) {
            ingest.append(PRINCIPAL, "dropping", 0, List.of(
                    good("before"), poison(), good("after"))::forEach);

            assertBusy(() -> assertEquals(
                    "the records either side of the poison are indexed, and the poison is not",
                    2L, totalHits("dropping")), 60, TimeUnit.SECONDS);

            assertEquals("and the poller is still POLLING, which is the state an operator "
                            + "reads -- the counterpart of BLOCK's stopped poller and the "
                            + "reason the two cases cannot both be satisfied by one behaviour",
                    "POLLING", pollerState("dropping"));

            ingest.append(PRINCIPAL, "dropping", 0, List.of(good("later"))::forEach);
            assertBusy(() -> assertEquals(
                    "and the shard is still ingesting AFTERWARDS -- a shard that stopped at "
                            + "the poison would hold the two above and nothing more, which is "
                            + "BLOCK's behaviour wearing DROP's name",
                    3L, totalHits("dropping")), 60, TimeUnit.SECONDS);
        }
    }

    /**
     * Under {@code BLOCK}, the shard STOPS rather than skipping.
     *
     * <p>⚠️ THE PRE-POISON RECORD IS THE POSITIVE CONTROL. Without it, "nothing
     * arrived" is satisfied by an ingester that was never connected, a stream
     * nobody subscribed to, or an index that failed to start — and the case
     * would pass with the whole path broken.
     */
    public void testUnderBLOCKTheShardSTOPSRatherThanSkipping() throws Exception {
        createIngestingIndex("blocking", "BLOCK");
        try (DefaultIngest ingest = ingesterFor("blocking")) {
            ingest.append(PRINCIPAL, "blocking", 0, List.of(good("before"))::forEach);
            assertBusy(() -> assertEquals("PREMISE: the path works and the shard is ingesting",
                    1L, totalHits("blocking")), 60, TimeUnit.SECONDS);

            ingest.append(PRINCIPAL, "blocking", 0, List.of(poison(), good("after"))::forEach);

            // ⚠️ A POSITIVE ASSERTION, NOT A SLEEP. Round 1 MEASURED what the
            // absence alone was worth: a consumer mutated to hand over the
            // first record and nothing ever again left this case GREEN, because
            // "the record after the poison never arrived" is equally true of a
            // batch that was never delivered. The poller's own state says
            // STOPPED rather than "nothing happened", and it is what an
            // operator reads too.
            assertBusy(() -> assertEquals("under BLOCK the shard's poller STOPS at the poison "
                            + "-- an operator is meant to find a stuck shard and a reason, not "
                            + "a silently short index",
                    "PAUSED", pollerState("blocking")), 60, TimeUnit.SECONDS);
            assertEquals("and the record AFTER the poison is not indexed: a poller that "
                            + "reported blocked while still applying records past the poison "
                            + "would be the silent skip under another name",
                    1L, totalHits("blocking"));
        }
    }

    private void createIngestingIndex(String name, String errorStrategy) throws Exception {
        createIndex(name, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", errorStrategy)
                .build());
        ensureGreen(name);
    }

    private DefaultIngest ingesterFor(String indexName) throws Exception {
        // ⚠️ UNDER THE TEST'S OWN TEMP ROOT, so the framework deletes it: a
        // case writing into the system temp directory leaves a segment tree
        // behind on every run.
        Path root = Files.createDirectory(createTempDir().resolve(indexName + "-store"));
        LocalFsBinStore store = new LocalFsBinStore(root);
        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index(indexName).getIndexUUID();
        UUID stream = BinStoreConsumerFactory.indexUuidOf(indexUuid);
        return new DefaultIngest(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                store, "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), HUB, Clock.systemUTC(),
                index -> stream);
    }

    /**
     * ⚠️ THE POLLER'S OWN STATE, through the same API an operator uses
     * (`_ingestion/_state`). Asserting only on document counts cannot tell a
     * shard that STOPPED from a batch that never arrived.
     */
    private String pollerState(String index) throws Exception {
        ShardIngestionState[] states = client().execute(GetIngestionStateAction.INSTANCE,
                new GetIngestionStateRequest(new String[] {index})).get().getShardStates();
        assertEquals("one shard, one state", 1, states.length);
        return states[0].getPollerState();
    }

    private long totalHits(String index) {
        client().admin().indices().prepareRefresh(index).get();
        return client().prepareSearch(index)
                .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    private static SegmentRecord good(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"n\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    /**
     * ⚠️ TRUNCATED JSON: the record is well-formed at every layer this project
     * owns -- a valid id, a valid op type, a valid version, a framed payload --
     * and unparseable at the one layer that reads it. That is what a real
     * poison record looks like here, because nothing on the write path parses
     * a document.
     */
    private static SegmentRecord poison() {
        return new SegmentRecord("poison", OpType.INDEX, OptionalLong.of(1),
                "{\"n\":".getBytes(StandardCharsets.UTF_8));
    }

}
