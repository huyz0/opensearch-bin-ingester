// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;

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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Criterion 4's NODE-LEVEL half, closing the gap M1.17 left open.
 *
 * <p>⚠️ M1.17's three T2 tests prove the POINTER CONTRACT — round trip through
 * the string form, resume from a persisted pointer rather than `earliest`,
 * exactly-one-batch replay — against a `FakeTransport` that never starts a
 * node, because that is what a fake transport is. This test proves the other
 * half: that {@code batch_start} really is written into and read back out of
 * REAL Lucene commit data by a real OpenSearch node, and that a real engine
 * closed and reopened resumes ingestion rather than getting stuck.
 *
 * <p>⚠️ "Closed and reopened" here means the CLOSE/OPEN INDEX API, not a killed
 * JVM process. `IngestionEngine` uses a {@code NoOpTranslogManager} — verified
 * by reading OpenSearch's own source, not assumed — so there is no translog to
 * replay underneath this: closing an index discards the in-memory engine and
 * reopening it goes through the SAME shard-recovery path a node restart uses
 * (an existing local copy, {@code batch_start} read from Lucene commit data by
 * {@code IngestionEngine}'s own open logic). A full node PROCESS kill needs
 * {@code internalCluster().restartNode(...)}, which needs the
 * {@code OpenSearchIntegTestCase} multi-node harness this project does not
 * have wired up yet — a build-and-fixture cost on the scale of the six fixes
 * `clusterTest` itself needed (M1.15b), named here rather than silently
 * substituted for.
 *
 * <p>⚠️ This does NOT re-prove the exact duplicate bound M1.17 already proves.
 * `IngestionEngine.commitIndexWriter` prefers {@code batch_start} over
 * `pointer.init.reset` whenever commit data carries one — OpenSearch's own
 * contract, not ours — so a real restart cannot distinguish "resumed from the
 * persisted pointer" from "reset to earliest" by document count alone: both
 * replay the same {@code _id}s harmlessly. That precise arithmetic is what
 * `BinStoreShardConsumerTest`/`RestartResumeTest` prove deterministically, at
 * T2, where the pointer can be controlled directly. What only a real node can
 * prove is that our factory and consumer are correctly WIRED into OpenSearch's
 * real commit/recovery mechanism — which is what this test does.
 */
public class RestartResumeIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    static {
        // ⚠️ Before the node starts: it builds the plugin during setUp().
        BinStorePlugin.install(new NodeSubscriptions(new HubTransport(), 64));
    }

    private static final class HubTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, push -> listener.onDelivery(new Delivery(
                    push.key(), push.segmentKey(), push.recordCount(),
                    push.firstOffset(), push.segment())));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    public void testBatchStartSurvivesARealEngineCloseAndReopen() throws Exception {
        Path root = Files.createTempDirectory("binstore-restart");
        LocalFsBinStore store = new LocalFsBinStore(root);

        createIndex("logs", Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        ensureGreen("logs");

        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndexUUID();
        UUID stream = BinStoreConsumerFactory.indexUuidOf(indexUuid);

        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                store, "bins/cluster-a", "pod1", TestSequencers.leased(store, "bins/cluster-a", "pod1"), HUB, Clock.systemUTC(),
                index -> stream)) {

            // ---- batch 1: 20 documents, then wait for them to be searchable
            ingest.append(PRINCIPAL, "logs", 0, docs(0, 20)::forEach);
            assertBusy(() -> {
                client().admin().indices().prepareRefresh("logs").get();
                assertEquals(20L, client().prepareSearch("logs")
                        .setQuery(matchAllQuery()).setSize(0).get()
                        .getHits().getTotalHits().value());
            }, 60, TimeUnit.SECONDS);

            // ⚠️ FORCE the commit that writes batch_start into Lucene commit
            // data. Without an explicit flush, whether one has happened yet is
            // a race against the engine's own commit schedule.
            client().admin().indices().prepareFlush("logs").get();

            // ⚠️ READ THE ACTUAL LUCENE COMMIT DATA. Not inferred, not assumed
            // -- this is what proves the round trip happened for real. A
            // missing key here means the whole rest of the test is exercising
            // nothing.
            String batchStart = readBatchStart();
            assertNotNull("batch_start must be present in real commit data after a flush",
                    batchStart);
            // ⚠️ Parsed by the REAL production factory, not a stand-in --
            // proves the format OpenSearch persisted is the format our own
            // code can read back.
            BinStoreOffset recovered = BinStoreOffset.fromString(batchStart);
            assertTrue("the persisted pointer is never negative", recovered.offset() >= 0);

            // ---- CLOSE and REOPEN: discards the in-memory engine, then goes
            // through the same shard-recovery path a node restart uses.
            client().admin().indices().prepareClose("logs").get();
            client().admin().indices().prepareOpen("logs").get();
            ensureGreen("logs");

            // ⚠️ LOSES NOTHING: batch 1 is still there after the reopen.
            assertBusy(() -> {
                client().admin().indices().prepareRefresh("logs").get();
                assertEquals(20L, client().prepareSearch("logs")
                        .setQuery(matchAllQuery()).setSize(0).get()
                        .getHits().getTotalHits().value());
            }, 60, TimeUnit.SECONDS);

            // ---- batch 2, through the SAME ingester: proves the poller
            // resumed and kept working after the reopen, rather than dying or
            // sticking on the persisted pointer.
            ingest.append(PRINCIPAL, "logs", 0, docs(20, 10)::forEach);
            assertBusy(() -> {
                client().admin().indices().prepareRefresh("logs").get();
                assertEquals(30L, client().prepareSearch("logs")
                        .setQuery(matchAllQuery()).setSize(0).get()
                        .getHits().getTotalHits().value());
            }, 60, TimeUnit.SECONDS);
        }
    }

    private String readBatchStart() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        var index = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndex();
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard shard = indexService.getShard(0);
        return shard.store().readLastCommittedSegmentsInfo().getUserData()
                .get(StreamPoller.BATCH_START);
    }

    private static List<SegmentRecord> docs(int from, int count) {
        List<SegmentRecord> out = new java.util.ArrayList<>();
        for (int i = from; i < from + count; i++) {
            out.add(new SegmentRecord("doc-" + i, OpType.INDEX, OptionalLong.of(1),
                    ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }
}
