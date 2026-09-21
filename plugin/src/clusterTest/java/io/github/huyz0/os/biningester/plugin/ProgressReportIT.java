// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;

import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * A real node reports the position its shard COMMITTED (M8.43, M7.17, FR-9).
 *
 * <p>⚠️ THE POSITION IS {@code batch_start} FROM THE LAST LUCENE COMMIT, and
 * the case compares against exactly that, read from the shard's store. What
 * the consumer DELIVERED is ahead of it by the window retention exists to
 * protect, so a reporter reading the delivered position would report further
 * than the shard could resume from -- and GC would delete what a restart needs.
 */
public class ProgressReportIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final List<ConsumerProgress> REPORTED = new CopyOnWriteArrayList<>();

    static {
        // ⚠️ Before the node starts: it builds the plugin during setUp().
        BinStorePlugin.install(node -> new NodeSubscriptions(new RecordingTransport(), 64));
    }

    private static final class RecordingTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, SubscriptionHub.assembling(
                    push -> listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(),
                            push.segment(), push.grant(), push.sequencerEpoch()))));
        }

        @Override
        public void report(ConsumerProgress progress) {
            REPORTED.add(progress);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        // ⚠️ A SECOND, so the case waits on the schedule rather than on a sleep
        return Settings.builder().put(super.nodeSettings())
                .put(BinStorePlugin.PROGRESS_INTERVAL.getKey(), "1s").build();
    }

    public void testTheNodeReportsTheCOMMITTEDBatchStart() throws Exception {
        Path root = Files.createTempDirectory("binstore-progress");
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
                store, "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), HUB, Clock.systemUTC(),
                index -> stream)) {
            ingest.append(PRINCIPAL, "logs", 0, docs(20)::forEach);
            assertBusy(() -> {
                client().admin().indices().prepareRefresh("logs").get();
                assertEquals(20L, client().prepareSearch("logs")
                        .setQuery(matchAllQuery()).setSize(0).get()
                        .getHits().getTotalHits().value());
            }, 60, TimeUnit.SECONDS);
            client().admin().indices().prepareFlush("logs").get();
            long committed = BinStoreOffset.fromString(batchStart()).offset();

            assertBusy(() -> assertTrue("⚠️ THE NODE REPORTED THE COMMITTED POSITION, "
                    + committed + ", for logs/0: " + REPORTED,
                    REPORTED.stream().flatMap(p -> p.entries().stream()).anyMatch(e ->
                            e.indexUuid().equals(indexUuid) && e.partition() == 0
                                    && e.consumedUpTo() == committed
                                    && !e.shardCopy().isBlank())), 30, TimeUnit.SECONDS);
        }
    }

    private String batchStart() throws Exception {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        var index = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndex();
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard shard = indexService.getShard(0);
        return shard.store().readLastCommittedSegmentsInfo().getUserData()
                .get(StreamPoller.BATCH_START);
    }

    private static List<SegmentRecord> docs(int count) {
        List<SegmentRecord> out = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(new SegmentRecord("doc-" + i, OpType.INDEX, OptionalLong.of(1),
                    ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }
}
