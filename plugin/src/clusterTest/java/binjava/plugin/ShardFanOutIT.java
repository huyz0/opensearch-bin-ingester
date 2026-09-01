// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.binstore.Body;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import binjava.ingest.CommitLog;
import binjava.ingest.SubscriptionHub;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Twenty real shards each build a consumer, poll it, decode and index — the
 * fan-out half of the walking skeleton at T4.
 *
 * <p>⚠️ THIS IS NOT CRITERION 3, and an earlier version of this file said it
 * was. Criterion 3 is about idle object-store COST, and that zero is not
 * falsifiable from here: no class on the consumer path holds a {@code BinStore}
 * at all — neither {@code plugin/src/main} nor {@code client/src/main} imports
 * {@code binjava.binstore}, because inline delivery carries the bytes. A store
 * wired to this test can only ever observe the ingester the test itself
 * constructed, so {@code SHARDS} could be 1 or 1,000 without changing what the
 * counter can see. Asserting a zero here would report SPEC row T8b as covered
 * by a test that cannot fail for it.
 *
 * <p>⚠️ An earlier version also claimed "the 1,600-consumer proof is
 * IdleCostTest at T1". No such class exists, or ever did: it was written,
 * measured flaky (3 passes in 8 runs) and deleted. Criterion 3's consumer half
 * is unproven and is M1.16c.
 *
 * <p>What this DOES prove, and what the earlier "still subscribed" assertion did
 * not: every one of the twenty shards polled, decoded and indexed. A review
 * showed subscriptions outlive dead pollers, so an early return in
 * {@code BinStoreShardConsumer.drain} left the subscription assertion green.
 * Requiring the documents to become SEARCHABLE cannot be satisfied that way.
 */
public class ShardFanOutIT extends OpenSearchSingleNodeTestCase {

    private static final int SHARDS = 20;
    private static final SubscriptionHub HUB = new SubscriptionHub();

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

    public void testEveryShardPollsDecodesAndIndexes() throws Exception {
        Path root = Files.createTempDirectory("binstore-fanout");
        // ⚠️ NOT a CountingBinStore: nothing here reads counts(), and wrapping it
        // would imply a cost assertion this test deliberately does not make.
        LocalFsBinStore store = new LocalFsBinStore(root);

        createIndex("logs", Settings.builder()
                .put("index.number_of_shards", SHARDS)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                // ⚠️ A SHORT poll timeout on purpose: the shards must poll many
                // times inside the window, or a zero proves only that nothing
                // was scheduled yet.
                .put("index.ingestion_source.poll.timeout", 50)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        ensureGreen("logs");

        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndexUUID();
        UUID stream = BinStoreConsumerFactory.indexUuidOf(indexUuid);

        // ⚠️ THE POSITIVE SIGNAL, before the zero means anything: every shard has
        // built a consumer and subscribed. Without this the assertion below is
        // also satisfied by a node whose pollers never started.
        assertBusy(() -> {
            int subscribed = 0;
            for (int p = 0; p < SHARDS; p++) {
                subscribed += HUB.subscriberCount(new RunKey(stream, p));
            }
            assertEquals("every shard subscribed", SHARDS, subscribed);
        }, 60, TimeUnit.SECONDS);

        // ⚠️ A settling window, so the push below lands on shards that are
        // established rather than still starting. testing.md rule 15 discourages
        // sleeps: this one waits for OpenSearch's own pollers to reach steady
        // state, and there is no seam to advance for those.
        //
        // ⚠️ An earlier version justified this sleep as "you cannot await an
        // absence" -- that was true of the cost assertion it preceded, and that
        // assertion is gone (see the class javadoc). A justification describing
        // deleted code is not a justification.
        Thread.sleep(3_000);

        // ⚠️ THE POSITIVE SIGNAL, and it has to be INDEXING rather than
        // subscription. An earlier version asserted the shards were "still
        // subscribed" after the window and claimed that showed the pollers had
        // not died. It does not: the subscription is owned by NodeSubscriptions
        // and outlives a dead or parked poller entirely. A review proved it —
        // an early `return` at the top of BinStoreShardConsumer.drain, so no
        // poller ever calls readNext, left that version BUILD SUCCESSFUL. That
        // is DT12 one tier up: the zero of twenty pollers that quietly died.
        //
        // Requiring the documents to become SEARCHABLE cannot be satisfied
        // without every poller having polled, decoded and indexed.
        publishOnePerShard(store, stream);
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("logs").get();
            assertEquals("every shard polled, decoded and indexed its record", SHARDS,
                    client().prepareSearch("logs").setQuery(QueryBuilders.matchAllQuery())
                            .setSize(0).get().getHits().getTotalHits().value());
        }, 60, TimeUnit.SECONDS);
    }

    /** One record per shard, in a single bundled segment plus one commit delta. */
    private static void publishOnePerShard(LocalFsBinStore store, UUID stream) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (int p = 0; p < SHARDS; p++) {
            writer.add(new RunKey(stream, p), new SegmentRecord("probe-" + p, OpType.INDEX,
                    OptionalLong.of(1), "{\"kind\":\"probe\"}".getBytes(StandardCharsets.UTF_8)),
                    1L);
        }
        byte[] segment = writer.toByteArray(1L);
        SegmentReader reader = SegmentReader.open(segment);
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        for (var entry : reader.directory()) {
            counts.put(entry.key(), entry.recordCount());
        }
        int headerLen = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN).getInt(8);
        String key = new SegmentKey("bins/cluster-a", reader.createdAtMillis(), "pod1", 0,
                headerLen).key();
        store.put(key, new Body(segment.length, () -> new ByteArrayInputStream(segment)));
        HUB.publish(new CommitLog(store, "bins/cluster-a").commit(key, counts), segment);
    }
}
