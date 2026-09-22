// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.Accumulator;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.sequencer.CommitLog;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Criterion 2's first half: a document written as {@code _bulk} is searchable in
 * a single-node OpenSearch cluster, having travelled producer → ingester →
 * local-FS segment → consumer → plugin → the ingestion engine.
 *
 * <p>Two node-imposed requirements that cost a session each, recorded so they
 * are not rediscovered: pull-based ingestion REQUIRES
 * {@code index.replication.type: SEGMENT} ("Replication type DOCUMENT is not
 * supported"), and an OpenSearch index UUID is <b>base64url</b>, so
 * {@code UUID.fromString} throws on every real index.
 *
 * <p>⚠️ When this fails, the engine's own explanation is NOT on the console. It
 * is captured into the JUnit XML {@code <system-out>} under
 * {@code plugin/build/test-results/clusterTest/}. Both defects that made this
 * test report "0 documents" were invisible until that file was read.
 *
 * <p>⚠️ {@code _offset} is deliberately not asserted here — that is T11c
 * (M1.15d), which needs documents spanning two flushes.
 */
public class SearchableIT extends OpenSearchSingleNodeTestCase {

    private static final int SEARCHABLE_RECORDS = 1_000;
    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final UUID STREAM_INDEX = UUID.randomUUID();

    static {
        // ⚠️ Installed before the node starts: the node builds the plugin during
        // setUp(), so a @Before would be too late.
        BinStorePlugin.install(node -> new NodeSubscriptions(new HubTransport(), 1024));
    }

    /** Bridges the ingester's hub to the consumer's transport seam. */
    private static final java.util.concurrent.atomic.AtomicInteger DELIVERED =
            new java.util.concurrent.atomic.AtomicInteger();

    private static final class HubTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, SubscriptionHub.assembling(push -> {
                DELIVERED.addAndGet(push.recordCount());
                listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                        push.recordCount(), push.firstOffset(), push.via(), push.segment(),
                        push.grant(), push.sequencerEpoch()));
            }));
        }
    }

    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override public long millis() { return millis; }
        void advance(Duration d) { millis += d.toMillis(); }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis); }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    public void testDocumentsAreSearchableAfterIngest() throws Exception {
        Path root = Files.createTempDirectory("binstore-e2e");
        CountingBinStore store = new CountingBinStore(new LocalFsBinStore(root));
        CommitLog log = new CommitLog(store, "bins/cluster-a", 0);
        TestClock clock = new TestClock();
        Accumulator accumulator = new Accumulator(
                new IngestConfig(Duration.ofSeconds(1), 8L << 20, "cluster-a"), clock);

        // ---- the index opts in to this ingestion source
        createIndex("logs", Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                // ⚠️ SEGMENT replication is REQUIRED for pull-based ingestion:
                // "Replication type DOCUMENT is not supported in pull-based
                // ingestion when index.ingestion_source.all_active is not
                // enabled". Nothing in our own docs said so -- only a real node
                // refusing the index does.
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                // ⚠️ BLOCK, not the default: a DROP strategy swallows every
                // indexing failure and the symptom is "0 documents" with no
                // reason anywhere.
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        ensureGreen("logs");

        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndexUUID();
        // ⚠️ Through the SAME conversion the factory uses, so the test and the
        // plugin agree on what stream an index maps to. Using UUID.fromString
        // here is what exposed that the factory could not.
        RunKey stream = new RunKey(BinStoreConsumerFactory.indexUuidOf(indexUuid), 0);

        // ---- producer: 1,000 documents, exactly as a _bulk request delivers them
        for (int i = 0; i < SEARCHABLE_RECORDS; i++) {
            accumulator.add(stream, new SegmentRecord("doc-" + i, OpType.INDEX,
                    OptionalLong.of(1),
                    ("{\"n\":" + i + ",\"kind\":\"probe\"}").getBytes(StandardCharsets.UTF_8)));
        }
        clock.advance(Duration.ofSeconds(1));

        // ⚠️ Diagnose the seam before asserting the outcome: if the engine never
        // created a shard consumer, no push can reach it and "0 documents" would
        // be blamed on indexing rather than on subscription.
        assertBusy(() -> assertEquals(
                "the engine must have created a shard consumer for " + stream,
                1, HUB.subscriberCount(stream)), 30, TimeUnit.SECONDS);

        long before = store.counts().total();
        // The T3 harness owns the exact HTTP 202 endpoint. This T4 seam has no
        // front door, so stamp before the durable handoff as a conservative
        // searchable-tail observation rather than pretending publish() is HTTP.
        long searchableStart = System.nanoTime();
        publish(accumulator, store, log);
        // ⚠️ TWO requests for 1,000 documents: the segment and its commit delta.
        assertEquals("1,000 documents cost two requests", 2, store.counts().total() - before);

        // ⚠️ Isolate DELIVERY from INDEXING. If the push never reached the
        // engine's consumer, "0 documents" is a subscription problem; if it did,
        // it is an indexing one. Guessing between the two is how a whole
        // afternoon disappears.
        assertBusy(() -> assertEquals("the push must reach the engine's consumer",
                SEARCHABLE_RECORDS, DELIVERED.get()), 30, TimeUnit.SECONDS);

        // ---- the engine polls our consumer, indexes, and Lucene makes it searchable
        Histogram searchableLatency = new Histogram(TimeUnit.SECONDS.toNanos(60), 3);
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("logs").get();
            SearchResponse response = client().prepareSearch("logs")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
            assertEquals("all 1,000 documents searchable",
                    (long) SEARCHABLE_RECORDS, response.getHits().getTotalHits().value());
            if (searchableLatency.getTotalCount() == 0) {
                searchableLatency.recordValue(Math.max(1, System.nanoTime() - searchableStart));
            }
        }, 60, TimeUnit.SECONDS);
        assertTrue("M9.11 searchable p99 at a 1 s ceiling",
                searchableLatency.getValueAtPercentile(99.0) < TimeUnit.SECONDS.toNanos(3));
        System.out.println("M9.11 searchable latency: records=" + SEARCHABLE_RECORDS
                + ", p99=" + searchableLatency.getValueAtPercentile(99.0)
                + " ns (clusterTest)");

        // ⚠️ _offset is deliberately NOT asserted here. It is T11c's row, which
        // requires documents spanning at least TWO flushes -- within one flush
        // the segment position and the sequencer's assignment coincide, so the
        // mutation T11c names survives. Folding it in here would report that row
        // as covered by a test that cannot fail for it.
        //
        // It is also not reachable through the search API at all:
        // MessageProcessorRunnable writes _offset as a LongPoint and a
        // StoredField with no mapping for either, so a sort fails the query
        // outright, _source does not carry it, and storedFields() returns null.
        // T11c must assert it through the engine, not through _search.

        // ---- and an idle cluster spends nothing
        long afterSearch = store.counts().total();
        Thread.sleep(2_000);
        assertEquals("an idle cluster issues zero object-store requests",
                afterSearch, store.counts().total());
    }

    private static void publish(Accumulator accumulator, CountingBinStore store, CommitLog log)
            throws Exception {
        byte[] segment = accumulator.drain().orElseThrow();
        SegmentReader reader = SegmentReader.open(segment);
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        for (var entry : reader.directory()) {
            counts.put(entry.key(), entry.recordCount());
        }
        int headerLen = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN).getInt(8);
        String key = new SegmentKey("bins/cluster-a", reader.createdAtMillis(), "pod1", 0,
                headerLen).key();
        store.put(key, new io.github.huyz0.os.biningester.binstore.Body(segment.length,
                () -> new java.io.ByteArrayInputStream(segment)));
        CommitDelta delta = log.commit(key, counts);
        // ⚠️ THE FOUR-ARGUMENT `publish`, because M5.47 removed the
        // two-argument one. An inline-forever policy with the bytes in hand
        // keeps this test's subject where it was and reads the store zero
        // times, exactly as the two-argument method did.
        HUB.publish(delta, key, segment, new io.github.huyz0.os.biningester.ingest.SegmentServing(
                        new io.github.huyz0.os.biningester.ingest.FetchPolicy(
                                new io.github.huyz0.os.biningester.ingest.FetchPolicyConfig(
                                        Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                        store.capabilities(), new io.github.huyz0.os.biningester.ingest.SegmentProxy(store)));
    }
}
