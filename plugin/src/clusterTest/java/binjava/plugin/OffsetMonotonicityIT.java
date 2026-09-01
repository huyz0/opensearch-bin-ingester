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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.Bits;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * T11c (M1.15d) — criterion 2's SECOND HALF: {@code _offset} is present on
 * every document and monotonic across at least two flushes.
 *
 * <p>⚠️ NOT reachable through {@code _search}. `MessageProcessorRunnable` writes
 * {@code _offset} as a Lucene {@code LongPoint} and a {@code StoredField} with
 * NO OpenSearch field mapping for either -- a sort on it fails the query
 * outright, `_source` does not carry it, and `storedFields()` on a hit returns
 * null. This test reads the stored field directly off the shard's own Lucene
 * reader, the way {@code RestartResumeIT} reads `batch_start` directly off the
 * shard's commit data rather than through the SPI.
 *
 * <p>⚠️ MUST span at least two flushes. `ConsumerRecord.offset()`'s own javadoc
 * names the exact defect this guards: deriving `_offset` from a record's
 * position in the SEGMENT (our own uploaded object) rather than from the value
 * the COMMIT LOG assigned it (ADR-0001) coincide for a single flush and diverge
 * on the second -- so a test confined to one flush cannot fail against that
 * regression no matter how carefully it asserts. Two batches of two documents
 * each, sent through separate `append` calls with the real system clock (the
 * same pattern `DeleteAndVersionIT` uses), gives the 250 ms flush interval time
 * to fire twice between them.
 */
public class OffsetMonotonicityIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    static {
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

    public void testOffsetIsPresentAndMonotonicAcrossTwoFlushes() throws Exception {
        Path root = Files.createTempDirectory("binstore-offset");
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
                store, "bins/cluster-a", "pod1", HUB, Clock.systemUTC(), index -> stream)) {

            // ---- flush 1: two documents
            ingest.append(PRINCIPAL, "logs", 0, docs("a", 2)::forEach);
            assertBusy(() -> assertEquals(2L, totalHits()), 60, TimeUnit.SECONDS);

            // ---- flush 2, through a SEPARATE append -- the real system clock
            // has had well over 250 ms to elapse since flush 1's assertBusy
            // returned, so this lands in a distinct flush rather than being
            // folded into the first one.
            ingest.append(PRINCIPAL, "logs", 0, docs("b", 2)::forEach);
            assertBusy(() -> assertEquals(4L, totalHits()), 60, TimeUnit.SECONDS);

            List<Long> offsets = readOffsetsInDocOrder();
            assertEquals("every one of the 4 documents carries _offset",
                    4, offsets.size());
            for (int i = 1; i < offsets.size(); i++) {
                assertTrue("_offset is monotonic across the flush boundary: "
                        + offsets, offsets.get(i) > offsets.get(i - 1));
            }
        }
    }

    private long totalHits() {
        client().admin().indices().prepareRefresh("logs").get();
        return client().prepareSearch("logs")
                .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    /**
     * Reads {@code _offset}'s stored-field value off every live document,
     * straight from the shard's own Lucene reader, in reader order -- which is
     * insertion order for a single-writer shard that has taken no deletes.
     */
    private List<Long> readOffsetsInDocOrder() throws Exception {
        client().admin().indices().prepareRefresh("logs").get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        var index = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndex();
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard shard = indexService.getShard(0);

        List<Long> offsets = new ArrayList<>();
        try (Engine.Searcher searcher = shard.acquireSearcher("offset-monotonicity-test")) {
            IndexReader reader = searcher.getIndexReader();
            StoredFields storedFields = reader.storedFields();
            for (LeafReaderContext ctx : reader.leaves()) {
                Bits liveDocs = ctx.reader().getLiveDocs();
                int maxDoc = ctx.reader().maxDoc();
                for (int localDoc = 0; localDoc < maxDoc; localDoc++) {
                    if (liveDocs != null && !liveDocs.get(localDoc)) {
                        continue;
                    }
                    Document doc = storedFields.document(ctx.docBase + localDoc);
                    String value = doc.get("_offset");
                    assertNotNull("every document carries an _offset", value);
                    offsets.add(Long.parseLong(value.trim()));
                }
            }
        }
        return offsets;
    }

    private static List<SegmentRecord> docs(String prefix, int count) {
        List<SegmentRecord> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(new SegmentRecord(prefix + "-" + i, OpType.INDEX, OptionalLong.of(1),
                    ("{\"n\":\"" + prefix + i + "\"}").getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }
}
