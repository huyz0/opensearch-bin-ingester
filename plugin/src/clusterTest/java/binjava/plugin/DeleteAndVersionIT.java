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
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Criterion 0, against a real node: a delete removes the document, and a
 * replayed stale version is rejected rather than resurrecting it.
 *
 * <p>⚠️ Neither half is OUR code's job to implement — it is OpenSearch's own
 * external-versioning machinery, triggered simply by {@code DefaultEnvelope}
 * carrying {@code _version} faithfully. Verified directly against OpenSearch's
 * source before writing this: {@code MessageProcessorRunnable} catches
 * {@code VersionConflictEngineException} and DROPS the message (debug-logged),
 * which does NOT trip {@code error_strategy: BLOCK} — a stale replay is an
 * expected outcome to the engine, not a fatal one. What this test proves is
 * that OUR envelope and OUR path deliver a {@code _version} the engine
 * actually honours end to end, which no T0-T2 test can reach because it is
 * about Lucene/engine state, not about our own framing.
 *
 * <p>⚠️ Until this test existed, criterion 0 had ZERO coverage anywhere in the
 * tree: the SPEC named {@code deleteRemovesTheDocumentFromTheIndex} and
 * {@code staleVersionIsRejectedOnReplay} as T4 rows, and neither had ever been
 * written. Found by checking the SPEC's own test table against the tree
 * rather than assuming a milestone with green gates has no gaps left.
 */
public class DeleteAndVersionIT extends OpenSearchSingleNodeTestCase {

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

    private DefaultIngest ingest;
    private UUID stream;

    private void createLogsIndex() throws Exception {
        Path root = Files.createTempDirectory("binstore-delver");
        LocalFsBinStore store = new LocalFsBinStore(root);

        createIndex("logs", Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                .put("index.ingestion_source.mapper_type", "default")
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                // ⚠️ BLOCK stays on: proving a version conflict does NOT trip
                // it is part of the point. A DROP strategy would make this test
                // pass whether or not the conflict path works at all.
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build());
        ensureGreen("logs");

        String indexUuid = client().admin().cluster().prepareState().get().getState()
                .metadata().index("logs").getIndexUUID();
        stream = BinStoreConsumerFactory.indexUuidOf(indexUuid);
        ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                store, "bins/cluster-a", "pod1", HUB, Clock.systemUTC(), index -> stream);
    }

    private void append(String id, OpType op, long version, String body) throws Exception {
        ingest.append(PRINCIPAL, "logs", 0, List.of(new SegmentRecord(id, op,
                OptionalLong.of(version), body.getBytes(StandardCharsets.UTF_8))));
    }

    private long totalHits() {
        client().admin().indices().prepareRefresh("logs").get();
        return client().prepareSearch("logs").setQuery(matchAllQuery())
                .setSize(0).get().getHits().getTotalHits().value();
    }

    private Object sourceField(String id, String field) {
        client().admin().indices().prepareRefresh("logs").get();
        var hits = client().prepareSearch("logs").setQuery(matchAllQuery())
                .setSize(10).get().getHits().getHits();
        for (var hit : hits) {
            if (id.equals(hit.getId())) {
                return hit.getSourceAsMap().get(field);
            }
        }
        return null;
    }

    public void testDeleteRemovesTheDocumentFromTheIndex() throws Exception {
        createLogsIndex();
        try {
            append("d1", OpType.INDEX, 1, "{\"n\":1}");
            assertBusy(() -> assertEquals(1L, totalHits()), 60, TimeUnit.SECONDS);

            append("d1", OpType.DELETE, 2, "");
            assertBusy(() -> assertEquals(
                    "the delete removed the document, not merely acked it",
                    0L, totalHits()), 60, TimeUnit.SECONDS);
        } finally {
            ingest.close();
        }
    }

    public void testStaleVersionIsRejectedOnReplayRatherThanResurrectingTheDocument()
            throws Exception {
        createLogsIndex();
        try {
            // ---- index, then delete at a HIGHER version
            append("d2", OpType.INDEX, 5, "{\"n\":5}");
            assertBusy(() -> assertEquals(1L, totalHits()), 60, TimeUnit.SECONDS);
            append("d2", OpType.DELETE, 6, "");
            assertBusy(() -> assertEquals(0L, totalHits()), 60, TimeUnit.SECONDS);

            // ⚠️ THE assertion. A replay of the ORIGINAL index action -- version
            // 5, stale against the delete's version 6 -- must be REJECTED by
            // OpenSearch's own external-version check, not silently re-applied.
            // A poller that dropped the version and re-indexed unconditionally
            // would resurrect "d2" here.
            append("d2", OpType.INDEX, 5, "{\"n\":5}");
            // ⚠️ A settle window, not a race: this asserts an ABSENCE, so there
            // is no positive event to await. The prior two assertBusy calls
            // already proved the engine is live and processing this stream
            // promptly, so a settle window here is checking the negative
            // against a system already shown to be responsive.
            Thread.sleep(3_000);
            assertEquals("a stale replay must not resurrect the deleted document",
                    0L, totalHits());
        } finally {
            ingest.close();
        }
    }

    public void testAnOlderReplayDoesNotOverwriteANewerLiveWrite() throws Exception {
        // ⚠️ Criterion -1. "Lane -1" is descriptive, not a wire mechanism: lanes
        // are explicitly OUT of M1's scope (ADR-0014; the wire format's `u8
        // lane` field does not exist until M3). What the criterion actually
        // demands is version safety independent of arrival order, which is the
        // SAME external-versioning guarantee criterion 0 proved -- exercised
        // here as two INDEX writes rather than an index-then-delete, forcing
        // the LOSING interleaving explicitly: the older (replay) write is
        // appended strictly AFTER the newer (live) one is already searchable,
        // so a last-writer-wins bug (applying whatever arrives last,
        // regardless of its version) cannot hide behind a lucky ordering.
        //
        // ⚠️ Also previously undiscovered: T5d named
        // `backfillWithOlderVersionDoesNotOverwriteLive` and it was never
        // written, so this criterion had zero coverage until now, the same gap
        // as criterion 0.
        createLogsIndex();
        try {
            // ---- the LIVE write, at the newer version
            append("d3", OpType.INDEX, 10, "{\"n\":10}");
            assertBusy(() -> {
                assertEquals(1L, totalHits());
                assertEquals(10, ((Number) sourceField("d3", "n")).intValue());
            }, 60, TimeUnit.SECONDS);

            // ---- the REPLAY, older, forced to arrive strictly after
            append("d3", OpType.INDEX, 3, "{\"n\":3}");
            Thread.sleep(3_000);

            // ⚠️ THE assertion: the live write's value survives. A poller that
            // applied writes in arrival order rather than by version would show
            // n=3 here.
            assertEquals("an older replay must not overwrite the newer live write",
                    10, ((Number) sourceField("d3", "n")).intValue());
            assertEquals(1L, totalHits());
        } finally {
            ingest.close();
        }
    }
}
