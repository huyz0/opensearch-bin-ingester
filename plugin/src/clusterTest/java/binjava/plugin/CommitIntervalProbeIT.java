// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.binstore.backend.LocalFsBinStore;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.CommitDelta;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import binjava.ingest.Accumulator;
import binjava.ingest.IngestConfig;
import binjava.ingest.RetentionRule;
import binjava.ingest.SubscriptionHub;
import binjava.sequencer.CommitLog;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Measurements M5 and M6, on a real node (M7.14).
 *
 * <p>⚠️ IT REPORTS A NUMBER; IT IS NOT A GATE. A case asserting a particular
 * commit interval would pin OpenSearch's translog policy, which is not ours to
 * pin — it moves with flush thresholds, heap pressure and the version. What is
 * asserted is that the number EXISTS, that the committed pointer is observable
 * at all, and that {@link RetentionRule#DEFAULT_SAFETY_MARGIN} exceeds what was
 * observed.
 *
 * <p>⚠️ MEASUREMENT M6 IS THE ONE THAT CHANGES A DESIGN. ADR-0005 records that
 * OpenSearch reports {@code streamPoller.getBatchStartPointer()} — the
 * IN-MEMORY pointer — while only {@code lastCommittedBatchStartPointer}
 * survives a crash, and that the API exposes no way to read the latter. This
 * probe reads the COMMITTED one directly from the shard's last Lucene commit
 * user data, which is where {@code IngestionEngine} writes
 * {@code StreamPoller.BATCH_START} inside the same commit as the documents. If
 * that is reachable from inside the node, the plugin can report the committed
 * position rather than the optimistic one — and {@code safetyMargin} exists
 * only to cover the difference.
 *
 * <p>⚠️ {@code safetyMargin} IS IN OFFSETS WHILE THE CORPUS SIZES IT IN TIME.
 * Research 09 §6.1 says to size it above the observed Lucene commit interval;
 * the retention rule adds it to an offset. The conversion is the number of
 * records a stream can commit within that interval, so this probe measures
 * BOTH: how long the pointer takes to advance, and how many records it advanced
 * by.
 */
public class CommitIntervalProbeIT extends OpenSearchSingleNodeTestCase {

    private static final SubscriptionHub HUB = new SubscriptionHub();

    static {
        BinStorePlugin.install(node -> new NodeSubscriptions(new HubTransport(), 1024));
    }

    private static final class HubTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return HUB.subscribe(key, SubscriptionHub.assembling(push ->
                    listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(), push.segment(),
                            push.grant(), push.sequencerEpoch()))));
        }
    }

    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override
        public long millis() {
            return millis;
        }

        void advance(Duration d) {
            millis += d.toMillis();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId z) {
            return this;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    /** How many records this probe indexes before forcing the second commit. */
    private static final long INDEXED = 60;

    public void testTheCommittedPointerIsObservableAndItsIntervalIsReported() throws Exception {
        Path root = Files.createTempDirectory("binstore-commit-interval");
        LocalFsBinStore store = new LocalFsBinStore(root);
        CommitLog log = new CommitLog(store, "bins/cluster-a", 0);
        TestClock clock = new TestClock();
        Accumulator accumulator = new Accumulator(
                new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"), clock);

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
        RunKey stream = new RunKey(BinStoreConsumerFactory.indexUuidOf(indexUuid), 0);
        assertBusy(() -> assertEquals("the engine must have created a shard consumer", 1,
                HUB.subscriberCount(stream)), 30, TimeUnit.SECONDS);

        IndicesService indices = getInstanceFromNode(IndicesService.class);
        IndexService index = indices.indexService(
                client().admin().cluster().prepareState().get().getState()
                        .metadata().index("logs").getIndex());
        IndexShard shard = index.getShard(0);

        Map<String, String> before = commitUserData(shard);
        // ⚠️ THE POINTER AT SHARD CREATION IS ALREADY THERE, and review
        // MEASURED it: an empty commit is written when the shard starts,
        // carrying `batch_start=0` beside `max_seq_no=-1`. Asserting the KEY
        // EXISTS therefore proves nothing about ingestion -- what has to be
        // shown is that the value MOVES.
        Optional<String> atCreation = pointerIn(before);
        List<Long> naturalWaitsMillis = new ArrayList<>();

        for (int flush = 0; flush < 3; flush++) {
            for (int i = 0; i < 20; i++) {
                accumulator.add(stream, new SegmentRecord("doc-" + flush + "-" + i,
                        OpType.INDEX, OptionalLong.of(1),
                        ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
            }
            clock.advance(Duration.ofMillis(250));
            publish(accumulator, store, log, flush);

            // ⚠️ HOW LONG THE POINTER TAKES TO MOVE ON ITS OWN, up to a bound.
            // A commit is driven by the TRANSLOG FLUSH POLICY -- 512 MB or 30
            // minutes by default -- not by ingestion, so on a small workload
            // nothing commits within any test's patience, which is itself the
            // measurement.
            long waitedFrom = System.nanoTime();
            long deadline = waitedFrom + TimeUnit.SECONDS.toNanos(10);
            boolean movedOnItsOwn = false;
            while (System.nanoTime() < deadline) {
                if (!commitUserData(shard).equals(before)) {
                    movedOnItsOwn = true;
                    break;
                }
                Thread.sleep(100);
            }
            naturalWaitsMillis.add(
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - waitedFrom));
            if (movedOnItsOwn) {
                before = commitUserData(shard);
            }
        }

        // MEASUREMENT M6: is the COMMITTED pointer observable at all? Force the
        // commit OpenSearch would otherwise make on its own schedule, and read
        // the commit's user data.
        assertBusy(() -> assertEquals("every record must reach the engine before the flush",
                60L, client().prepareSearch("logs").setSize(0).get().getHits()
                        .getTotalHits().value()), 60, TimeUnit.SECONDS);
        client().admin().indices().prepareFlush("logs").get();
        Map<String, String> after = commitUserData(shard);

        // ⚠️ PRINTED, NOT ONLY LOGGED. The measurement IS the deliverable, and
        // the test framework's logger does not reach the JUnit XML -- a number
        // nobody can read afterwards is a number that was not reported.
        System.out.println("MEASUREMENT M5/M6: commitUserDataKeys=" + after.keySet()
                + " batchStart=" + pointerIn(after) + " naturalWaitsMillis="
                + naturalWaitsMillis + " recordsIndexed=" + INDEXED
                + " batchStartAtCreation=" + atCreation);

        assertTrue("MEASUREMENT M6: the committed pointer must be observable AND must "
                        + "MOVE. The key alone proves nothing -- an empty commit written at "
                        + "shard creation already carries batch_start beside max_seq_no=-1, "
                        + "which review measured -- so what is asserted is that the value "
                        + "after " + INDEXED + " indexed records DIFFERS from the one at "
                        + "creation (" + atCreation + " -> " + pointerIn(after) + "). Keys "
                        + "seen: " + after.keySet(),
                pointerIn(after).isPresent() && !pointerIn(after).equals(atCreation));

        long advancedBy = Long.parseLong(pointerIn(after).orElseThrow().trim())
                - Long.parseLong(atCreation.orElse("0").trim());
        assertTrue("MEASUREMENT M5: the default safety margin ("
                        + RetentionRule.DEFAULT_SAFETY_MARGIN + " records) must exceed what "
                        + "the pointer advanced by between the ONLY two commits this probe "
                        + "saw (" + advancedBy + " offsets). ⚠️ AND THAT IS A FLOOR, NOT A "
                        + "PRODUCTION FIGURE: the pointer did not advance on its own within "
                        + naturalWaitsMillis + " ms, because a commit follows the translog "
                        + "flush policy rather than ingestion -- so the second commit had "
                        + "to be FORCED, and what a real deployment's flush window holds is "
                        + "a different and larger number",
                RetentionRule.DEFAULT_SAFETY_MARGIN > advancedBy);
    }

    /**
     * The pointer in the shard's LAST LUCENE COMMIT — the one that survives a
     * crash, which the ingestion-state API does not expose.
     */
    private static Map<String, String> commitUserData(IndexShard shard) throws Exception {
        try (org.opensearch.common.concurrent.GatedCloseable<org.apache.lucene.index.IndexCommit>
                held = shard.acquireLastIndexCommit(false)) {
            return Map.copyOf(held.get().getUserData());
        }
    }

    /**
     * The ingestion pointer inside a commit's user data, if it is there at all.
     *
     * <p>⚠️ MATCHED ON THE KEY'S SHAPE RATHER THAN ON A LITERAL, because the
     * literal is {@code StreamPoller.BATCH_START}'s value and this probe's
     * whole question is whether it is present — asserting the literal would
     * turn "not observable" into "test typo" and back again.
     */
    private static Optional<String> pointerIn(Map<String, String> userData) {
        return userData.entrySet().stream()
                .filter(e -> e.getKey().toLowerCase(java.util.Locale.ROOT).contains("batch"))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    private static void publish(Accumulator accumulator, LocalFsBinStore store, CommitLog log,
            long sequence) throws Exception {
        byte[] segment = accumulator.drain().orElseThrow();
        SegmentReader reader = SegmentReader.open(segment);
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        for (var entry : reader.directory()) {
            counts.put(entry.key(), entry.recordCount());
        }
        int headerLen = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN).getInt(8);
        String key = new SegmentKey("bins/cluster-a", reader.createdAtMillis(), "pod1",
                sequence, headerLen).key();
        store.put(key, new binjava.binstore.Body(segment.length,
                () -> new java.io.ByteArrayInputStream(segment)));
        CommitDelta delta = log.commit(key, counts);
        HUB.publish(delta, key, segment, new binjava.ingest.SegmentServing(
                new binjava.ingest.FetchPolicy(new binjava.ingest.FetchPolicyConfig(
                        Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                store.capabilities(), new binjava.ingest.SegmentProxy(store)));
    }
}
