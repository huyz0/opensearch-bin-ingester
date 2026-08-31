// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.CommitDelta;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import binjava.ingest.Accumulator;
import binjava.ingest.CommitLog;
import binjava.ingest.IngestConfig;
import binjava.ingest.SegmentPublisher;
import binjava.ingest.SubscriptionHub;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.index.IngestionShardConsumer.ReadResult;

/**
 * The walking skeleton, end to end, in one process.
 *
 * <p>producer -&gt; accumulator -&gt; segment -&gt; LocalFsBinStore -&gt; commit log
 * -&gt; subscription -&gt; consumer -&gt; plugin shard consumer.
 *
 * <p>WARNING: this is NOT the milestone's acceptance test. Criterion 2 requires
 * the documents to be SEARCHABLE in a real single-node OpenSearch cluster, which
 * needs the OpenSearch test framework and a running node -- that is the T4 tier
 * and it is not this. What this proves is that every seam between the two ends
 * fits: the bytes a producer hands in come back out of the OpenSearch SPI with
 * the right offsets and the right envelope, through a real filesystem.
 */
class EndToEndTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override public long millis() { return millis; }
        void advance(Duration d) { millis += d.toMillis(); }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis); }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
    }

    /** Bridges the ingester's hub to the consumer's transport seam. */
    private record HubTransport(SubscriptionHub hub) implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return hub.subscribe(key, push -> listener.onDelivery(new Delivery(
                    push.key(), push.segmentKey(), push.recordCount(), push.firstOffset(),
                    push.segment())));
        }
    }

    @Test
    void aDocumentTravelsFromProducerToThePluginWithItsOffsetAndEnvelope(@TempDir Path dir)
            throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(new LocalFsBinStore(dir.resolve("bucket")));
        SubscriptionHub hub = new SubscriptionHub();
        CommitLog log = new CommitLog(store, "bins/cluster-a");
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        Accumulator accumulator =
                new Accumulator(new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a"),
                        clock);

        RunKey stream = new RunKey(INDEX, 3);
        try (NodeSubscriptions node = new NodeSubscriptions(new HubTransport(hub), 64)) {
            BinStorePlugin plugin = new BinStorePlugin(node);
            assertThat(plugin.getIngestionConsumerFactories()).containsKey("BINSTORE");

            try (BinStoreShardConsumer shardConsumer =
                    new BinStoreShardConsumer(3, node.clientFor(stream))) {

                // ---- producer: 100 documents, as a _bulk request would deliver them
                for (int i = 0; i < 100; i++) {
                    accumulator.add(stream, new SegmentRecord("doc-" + i, OpType.INDEX,
                            OptionalLong.of(i + 1),
                            ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
                }
                clock.advance(Duration.ofMillis(250));

                // ---- ingester: one segment, one PUT, one commit delta
                long before = store.counts().total();
                byte[] segment = drainAndPublish(accumulator, publisher, store, log, hub, stream);
                long spent = store.counts().total() - before;

                // ⚠️ 100 documents cost TWO requests: the segment and its delta.
                // Not 100, not one per stream -- the whole economic claim, on a
                // real filesystem rather than a fake.
                assertThat(spent).as("100 documents, 2 requests").isEqualTo(2);
                assertThat(segment).isNotEmpty();

                // ---- consumer -> plugin: the records come back out of the SPI
                List<ReadResult<BinStoreOffset, BinStoreMessage>> results = new ArrayList<>();
                while (results.size() < 100) {
                    var batch = shardConsumer.readNext(1_000, 500);
                    if (batch.isEmpty()) {
                        break;
                    }
                    results.addAll(batch);
                }

                assertThat(results).as("every document arrived").hasSize(100);
                for (int i = 0; i < 100; i++) {
                    // ⚠️ Offsets are the COMMIT LOG's, contiguous and monotonic.
                    assertThat(results.get(i).getPointer()).isEqualTo(new BinStoreOffset(i));
                    String json = new String(results.get(i).getMessage().getPayload(),
                            StandardCharsets.UTF_8);
                    // ⚠️ The DEFAULT envelope OpenSearch's mapper expects, with the
                    // producer's opaque body copied through byte for byte.
                    assertThat(json).isEqualTo("{\"_id\":\"doc-" + i + "\",\"_op_type\":\"index\","
                            + "\"_version\":" + (i + 1) + ",\"_source\":{\"n\":" + i + "}}");
                }

                // ⚠️ And now IDLE: nothing more is produced, so nothing is spent.
                long afterIngest = store.counts().total();
                for (int i = 0; i < 50; i++) {
                    assertThat(shardConsumer.readNext(10, 1)).isEmpty();
                    shardConsumer.getPointerBasedLag(new BinStoreOffset(0));
                }
                assertThat(store.counts().total() - afterIngest)
                        .as("an idle consumer, and a lag poll, cost nothing").isZero();
            }
        }
    }

    @Test
    void aDeleteSurvivesTheWholePathAsADelete(@TempDir Path dir) throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(new LocalFsBinStore(dir.resolve("b")));
        SubscriptionHub hub = new SubscriptionHub();
        CommitLog log = new CommitLog(store, "p");
        SegmentPublisher publisher = new SegmentPublisher(store, "p", "pod1");
        Accumulator accumulator =
                new Accumulator(new IngestConfig(Duration.ofMillis(250), 8L << 20, "c"), clock);
        RunKey stream = new RunKey(INDEX, 0);

        try (NodeSubscriptions node = new NodeSubscriptions(new HubTransport(hub), 16);
                BinStoreShardConsumer consumer =
                        new BinStoreShardConsumer(0, node.clientFor(stream))) {
            accumulator.add(stream, new SegmentRecord("gone", OpType.INDEX, OptionalLong.of(1),
                    "{\"a\":1}".getBytes(StandardCharsets.UTF_8)));
            accumulator.add(stream, new SegmentRecord("gone", OpType.DELETE, OptionalLong.of(2),
                    new byte[0]));
            clock.advance(Duration.ofMillis(250));
            drainAndPublish(accumulator, publisher, store, log, hub, stream);

            var results = consumer.readNext(10, 500);
            assertThat(results).hasSize(2);
            String deleteJson = new String(results.get(1).getMessage().getPayload(),
                    StandardCharsets.UTF_8);
            // ⚠️ criterion 0's middle clause: the delete must still BE a delete
            // after the round trip, with no _source. An index of {} here would
            // resurrect the document instead of removing it.
            assertThat(deleteJson)
                    .isEqualTo("{\"_id\":\"gone\",\"_op_type\":\"delete\",\"_version\":2}");
        }
    }

    private static byte[] drainAndPublish(Accumulator accumulator, SegmentPublisher publisher,
            CountingBinStore store, CommitLog log, SubscriptionHub hub, RunKey stream)
            throws Exception {
        // Count the records before draining, since drain resets the accumulator.
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        Optional<byte[]> drained = accumulator.drain();
        byte[] segment = drained.orElseThrow();
        SegmentReader reader = SegmentReader.open(segment);
        for (var entry : reader.directory()) {
            counts.put(entry.key(), entry.recordCount());
        }
        // Re-publish the already-drained bytes under a key, then commit.
        String key = new binjava.format.SegmentKey("bins/cluster-a",
                reader.createdAtMillis(), "pod1", 0,
                java.nio.ByteBuffer.wrap(segment).order(java.nio.ByteOrder.BIG_ENDIAN).getInt(8))
                .key();
        store.put(key, new binjava.binstore.Body(segment.length,
                () -> new java.io.ByteArrayInputStream(segment)));
        CommitDelta delta = log.commit(key, counts);
        hub.publish(delta, segment);
        return segment;
    }
}
