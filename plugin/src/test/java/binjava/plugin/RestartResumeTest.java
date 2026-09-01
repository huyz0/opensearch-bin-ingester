// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.client.ConsumerClient;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IngestionShardConsumer.ReadResult;

/**
 * Criterion 4's POINTER CONTRACT, at T2 — NOT the node level.
 *
 * <p>⚠️ This does not start or kill an OpenSearch node, and criterion 4's own
 * words are "killing and restarting the OpenSearch node". What is proven here:
 * {@code batch_start} is OpenSearch's, not ours, and it persists the pointer of
 * the FIRST message of the batch a node was mid-way through, so a crash replays
 * that batch and nothing earlier. Our side of that contract is two things: the
 * pointer must survive a round trip through its string form (that is what is
 * written into Lucene's commit data), and {@code readNext} must honour it rather
 * than starting from the beginning of what it happens to hold. A real node
 * killed and restarted, with {@code batch_start} read back out of real Lucene
 * commit data, is M1.17b.
 */
class RestartResumeTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(INDEX, 0);

    private static final class FakeTransport implements SubscriptionTransport {
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        void push(Delivery d) {
            listeners.forEach(l -> l.onDelivery(d));
        }
    }

    /** One commit batch: {@code count} records starting at {@code firstOffset}. */
    private static Delivery batch(long firstOffset, int count) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (int i = 0; i < count; i++) {
            long offset = firstOffset + i;
            w.add(KEY, new SegmentRecord("doc-" + offset, OpType.INDEX, OptionalLong.of(1),
                    ("{\"n\":" + offset + "}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return new Delivery(KEY, "seg-" + firstOffset, count, firstOffset, w.toByteArray(1L));
    }

    private static List<Long> offsets(List<ReadResult<BinStoreOffset, BinStoreMessage>> results) {
        List<Long> out = new ArrayList<>();
        for (var r : results) {
            out.add(r.getPointer().offset());
        }
        return out;
    }

    @Test
    void aPointerSurvivesTheRoundTripThroughCommitData() {
        // ⚠️ This is the ONLY form the pointer survives a restart in: OpenSearch
        // writes `asString()` into Lucene's commit data and hands it back to
        // `parsePointerFromString` on recovery. A pointer that did not
        // round-trip would resume somewhere else entirely, and the loss would
        // look like a consumer bug rather than a serialisation one.
        BinStoreOffset original = new BinStoreOffset(4_294_967_296L);
        BinStoreOffset recovered = new BinStoreConsumerFactory(
                new NodeSubscriptions(new FakeTransport(), 8))
                .parsePointerFromString(original.asString());

        assertThat(recovered).isEqualTo(original);
        assertThat(recovered.offset()).isEqualTo(4_294_967_296L);
    }

    @Test
    void readNextResumesFromThePersistedPointerRatherThanFromTheBeginning() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient client = new ConsumerClient(transport, KEY, 64);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, client)) {
            transport.push(batch(0, 10));

            // ⚠️ Resuming from offset 4, EXCLUSIVE. The mutation this refuses is
            // "resume from earliest", which returns 0..9 -- every already-indexed
            // document replayed on every restart, unbounded duplication.
            var resumed = consumer.readNext(new BinStoreOffset(4), false, 100, 200);

            assertThat(offsets(resumed)).containsExactly(5L, 6L, 7L, 8L, 9L);
        }
    }

    @Test
    void restartDuplicatesOnlyWithinOneCommitBatch() throws Exception {
        // ⚠️ Criterion 4's bound, and the reason `batch_start` is the START of
        // the batch and not its end. Two commit batches are delivered: 0..4 and
        // 5..9. The node crashed mid-way through the second, so what it
        // persisted is 5 -- the first offset of the batch it had not finished.
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient client = new ConsumerClient(transport, KEY, 64);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, client)) {
            transport.push(batch(0, 5));
            transport.push(batch(5, 5));

            var resumed = consumer.readNext(new BinStoreOffset(5), true, 100, 200);

            // ⚠️ EXACTLY the interrupted batch. Not less -- resuming from the
            // batch END (9) would lose 5..8, which is criterion 4's "loses
            // nothing" half. Not more -- resuming from earliest would replay
            // 0..4 as well, which is the "duplicates only within one commit
            // batch" half. Both mutations are refused by the same assertion.
            assertThat(offsets(resumed)).containsExactly(5L, 6L, 7L, 8L, 9L);
            assertThat(resumed).hasSize(5);
        }
    }
}
