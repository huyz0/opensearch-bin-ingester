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
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IngestionShardConsumer.ReadResult;

/** WARNING: timeoutMillis is ours to use -- that is what makes zero idle cost possible. */
class BinStoreShardConsumerTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(A, 0);

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

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return new Delivery(KEY, "seg", ids.length, firstOffset, w.toByteArray(1L));
    }

    @Test
    void closingAConsumerBuiltViaTheSharedConstructorReleasesRatherThanCloses() throws Exception {
        // ⚠️ THE T1 regression for M1.17b's real bug, which until now only the
        // expensive T4 RestartResumeIT caught -- both reviewers confirmed
        // reverting the fix left the entire T0-T2 `./gradlew test` suite green.
        // BinStoreConsumerFactory.createShardConsumer wires production through
        // the 3-arg (RunKey, NodeSubscriptions) constructor specifically so
        // close() RELEASES the shared client instead of closing it directly --
        // this constructs that exact path, not NodeSubscriptions in isolation.
        FakeTransport transport = new FakeTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, KEY, node);
            consumer.close();

            assertThat(transport.listeners)
                    .as("closing the consumer released the shared client's subscription")
                    .isEmpty();

            // ⚠️ THE assertion that catches `onClose = client::close` reverted:
            // under that mutation the entry is never removed from
            // NodeSubscriptions, so this call hands back the SAME dead client
            // instead of opening a fresh one.
            node.clientFor(KEY);
            assertThat(node.clientsCreated())
                    .as("a NEW client was constructed for the reused key, not the dead one")
                    .isEqualTo(2);
        }
    }

    @Test
    void readNextReturnsMessagesWithTheirPointers() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 16);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(3, c)) {
            t.push(delivery(100, "a", "b"));
            var results = consumer.readNext(10, 200);
            assertThat(results).hasSize(2);
            assertThat(results.get(0).getPointer()).isEqualTo(new BinStoreOffset(100));
            assertThat(new String(results.get(0).getMessage().getPayload(), StandardCharsets.UTF_8))
                    .startsWith("{\"_id\":\"a\"");
            assertThat(results.get(1).getPointer()).isEqualTo(new BinStoreOffset(101));
            // ⚠️ The SEGMENT's creation time, never this node's clock: the value
            // came from `toByteArray(1L)` above. A replay must produce the same
            // documents as the original run, so an ingestion-time stamp is a
            // defect. Asserting a constant is what kills BOTH mutations -- a
            // hardcoded 0L and a substituted System.currentTimeMillis().
            assertThat(results.get(0).getMessage().getTimestamp()).isEqualTo(1L);
            assertThat(results.get(1).getMessage().getTimestamp()).isEqualTo(1L);
            assertThat(consumer.getShardId()).isEqualTo(3);
        }
    }

    @Test
    void readNextBlocksForTheTimeoutWhenIdleAndReturnsEmpty() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 16);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, c)) {
            long start = System.nanoTime();
            var results = consumer.readNext(10, 300);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            // WARNING: the engine calls readNext again immediately, so returning
            // early IS a poll. Blocking here is the whole mechanism.
            assertThat(results).isEmpty();
            assertThat(elapsed).isGreaterThanOrEqualTo(290);
        }
    }

    @Test
    void aBatchDoesNotWaitAgainOnceSomethingArrived() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 16);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, c)) {
            t.push(delivery(0, "a", "b"));
            long start = System.nanoTime();
            var results = consumer.readNext(10, 5_000);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            // WARNING: blocking again after the first record would hold a full
            // batch hostage to the timeout -- 5 seconds added to every batch.
            assertThat(results).hasSize(2);
            assertThat(elapsed).as("returned as soon as the queue drained").isLessThan(2_000);
        }
    }

    @Test
    void includeStartDecidesWhetherThePointerItselfIsReplayed() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 16);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, c)) {
            t.push(delivery(5, "a", "b", "c"));
            var exclusive = consumer.readNext(new BinStoreOffset(5), false, 10, 200);
            // WARNING: exclusive is the resume path. Including the start replays
            // the last record of every batch, forever.
            assertThat(exclusive).hasSize(2);
            assertThat(exclusive.get(0).getPointer()).isEqualTo(new BinStoreOffset(6));
        }
    }

    @Test
    void lagIsServedFromMemoryAndCostsNothingWhenIdle() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 16);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, c)) {
            assertThat(consumer.getPointerBasedLag(new BinStoreOffset(0)))
                    .as("nothing seen yet").isZero();
            t.push(delivery(0, "a", "b", "c"));
            consumer.readNext(10, 200);
            // WARNING: OpenSearch calls this periodically even while PAUSED. One
            // store read here and an idle cluster costs money forever.
            assertThat(consumer.getPointerBasedLag(new BinStoreOffset(0))).isEqualTo(2);
            assertThat(consumer.getPointerBasedLag(new BinStoreOffset(2))).isZero();
            assertThat(consumer.getPointerBasedLag(new BinStoreOffset(99)))
                    .as("never negative").isZero();
        }
    }

    @Test
    void pointersAreParsedFromTheirStringForm() throws Exception {
        FakeTransport t = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(t, KEY, 4);
                BinStoreShardConsumer consumer = new BinStoreShardConsumer(0, c)) {
            assertThat(consumer.pointerFromOffset(new BinStoreOffset(77).asString()))
                    .isEqualTo(new BinStoreOffset(77));
            assertThat(consumer.earliestPointer()).isEqualTo(new BinStoreOffset(0));
        }
    }
}
