// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** WARNING: readNext BLOCKS -- that is what makes zero idle cost reachable. */
class ConsumerClientTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(A, 0);

    /** A transport with no socket: criterion 3 runs 1,600 consumers this way. */
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

        int listenerCount() {
            return listeners.size();
        }
    }

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        return new Delivery(KEY, "seg", ids.length, firstOffset, segmentOf(ids));
    }

    @Test
    void readNextReturnsRecordsInOrderWithTheCommitLogsOffsets() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            transport.push(delivery(100, "a", "b", "c"));
            // WARNING: offsets come from the COMMIT LOG's assignment, advanced
            // per record -- not from the record's position in the segment. The
            // two coincide within one flush and diverge across flushes.
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(100);
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(101);
            ConsumerRecord third = c.readNext(Duration.ofMillis(50)).orElseThrow();
            assertThat(third.offset()).isEqualTo(102);
            assertThat(third.record().id()).isEqualTo("c");
        }
    }

    @Test
    void offsetsContinueAcrossFlushesRatherThanRestarting() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            transport.push(delivery(0, "a", "b"));
            transport.push(delivery(2, "c"));
            List<Long> offsets = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                offsets.add(c.readNext(Duration.ofMillis(50)).orElseThrow().offset());
            }
            // WARNING: deriving the offset from the segment position gives
            // 0,1,0 here -- passes any single-flush test, then silently restarts
            // every consumer's dedup at zero on the second flush.
            assertThat(offsets).containsExactly(0L, 1L, 2L);
        }
    }

    @Test
    void readNextBlocksUntilAPushArrives() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            CountDownLatch reading = new CountDownLatch(1);
            var result = new java.util.concurrent.atomic.AtomicReference<Optional<ConsumerRecord>>();
            Thread reader = Thread.ofVirtual().start(() -> {
                try {
                    reading.countDown();
                    result.set(c.readNext(Duration.ofSeconds(30)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(50);
            assertThat(result.get()).as("still parked, not spinning").isNull();

            transport.push(delivery(7, "x"));
            reader.join(Duration.ofSeconds(5));
            // WARNING: it must WAKE, and promptly. A version that returned empty
            // immediately would turn every caller into a polling loop and NFR-2
            // would be unreachable through the SPI.
            assertThat(result.get()).isPresent();
            assertThat(result.get().orElseThrow().offset()).isEqualTo(7);
        }
    }

    @Test
    void readNextBlocksForTheFullTimeoutWhenNothingArrives() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            long start = System.nanoTime();
            Optional<ConsumerRecord> got = c.readNext(Duration.ofMillis(300));
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            assertThat(got).isEmpty();
            // WARNING: returning EARLY is the same defect as not blocking at all,
            // wearing a different mask -- OpenSearch's ingestion loop calls
            // readNext again immediately, so an early return is a poll at
            // whatever rate that loop runs. Criterion 5's second half.
            assertThat(elapsedMillis).as("waited the whole timeout").isGreaterThanOrEqualTo(290);
        }
    }

    @Test
    void aQueuedDeliveryIsReturnedWithoutWaiting() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            transport.push(delivery(0, "a"));
            long start = System.nanoTime();
            assertThat(c.readNext(Duration.ofSeconds(10))).isPresent();
            assertThat((System.nanoTime() - start) / 1_000_000)
                    .as("must not wait when work is already queued").isLessThan(1_000);
        }
    }

    @Test
    void aFullQueueDropsRatherThanBlockingTheIngester() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 2)) {
            for (int i = 0; i < 10; i++) {
                transport.push(delivery(i, "r" + i));
            }
            // WARNING: the commit is already durable. Blocking the ingester on a
            // slow consumer would let one wedged node stall every writer; the
            // consumer falls behind and recovers from the log instead.
            assertThat(c.queuedDeliveries()).isEqualTo(2);
        }
    }

    @Test
    void closingUnsubscribes() throws Exception {
        FakeTransport transport = new FakeTransport();
        ConsumerClient c = new ConsumerClient(transport, KEY, 4);
        assertThat(transport.listenerCount()).isEqualTo(1);
        c.close();
        assertThat(transport.listenerCount()).as("no listener left behind").isZero();
    }

    @Test
    void thePayloadIsTheDefaultEnvelope() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 4)) {
            transport.push(delivery(5, "doc"));
            ConsumerRecord r = c.readNext(Duration.ofMillis(50)).orElseThrow();
            String json = new String(r.payload(), StandardCharsets.UTF_8);
            assertThat(json).startsWith("{\"_id\":\"doc\",\"_op_type\":\"index\"")
                    .contains("\"_source\":{\"id\":\"doc\"}");
        }
    }
}
