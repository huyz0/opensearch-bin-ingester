// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CatchUpDeliveryLaneTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ab"), 0);

    private static final class PausingRecords extends ArrayDeque<ConsumerRecord> {
        private final CountDownLatch removed = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);

        @Override
        public ConsumerRecord poll() {
            ConsumerRecord record = super.poll();
            removed.countDown();
            try {
                resume.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return record;
        }
    }

    private static Delivery delivery(long offset, String... ids) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (String id : ids) {
            writer.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return new Delivery(KEY, "catch-up-segment", ids.length, offset, FetchMode.INLINE,
                writer.toByteArray(1L));
    }

    @Test
    void liveRecordsYieldToCatchUpAfterFiniteRecordQuantum() throws Exception {
        try (ConsumerClient client = new ConsumerClient(KEY, 64, null, TestRetries.noFailedFetch())) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, delivery(0, "replay-0", "replay-1", "replay-2"));
            String[] liveIds = new String[ConsumerClient.LIVE_RECORD_QUANTUM];
            for (int i = 0; i < liveIds.length; i++) {
                liveIds[i] = "live-" + i;
            }
            client.deliver(delivery(100, liveIds));
            client.deliver(delivery(100 + liveIds.length, "live-after-quantum"));

            List<String> ids = new ArrayList<>();
            for (int i = 0; i < ConsumerClient.LIVE_RECORD_QUANTUM + 1; i++) {
                ids.add(client.readNext(Duration.ZERO).orElseThrow().record().id());
            }

            assertThat(ids).hasSize(ConsumerClient.LIVE_RECORD_QUANTUM + 1);
            assertThat(ids.subList(0, ConsumerClient.LIVE_RECORD_QUANTUM))
                    .allMatch(id -> id.startsWith("live-"));
            assertThat(ids.get(ConsumerClient.LIVE_RECORD_QUANTUM)).isEqualTo("replay-0");
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("live-after-quantum");
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("replay-1");
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("replay-2");
        }
    }

    @Test
    void matchingEndCompletesOnlyAfterEveryReplayRecordIsDelivered() throws Exception {
        try (ConsumerClient client = new ConsumerClient(KEY, 8, null, TestRetries.noFailedFetch())) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, delivery(0, "a", "b", "c"));

            assertThat(client.completeCatchUp(UUID.randomUUID())).isFalse();
            assertThat(client.catchUpComplete(request)).isFalse();
            assertThat(client.completeCatchUp(request)).isFalse();
            assertThat(client.catchUpComplete(request)).isFalse();

            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id()).isEqualTo("a");
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id()).isEqualTo("b");
            assertThat(client.catchUpComplete(request)).isFalse();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id()).isEqualTo("c");
            assertThat(client.completeCatchUp(request)).isTrue();
            assertThat(client.catchUpComplete(request)).isTrue();
            assertThat(client.catchUpComplete(UUID.randomUUID())).isFalse();
        }
    }

    @Test
    void catchUpRejectsForeignEventsAndEndsAndAllowsReuseAfterCompletion() throws Exception {
        try (ConsumerClient client = new ConsumerClient(KEY, 2, null, TestRetries.noFailedFetch())) {
            UUID request = UUID.randomUUID();
            UUID foreign = UUID.randomUUID();
            client.beginCatchUp(request);

            assertThatThrownBy(() -> client.beginCatchUp(foreign))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> client.deliverCatchUp(foreign, delivery(0, "foreign")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(client.completeCatchUp(foreign)).isFalse();
            assertThat(client.catchUpComplete(foreign)).isFalse();
            assertThat(client.completeCatchUp(request)).isTrue();
            assertThat(client.catchUpComplete(request)).isTrue();
            assertThatThrownBy(() -> client.deliverCatchUp(request, delivery(0, "late")))
                    .isInstanceOf(IllegalStateException.class);

            client.beginCatchUp(foreign);
            assertThat(client.catchUpComplete(request)).isFalse();
        }
    }

    @Test
    void completionCannotPassAReplayRecordDuringItsHandoff() throws Exception {
        UUID request = UUID.randomUUID();
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(1, new Object(), new Semaphore(0));
        lane.begin(request);
        lane.put(request, delivery(0, "last"));
        assertThat(lane.end(request)).isFalse();
        PausingRecords records = new PausingRecords();
        records.add(new ConsumerRecord(0, new io.github.huyz0.os.biningester.format.SegmentRecord(
                "last", io.github.huyz0.os.biningester.format.OpType.INDEX,
                java.util.OptionalLong.of(1), "{}".getBytes(StandardCharsets.UTF_8)), 1));

        Thread reader = Thread.ofVirtual().start(() -> lane.handoff(records));
        assertThat(records.removed.await(5, TimeUnit.SECONDS)).isTrue();
        CountDownLatch checked = new CountDownLatch(1);
        Thread checker = Thread.ofVirtual().start(() -> {
            checked.countDown();
            lane.complete(request);
        });

        assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(awaitsMonitor(checker, Duration.ofSeconds(5)))
                .as("completion is blocked on the record handoff's atomic accounting")
                .isTrue();
        records.resume.countDown();
        reader.join(Duration.ofSeconds(5));
        checker.join(Duration.ofSeconds(5));
        assertThat(checked.getCount()).isZero();
        assertThat(lane.complete(request)).isTrue();
    }

    private static boolean awaitsMonitor(Thread thread, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread.getState() == Thread.State.BLOCKED;
    }

    private static boolean awaits(Thread thread, Thread.State expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (thread.getState() != expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread.getState() == expected;
    }

    @Test
    void catchUpLaneBackpressuresWithoutBlockingLiveDelivery() throws Exception {
        try (ConsumerClient client = new ConsumerClient(KEY, 1, null, TestRetries.noFailedFetch())) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, delivery(0, "first"));
            Delivery second = delivery(1, "second");
            CountDownLatch finished = new CountDownLatch(1);
            Thread producer = Thread.ofVirtual().start(() -> {
                try {
                    client.deliverCatchUp(request, second);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(awaits(producer, Thread.State.WAITING, Duration.ofSeconds(5)))
                    .as("producer reached the full catch-up lane's blocking put")
                    .isTrue();
            assertThat(finished.getCount())
                    .as("full catch-up lane applies backpressure").isEqualTo(1L);
            client.deliver(delivery(100, "live"));
            assertThat(client.queuedDeliveries()).isEqualTo(1);

            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("live");
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("first");
            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            producer.join(Duration.ofSeconds(5));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("second");
        }

        try (ConsumerClient client = new ConsumerClient(KEY, 1, null, TestRetries.noFailedFetch())) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, delivery(0, "retained"));
            CountDownLatch finished = new CountDownLatch(1);
            AtomicBoolean interrupted = new AtomicBoolean();
            Delivery blocked = delivery(1, "cancelled");
            Thread producer = Thread.ofVirtual().start(() -> {
                try {
                    client.deliverCatchUp(request, blocked);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                } finally {
                    finished.countDown();
                }
            });

            assertThat(awaits(producer, Thread.State.WAITING, Duration.ofSeconds(5)))
                    .as("producer reached the full catch-up lane's blocking put")
                    .isTrue();
            producer.interrupt();
            producer.join(Duration.ofSeconds(5));
            assertThat(finished.getCount()).isZero();
            assertThat(interrupted).isTrue();
            assertThat(client.completeCatchUp(request)).isFalse();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().record().id())
                    .isEqualTo("retained");
            assertThat(client.catchUpComplete(request)).isTrue();
            assertThat(client.readNext(Duration.ofMillis(30))).isEmpty();
        }
    }
}
