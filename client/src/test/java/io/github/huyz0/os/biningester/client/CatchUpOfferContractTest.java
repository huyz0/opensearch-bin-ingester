// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CatchUpOfferContractTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ae"), 0);

    @Test
    void fullLaneUsesOnlyNonBlockingQueueAdmission() throws Exception {
        RejectBlockingQueue queue = new RejectBlockingQueue();
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(queue, new Object(), new Semaphore(0));
        UUID request = UUID.randomUUID();
        lane.begin(request);

        assertThat(lane.tryPut(request, delivery(0))).isTrue();
        assertThat(lane.tryPut(request, delivery(1))).isFalse();
    }

    @Test
    void consumerAvailabilityIsPublishedOnlyAfterDeliveryIsEnqueued() throws Exception {
        GatedOfferQueue queue = new GatedOfferQueue();
        Semaphore consumerAvailable = new Semaphore(0);
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(queue, new Object(), consumerAvailable);
        UUID request = UUID.randomUUID();
        Delivery delivery = delivery(2);
        lane.begin(request);
        AtomicBoolean accepted = new AtomicBoolean();
        Thread producer = Thread.ofVirtual().start(() -> accepted.set(lane.tryPut(request, delivery)));

        try {
            assertThat(queue.awaitOfferEntry()).as("producer reached queue admission").isTrue();
            assertThat(consumerAvailable.tryAcquire())
                    .as("a waiting consumer is not notified before queue insertion completes")
                    .isFalse();
        } finally {
            queue.allowOffer();
        }
        producer.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(producer.isAlive()).isFalse();
        assertThat(accepted).isTrue();
        assertThat(consumerAvailable.tryAcquire()).isTrue();
        assertThat(lane.poll()).isSameAs(delivery);
    }

    @Test
    void competingReaderCannotConsumeNotificationBeforeLanePermitIsPublished() throws Exception {
        GatedReleaseSemaphore consumerAvailable = new GatedReleaseSemaphore();
        CatchUpDeliveryLane lane = new CatchUpDeliveryLane(1, new Object(), consumerAvailable);
        UUID request = UUID.randomUUID();
        Delivery delivery = delivery(3);
        lane.begin(request);
        CountDownLatch readerReady = new CountDownLatch(1);
        CountDownLatch readerDone = new CountDownLatch(1);
        AtomicReference<Delivery> observed = new AtomicReference<>();
        AtomicBoolean accepted = new AtomicBoolean();
        Thread reader = Thread.ofVirtual().start(() -> {
            readerReady.countDown();
            try {
                consumerAvailable.acquire();
                if (lane.tryAcquireDelivery()) {
                    observed.set(lane.poll());
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                readerDone.countDown();
            }
        });
        assertThat(readerReady.await(5, TimeUnit.SECONDS)).isTrue();
        Thread producer = Thread.ofVirtual().start(() -> accepted.set(lane.tryPut(request, delivery)));

        try {
            assertThat(consumerAvailable.awaitPublished()).as("shared consumer permit was published").isTrue();
            assertThat(readerDone.await(5, TimeUnit.SECONDS)).as("notified reader completed").isTrue();
            assertThat(observed.get())
                    .as("the notified reader can acquire the lane permit and visible delivery")
                    .isSameAs(delivery);
        } finally {
            consumerAvailable.allowReleaseToReturn();
        }
        producer.join(TimeUnit.SECONDS.toMillis(5));
        reader.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(producer.isAlive()).isFalse();
        assertThat(reader.isAlive()).isFalse();
        assertThat(accepted).isTrue();
    }

    private static Delivery delivery(long offset) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(KEY, new SegmentRecord("doc-" + offset, OpType.INDEX,
                OptionalLong.of(offset), "{}".getBytes(StandardCharsets.UTF_8)), offset);
        return new Delivery(KEY, "segment-" + offset, 1, offset, FetchMode.INLINE,
                writer.toByteArray(11L));
    }

    private static class RejectBlockingQueue extends ArrayBlockingQueue<Delivery> {
        private RejectBlockingQueue() {
            super(1);
        }

        @Override
        public boolean offer(Delivery delivery, long timeout, TimeUnit unit) {
            throw new AssertionError("catch-up admission must not use a timed offer");
        }

        @Override
        public void put(Delivery delivery) {
            throw new AssertionError("catch-up admission must not use a blocking put");
        }
    }

    private static final class GatedOfferQueue extends RejectBlockingQueue {
        private final CountDownLatch offerEntered = new CountDownLatch(1);
        private final CountDownLatch allowOffer = new CountDownLatch(1);

        @Override
        public boolean offer(Delivery delivery) {
            offerEntered.countDown();
            try {
                allowOffer.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
            return super.offer(delivery);
        }

        private boolean awaitOfferEntry() throws InterruptedException {
            return offerEntered.await(5, TimeUnit.SECONDS);
        }

        private void allowOffer() {
            allowOffer.countDown();
        }
    }

    private static final class GatedReleaseSemaphore extends Semaphore {
        private final CountDownLatch published = new CountDownLatch(1);
        private final CountDownLatch allowReturn = new CountDownLatch(1);

        private GatedReleaseSemaphore() {
            super(0);
        }

        @Override
        public void release() {
            super.release();
            published.countDown();
            try {
                allowReturn.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean awaitPublished() throws InterruptedException {
            return published.await(5, TimeUnit.SECONDS);
        }

        private void allowReleaseToReturn() {
            allowReturn.countDown();
        }
    }
}
