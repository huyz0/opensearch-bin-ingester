// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link PushQueue}'s shutdown drain and its byte budget's boundary (M11.16,
 * H11).
 *
 * <p>⚠️ THE FIRST DELIVERY IS HELD IN THE SUBSCRIBER, so the queue behind it
 * is a state the test builds: its bytes stay charged while it is delivered,
 * and the pushes offered meanwhile are still queued when the drain begins.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class PushQueuePinsTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("7a3c9e10-2222-4333-8444-555566667777"), 0);

    /** Records each completed push's chain sequence; the first waits on {@code gate}. */
    private static final class Held implements SubscriptionHub.Subscriber {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);
        final List<Long> delivered = new CopyOnWriteArrayList<>();

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            return (buffer, offset, length) -> { };
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            if (entered.getCount() > 0) {
                entered.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            pushes.forEach(push -> delivered.add(push.chainSequence()));
        }

        @Override
        public String az() {
            return null;
        }
    }

    private static SegmentServing serving() {
        MemoryBinStore store = new MemoryBinStore();
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store));
    }

    private static void offer(PushQueue queue, long sequence, int bytes) {
        String segmentKey = "seg-" + sequence;
        queue.offer(new CommitDelta(sequence, segmentKey, List.of(new RunCommit(KEY, 1, 10))),
                segmentKey, new byte[bytes], SubscriptionHub.EPOCH_UNKNOWN);
    }

    @Test
    void theDrainDeliversEveryPushAlreadyQueuedInOrder() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held();
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 1 << 20);
            offer(queue, 1, 10);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();
            offer(queue, 2, 10);
            offer(queue, 3, 10);

            Thread drain = Thread.ofPlatform().start(queue::drain);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (drain.getState() != Thread.State.TIMED_WAITING
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(drain.getState()).as("PREMISE: the drain began while 2 and 3 were queued")
                    .isEqualTo(Thread.State.TIMED_WAITING);
            held.gate.countDown();
            drain.join(TimeUnit.SECONDS.toMillis(10));

            assertThat(held.delivered)
                    .as("⚠️ SHUTTING DOWN DELIVERS what is queued, behind the sentinel's place")
                    .containsExactly(1L, 2L, 3L);
            assertThat(queue.dropped()).isZero();
        }
    }

    @Test
    void aPushThatFillsTheBudgetExactlyIsQueuedAndOneByteMoreIsDropped() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held();
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 100);
            offer(queue, 1, 60);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();

            offer(queue, 2, 40);
            assertThat(queue.dropped()).as("60 + 40 is AT the budget of 100, not past it")
                    .isZero();
            offer(queue, 3, 1);
            assertThat(queue.dropped()).as("60 + 40 + 1 is past it").isEqualTo(1);

            held.gate.countDown();
            queue.drain();
            assertThat(held.delivered).containsExactly(1L, 2L);
        }
    }
}
