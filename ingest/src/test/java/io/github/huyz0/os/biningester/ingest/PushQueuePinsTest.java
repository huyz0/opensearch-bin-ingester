// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
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
        /** Whether an interrupt of the held delivery is swallowed, not restored. */
        final boolean swallow;
        /** What a swallowing delivery waits for next, so the drain finishes first. */
        final CountDownLatch resume = new CountDownLatch(1);

        Held() {
            this(false);
        }

        Held(boolean swallow) {
            this.swallow = swallow;
        }

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
                    if (!swallow) {
                        Thread.currentThread().interrupt();
                    } else {
                        // ⚠️ HELD UNTIL THE DRAIN HAS RETURNED (M13.16 review
                        // T2): let go at once, the pusher could reach take()
                        // before the drain emptied the queue, deliver the next
                        // push and exit on the first sentinel -- the re-queued
                        // one never tested.
                        try {
                            resume.await();
                        } catch (InterruptedException ignored) {
                            // swallowed as well
                        }
                    }
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
                store.capabilities(), new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()));
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

    /**
     * ⚠️ M12.15 (M11.16 T1): when the drain's bound runs out the pusher is
     * interrupted, and every push still queued behind the one in flight is
     * never delivered. Those were lost UNCOUNTED -- neither {@code dropped()}
     * (the budget) nor {@code undeliverable()} (a delivery that threw) saw
     * them. The bound is injected so this is a state the test builds, not a
     * five-second sleep: the first delivery is held and never released, so a
     * 1 ms bound always runs out.
     */
    @Test
    void pushesStillQueuedWhenTheDrainBoundRunsOutAreCountedAbandoned() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held();
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 1 << 20, Duration.ofMillis(1));
            offer(queue, 1, 10);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();
            offer(queue, 2, 10);
            offer(queue, 3, 10);

            queue.drain();

            assertThat(queue.abandoned())
                    .as("⚠️ 2 and 3 were queued behind the held delivery and never delivered")
                    .isEqualTo(2);
            assertThat(held.delivered).as("only the delivery in flight finished")
                    .doesNotContain(2L, 3L);
            assertThat(queue.dropped()).isZero();
            assertThat(queue.undeliverable()).isZero();
        }
    }

    /**
     * ⚠️ THE ABANDONED PUSHES' BYTES ARE RELEASED (M13.16, M12.15 review T2):
     * kept, the budget would count bytes nothing will ever deliver, and later
     * offers would be dropped for them.
     */
    @Test
    void anAbandonedPushsBytesAreReleased() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held();
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 1 << 20, Duration.ofMillis(1));
            offer(queue, 1, 10);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();
            offer(queue, 2, 20);
            offer(queue, 3, 30);

            queue.drain();

            awaitTrue(() -> queue.queuedBytes() == 0, "every byte released");
            assertThat(queue.abandoned()).isEqualTo(2);
        }
    }

    /**
     * ⚠️ A DRAIN THAT GIVES UP STOPS THE PUSHER (M13.16, M12.15 review T3): the
     * held delivery is interrupted, so the thread ends rather than leaking for
     * the life of the process.
     */
    @Test
    void aDrainThatGivesUpStopsTheHeldPusher() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held();
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 1 << 20, Duration.ofMillis(1));
            offer(queue, 1, 10);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();
            offer(queue, 2, 10);
            try {
                queue.drain();

                awaitTrue(() -> !queue.pusherAlive(), "the pusher ended");
            } finally {
                held.gate.countDown();
            }
        }
    }

    /**
     * ⚠️ A SUBSCRIBER THAT SWALLOWS THE INTERRUPT (M13.16, M12.15 review T1):
     * the pusher takes its next entry rather than exiting, so the sentinel goes
     * back behind the abandoned ones -- without it that take() never returns.
     */
    @Test
    void aPusherWhoseSubscriberSwallowsTheInterruptStillExits() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Held held = new Held(true);
        try (var ignored = hub.subscribe(KEY, held)) {
            PushQueue queue = new PushQueue(hub, serving(), 1 << 20, Duration.ofMillis(1));
            offer(queue, 1, 10);
            assertThat(held.entered.await(10, TimeUnit.SECONDS)).isTrue();
            offer(queue, 2, 10);

            queue.drain();
            assertThat(queue.abandoned()).as("2, queued behind the held one").isEqualTo(1);
            held.resume.countDown();

            awaitTrue(() -> !queue.pusherAlive(), "the pusher ended despite the swallow");
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(condition.getAsBoolean()).as(what).isTrue();
    }

    /**
     * ⚠️ THE BOUND DEFAULTINGEST GIVES ITS QUEUE (M13.16, M12.15 review T4):
     * exactly the production one, where the case below pinned the constant.
     */
    @Test
    void defaultIngestGivesItsQueueTheProductionBound() throws Exception {
        try (DefaultIngest ingest = IngestTestSupport.ingest(
                new io.github.huyz0.os.biningester.binstore.CountingBinStore(new MemoryBinStore()),
                new SubscriptionHub(), IngestTestSupport.NEVER)) {
            assertThat(ingest.pushDrainBound()).isEqualTo(PushQueue.DRAIN_BOUND);
        }
    }

    /**
     * ⚠️ The production bound, pinned: under Kubernetes' default 30 s grace
     * period with room for the final flush, and long enough that a subscriber
     * one slow segment behind is not cut off at shutdown.
     */
    @Test
    void theProductionDrainBoundIsFiveSeconds() {
        assertThat(PushQueue.DRAIN_BOUND).isEqualTo(Duration.ofSeconds(5));
    }
}
