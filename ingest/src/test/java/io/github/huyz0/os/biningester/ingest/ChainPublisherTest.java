// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The one publisher, in chain order, with the writer's held bytes (M10.18, ADR-0075). */
class ChainPublisherTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    private CountingBinStore store;
    private SubscriptionHub hub;
    private final List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
    private AutoCloseable subscription;
    private ChainPublisher publisher;

    @BeforeEach
    void start() throws Exception {
        store = new CountingBinStore(new MemoryBinStore());
        hub = new SubscriptionHub();
        subscription = hub.subscribe(KEY, SubscriptionHub.assembling(got::add));
        publisher = new ChainPublisher(hub, serving(store), 4L << 20);
    }

    @AfterEach
    void stop() throws Exception {
        publisher.close();
        subscription.close();
    }

    private static SegmentServing serving(CountingBinStore store) {
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store));
    }

    private static byte[] segmentOf(int size, int seed) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + seed);
        }
        return b;
    }

    private void stored(String key, byte[] bytes) throws Exception {
        store.put(key, Body.ofBytes(bytes));
    }

    private static CommitDelta delta(long sequence, String segmentKey, long firstOffset) {
        return new CommitDelta(sequence, segmentKey, List.of(new RunCommit(KEY, 2, firstOffset)));
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void deltasArePublishedInChainOrderAndAnythingNotAfterTheLastIsDropped() throws Exception {
        for (String key : List.of("s1", "s2", "s3")) {
            stored(key, segmentOf(1024, key.hashCode()));
        }

        publisher.offer(3, delta(1, "s1", 0));
        publisher.offer(3, delta(2, "s2", 2));
        publisher.offer(3, delta(2, "s2", 2)); // a duplicate: the local hook and a push
        publisher.offer(3, delta(1, "s1", 0)); // behind the last
        publisher.offer(2, delta(9, "s3", 4)); // a stale leaseholder's epoch
        publisher.offer(4, delta(0, "s3", 4)); // a new term restarts the sequence
        await(() -> publisher.published() + publisher.stale() == 6, "all six handled");

        assertThat(got).extracting(SubscriptionHub.Push::segmentKey)
                .containsExactly("s1", "s2", "s3");
        assertThat(got).extracting(SubscriptionHub.Push::firstOffset)
                .containsExactly(0L, 2L, 4L);
        assertThat(got).extracting(SubscriptionHub.Push::chainSequence)
                .as("each carries its delta's own chain position").containsExactly(1L, 2L, 0L);
        assertThat(publisher.stale()).isEqualTo(3);
        assertThat(got.get(2).sequencerEpoch()).isEqualTo(4);
    }

    @Test
    void heldBytesServeTheWritersOwnSegmentWithoutAGetAndAreUsedOnce() throws Exception {
        byte[] mine = segmentOf(4096, 1);
        stored("mine", mine);

        publisher.hold("mine", mine);
        publisher.offer(1, delta(0, "mine", 0));
        await(() -> got.size() == 1, "the push");

        assertThat(got.get(0).via()).isEqualTo(FetchMode.INLINE);
        assertThat(got.get(0).segment()).isEqualTo(mine);
        assertThat(store.counts().gets()).as("the writer holds what it wrote").isZero();
        assertThat(publisher.heldBytes()).as("used once, then forgotten").isZero();
        publisher.forget("mine");
        assertThat(publisher.heldBytes()).as("really gone, not merely uncounted").isZero();
    }

    @Test
    void aSegmentThisPodDidNotWriteIsPublishedCold() throws Exception {
        byte[] theirs = segmentOf(4096, 2);
        stored("theirs", theirs);
        byte[] mine = segmentOf(4096, 3);
        stored("mine", mine);
        publisher.hold("mine", mine);

        publisher.offer(1, new CommitDelta(0, List.of(
                new SegmentCommit("theirs", List.of(new RunCommit(KEY, 2, 0))),
                new SegmentCommit("mine", List.of(new RunCommit(KEY, 2, 2))))));
        await(() -> got.size() == 2, "both segments");

        assertThat(got).extracting(SubscriptionHub.Push::segmentKey)
                .containsExactly("theirs", "mine");
        assertThat(got).allSatisfy(push -> assertThat(push.chainSequence()).isZero());
        assertThat(store.counts().gets())
                .as("one GET, for the segment this pod does not hold").isEqualTo(1);
    }

    @Test
    void theHoldIsBoundedInBytesOldestFirstAndAForgottenSegmentLeavesNothing() throws Exception {
        ChainPublisher small = new ChainPublisher(hub, serving(store), 10_000);
        try {
            small.hold("a", new byte[4000]);
            small.hold("b", new byte[4000]);
            small.hold("c", new byte[4000]);
            assertThat(small.heldBytes()).isEqualTo(8000);
            assertThat(small.heldEvicted()).as("a, the oldest, went").isEqualTo(1);
            small.forget("a");
            assertThat(small.heldBytes()).as("a is really gone, not merely uncounted")
                    .isEqualTo(8000);

            small.hold("d", new byte[2000]);
            assertThat(small.heldBytes()).as("exactly at the bound still fits").isEqualTo(10_000);
            assertThat(small.heldEvicted()).isEqualTo(1);
            small.forget("d");

            small.hold("c", new byte[4000]); // held again: replaced, not added
            assertThat(small.heldBytes()).isEqualTo(8000);

            small.forget("b");
            small.forget("c");
            assertThat(small.heldBytes()).isZero();

            small.hold("huge", new byte[20_000]);
            assertThat(small.heldBytes()).as("above the bound: never held").isZero();
            small.hold("exact", new byte[10_000]);
            assertThat(small.heldBytes()).as("exactly the bound is held").isEqualTo(10_000);
        } finally {
            small.close();
        }
    }

    @Test
    void aFullQueueDropsAndCountsRatherThanBlockingTheCaller() throws Exception {
        ChainPublisher stalled = new ChainPublisher(hub, serving(store), 0);
        stalled.close(); // no worker: nothing drains
        int accepted = 0;
        long acceptedBytes = 0;
        for (int i = 0; i < ChainPublisher.QUEUE_DEPTH + 5; i++) {
            CommitDelta next = delta(i, "s" + i, 2L * i);
            if (stalled.offer(1, next)) {
                accepted++;
                acceptedBytes += ChainPublisher.estimatedBytes(next);
            }
        }
        assertThat(accepted).isEqualTo(ChainPublisher.QUEUE_DEPTH);
        assertThat(stalled.overflowed()).isEqualTo(5);
        assertThat(stalled.queuedBytes()).as("a delta refused by count is not counted in bytes")
                .isEqualTo(acceptedBytes);
    }

    @Test
    void theQueueIsBoundedInBytesSoLargeDeltasDropBeforeTheCountIsReached() throws Exception {
        ChainPublisher stalled = new ChainPublisher(hub, serving(store), 0);
        stalled.close(); // no worker: nothing drains
        List<RunCommit> runs = new java.util.ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            runs.add(new RunCommit(new RunKey(UUID.randomUUID(), 0), 1, i));
        }
        CommitDelta large = new CommitDelta(0, "bins/cluster-a/data/big.bseg", runs);
        assertThat(ChainPublisher.estimatedBytes(large))
                .as("its keys and its runs: 64 + (64 + 2 per key char + 48 per run)")
                .isEqualTo(64 + 64 + 2L * "bins/cluster-a/data/big.bseg".length() + 48L * 10_000);
        int accepted = 0;
        for (int i = 0; i < ChainPublisher.QUEUE_DEPTH; i++) {
            if (stalled.offer(1, large)) {
                accepted++;
            }
        }
        assertThat(accepted).as("the byte bound, long before the count").isLessThan(1000);
        assertThat(stalled.overflowed()).isEqualTo(ChainPublisher.QUEUE_DEPTH - accepted);
        assertThat(stalled.queuedBytes()).isLessThanOrEqualTo(ChainPublisher.QUEUE_BYTES)
                .isEqualTo(accepted * ChainPublisher.estimatedBytes(large));
    }

    @Test
    void aPublishThatFailsIsCountedAndTheWorkerGoesOn() throws Exception {
        stored("s2", segmentOf(1024, 2));
        // "absent" is neither held nor stored: its cold publication fails.
        publisher.offer(1, delta(0, "absent", 0));
        publisher.offer(1, delta(1, "s2", 2));
        await(() -> got.stream().anyMatch(p -> p.segmentKey().equals("s2")), "the next delta");

        assertThat(publisher.failed()).isEqualTo(1);
        assertThat(got).extracting(SubscriptionHub.Push::segmentKey).containsExactly("s2");
    }

    @Test
    void closingDeliversWhatIsQueuedFirst() throws Exception {
        for (int i = 0; i < 20; i++) {
            stored("q" + i, segmentOf(512, i));
        }
        ChainPublisher closing = new ChainPublisher(hub, serving(store), 0);
        for (int i = 0; i < 20; i++) {
            closing.offer(1, delta(i, "q" + i, 2L * i));
        }
        closing.close();

        assertThat(closing.published()).isEqualTo(20);
        assertThat(closing.abandoned()).isZero();
        assertThat(closing.queuedBytes()).as("every delivered delta is released").isZero();
        assertThat(got).hasSize(20);
    }

    @Test
    void closingGivesUpOnAStuckSubscriberAndCountsWhatItAbandons() throws Exception {
        for (String key : List.of("x0", "x1", "x2")) {
            stored(key, segmentOf(512, key.hashCode()));
        }
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        RunKey stuckKey = new RunKey(UUID.randomUUID(), 0);
        try (var stuck = hub.subscribe(stuckKey, SubscriptionHub.assembling(push -> {
            entered.countDown();
            try {
                new java.util.concurrent.CountDownLatch(1).await(); // until interrupted
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }))) {
            ChainPublisher closing = new ChainPublisher(hub, serving(store), 0);
            for (int i = 0; i < 3; i++) {
                closing.offer(1, new CommitDelta(i, "x" + i,
                        List.of(new RunCommit(stuckKey, 2, 2L * i))));
            }
            assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
            long started = System.nanoTime();
            closing.close();

            assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started))
                    .as("bounded, within the grace period").isLessThan(15);
            assertThat(closing.abandoned()).as("the two still queued").isEqualTo(2);
            assertThat(closing.queued()).isZero();
        }
    }
}
