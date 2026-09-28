// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The Tier-2 reader's residue from M10.22's review (M11.12, H7): a stalled
 * read holds nothing a delivery or a non-emptying release needs.
 */
class TierTwoResidueTest {

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, List.of(
                new io.github.huyz0.os.biningester.format.SegmentCommit("seg/" + sequence,
                        List.of(new io.github.huyz0.os.biningester.format.RunCommit(
                                new RunKey(UUID.fromString(
                                        "00000000-0000-0000-0000-00000000000a"), 0), 1, 0)),
                        new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution(
                                "pod1", "inc-1", 0))));
    }

    @Test
    void aCursorObservedDuringAStalledReadIsNotHeldBehindIt() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<CommitDelta> consumed = new CopyOnWriteArrayList<>();
        TierTwoChainPoller poller = new TierTwoChainPoller(3, 40, (epoch, sequence) -> {
            reading.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.of(delta(sequence));
        }, () -> false, consumed::add);

        Thread poll = Thread.ofVirtual().start(() -> poller.poll(1));
        try {
            assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> observed = CompletableFuture.runAsync(
                    () -> poller.observeCursor(3, 41));
            observed.get(5, TimeUnit.SECONDS);
            assertThat(observed)
                    .as("⚠️ A DELIVERY's CURSOR IS NOT HELD BEHIND THE STALLED READ -- and "
                            + "with it the client monitor a non-emptying release needs")
                    .isDone();
        } finally {
            release.countDown();
            poll.join(5_000);
        }
        assertThat(consumed)
                .as("⚠️ THE READ's DELTA, 41, WAS ALREADY DELIVERED WHILE IT WAS READ: "
                        + "dropped, not replayed")
                .isEmpty();
    }

    @Test
    void aDeltaReadWithNoCursorMovementIsConsumed() throws Exception {
        List<CommitDelta> consumed = new CopyOnWriteArrayList<>();
        TierTwoChainPoller poller = new TierTwoChainPoller(3, 40,
                (epoch, sequence) -> Optional.of(delta(sequence)), () -> false, consumed::add);
        poller.poll(1);
        assertThat(consumed).extracting(CommitDelta::sequence).containsExactly(41L);
    }

    @Test
    void aReleaseOfAStreamHeldTwiceOnAOneStreamNodeDoesNotWaitForAStalledRead()
            throws Exception {
        CountDownLatch readReached = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        Map<RunKey, SubscriptionTransport.Listener> listeners = new ConcurrentHashMap<>();
        SubscriptionTransport transport = (key, subscribed) -> {
            listeners.put(key, subscribed);
            return () -> { };
        };
        RunKey a = new RunKey(UUID.fromString("00000000-0000-0000-0000-00000000000a"), 0);

        try (NodeSubscriptions node = new NodeSubscriptions(transport, 8)) {
            node.enableTierTwo((epoch, sequence) -> {
                readReached.countDown();
                try {
                    if (!allowRead.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("test did not release the stalled Tier 2 read");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", interrupted);
                }
                return Optional.empty();
            }, () -> false, ignored -> { }, () -> { });
            node.clientFor(a);
            node.clientFor(a);
            listeners.get(a).onDelivery(new Delivery(a, "segment/key", 1, 0,
                    FetchMode.INLINE, new byte[0], null, 0, 40));

            Thread poll = Thread.ofVirtual().start(() -> node.pollTierTwo(1));
            try {
                assertThat(readReached.await(5, TimeUnit.SECONDS)).isTrue();
                Thread release = Thread.ofVirtual().start(() -> node.release(a));
                release.join(5_000);

                assertThat(release.isAlive())
                        .as("⚠️ A IS STILL HELD ONCE, so the node does not go idle: the "
                                + "release has nothing to order against the stalled read")
                        .isFalse();
                assertThat(node.openClients()).isEqualTo(1);
            } finally {
                allowRead.countDown();
                poll.join(5_000);
            }
        }
    }
}
