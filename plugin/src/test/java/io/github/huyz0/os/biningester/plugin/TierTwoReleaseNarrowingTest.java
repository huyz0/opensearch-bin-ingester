// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Only the release that EMPTIES the node waits for an admitted Tier-2 read
 * (M10.22, harvested from `fb0f56b`).
 *
 * <p>⚠️ THE ORDERING M9.43 NEEDS is that a read admitted while clients exist
 * finishes before the node goes idle -- `TierTwoReleaseRaceTest` pins that.
 * It never needed a shard relocating off a node that still holds another to
 * block behind a stalled read, which the node-wide monitor imposed.
 */
class TierTwoReleaseNarrowingTest {

    @Test
    void aReleaseThatLeavesTheNodeHoldingAnotherStreamDoesNotWaitForAStalledRead()
            throws Exception {
        CountDownLatch readReached = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        Map<RunKey, SubscriptionTransport.Listener> listeners = new ConcurrentHashMap<>();
        SubscriptionTransport transport = (key, subscribed) -> {
            listeners.put(key, subscribed);
            return () -> { };
        };
        RunKey a = new RunKey(UUID.fromString("00000000-0000-0000-0000-00000000000a"), 0);
        RunKey b = new RunKey(UUID.fromString("00000000-0000-0000-0000-00000000000b"), 0);

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
            node.clientFor(b);
            listeners.get(a).onDelivery(new Delivery(a, "segment/key", 1, 0,
                    FetchMode.INLINE, new byte[0], null, 0, 40));

            Thread poll = Thread.ofVirtual().start(() -> node.pollTierTwo(1));
            try {
                assertThat(readReached.await(5, TimeUnit.SECONDS)).isTrue();
                Thread release = Thread.ofVirtual().start(() -> node.release(a));
                release.join(5_000);

                assertThat(release.isAlive())
                        .as("⚠️ B IS STILL HELD, so the node does not go idle: releasing A "
                                + "has nothing to order against the stalled read")
                        .isFalse();
                assertThat(node.openClients()).isEqualTo(1);
            } finally {
                allowRead.countDown();
                poll.join(5_000);
            }
        }
    }
}
