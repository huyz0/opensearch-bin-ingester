// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TierTwoReleaseRaceTest {
    @Test
    void finalReleaseWaitsUntilAnAdmittedTierTwoReadHasStartedAndFinished() throws Exception {
        CountDownLatch readReached = new CountDownLatch(1);
        CountDownLatch allowRead = new CountDownLatch(1);
        CountDownLatch releaseAttempted = new CountDownLatch(1);
        CountDownLatch releaseCompleted = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        AtomicReference<SubscriptionTransport.Listener> listener = new AtomicReference<>();
        SubscriptionTransport transport = (key, subscribed) -> {
            listener.set(subscribed);
            return () -> { };
        };
        RunKey run = new RunKey(UUID.fromString(
                "00000000-0000-0000-0000-000000000004"), 0);

        try (NodeSubscriptions node = new NodeSubscriptions(transport, 8)) {
            node.enableTierTwo((epoch, sequence) -> {
                readReached.countDown();
                try {
                    if (!allowRead.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("test did not release the blocked Tier 2 read");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Tier 2 read was interrupted", interrupted);
                }
                gets.incrementAndGet();
                return Optional.empty();
            }, () -> false, ignored -> { }, () -> { });
            node.clientFor(run);
            listener.get().onDelivery(new Delivery(run, "segment/key", 1, 0,
                    FetchMode.INLINE, new byte[0], null, 0, 40));

            Thread poll = Thread.ofVirtual().start(() -> node.pollTierTwo(1));
            assertThat(readReached.await(5, TimeUnit.SECONDS)).isTrue();
            Thread release = Thread.ofVirtual().start(() -> {
                releaseAttempted.countDown();
                node.release(run);
                releaseCompleted.countDown();
            });

            try {
                assertThat(releaseAttempted.await(5, TimeUnit.SECONDS)).isTrue();
                release.join(100);
                assertThat(release.getState()).isEqualTo(Thread.State.BLOCKED);
                assertThat(gets).hasValue(0);
            } finally {
                allowRead.countDown();
            }

            poll.join(5_000);
            release.join(5_000);
            assertThat(poll.isAlive()).isFalse();
            assertThat(release.isAlive()).isFalse();
            assertThat(releaseCompleted.getCount()).isZero();
            assertThat(gets).hasValue(1);

            node.pollTierTwo(2);
            assertThat(gets).hasValue(1);
        }
    }
}
