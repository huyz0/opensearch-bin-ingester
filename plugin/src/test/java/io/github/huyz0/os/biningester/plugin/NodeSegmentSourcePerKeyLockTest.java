// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The node cache must not turn one slow segment GET into a node-wide pause. */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NodeSegmentSourcePerKeyLockTest {

    private static final Grant SLOW = new Grant(
            "https://store.example/slow", Instant.EPOCH.plus(Duration.ofMinutes(1)));
    private static final Grant FAST = new Grant(
            "https://store.example/fast", Instant.EPOCH.plus(Duration.ofMinutes(1)));

    @Test
    void aSlowGETForOneKeyDoesNotBlockAnotherKey() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch fastDone = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SegmentSource delegate = grant -> {
            if (grant.equals(SLOW)) {
                slowEntered.countDown();
                try {
                    releaseSlow.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("slow fetch interrupted", interrupted);
                }
            }
            return grant.url().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        };
        NodeSegmentSource source = new NodeSegmentSource(delegate, 1L << 20);

        Thread slow = Thread.ofVirtual().start(() -> fetch(source, SLOW, failure));
        assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread fast = Thread.ofVirtual().start(() -> {
            fetch(source, FAST, failure);
            fastDone.countDown();
        });
        try {
            assertThat(fastDone.await(500, TimeUnit.MILLISECONDS))
                    .as("a slow GET for one key must not hold the node-wide cache lock")
                    .isTrue();
        } finally {
            releaseSlow.countDown();
        }
        slow.join(5_000);
        fast.join(5_000);
        assertThat(slow.isAlive()).isFalse();
        assertThat(fast.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
    }

    @Test
    void concurrentFetchesForOneKeyShareOneInFlightGET() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondReturned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger delegateCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        SegmentSource delegate = grant -> {
            if (delegateCalls.incrementAndGet() == 1) {
                firstEntered.countDown();
                try {
                    releaseFirst.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("first fetch interrupted", interrupted);
                }
            }
            return grant.url().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        };
        NodeSegmentSource source = new NodeSegmentSource(delegate, 1L << 20);

        Thread first = Thread.ofVirtual().start(() -> fetch(source, SLOW, failure));
        assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread second = Thread.ofVirtual().start(() -> {
            fetch(source, SLOW, failure);
            secondReturned.countDown();
        });
        try {
            assertThat(secondReturned.await(250, TimeUnit.MILLISECONDS))
                    .as("same-key callers must share the in-flight GET")
                    .isFalse();
        } finally {
            releaseFirst.countDown();
        }
        first.join(5_000);
        second.join(5_000);
        assertThat(first.isAlive()).isFalse();
        assertThat(second.isAlive()).isFalse();
        assertThat(delegateCalls).hasValue(1);
        assertThat(failure.get()).isNull();
    }

    @Test
    void theSubscriptionChannelUsesItsOwnConnectTimeout() {
        NodeSubscriptions built = new BinStorePlugin(
                org.opensearch.common.settings.Settings.builder()
                        .put("node.name", "m8-67-timeout")
                        .put("binstore.ingester.endpoint", "http://127.0.0.1:9")
                        .build()).subscriptions();
        try {
            assertThat(built.channel().connectTimeout())
                    .isEqualTo(NodeSubscriptions.SUBSCRIPTION_CONNECT_TIMEOUT)
                    .isNotEqualTo(NodeSubscriptions.SEGMENT_FETCH_TIMEOUT);
        } finally {
            built.close();
        }
    }

    private static void fetch(NodeSegmentSource source, Grant grant,
            AtomicReference<Throwable> failure) {
        try {
            source.fetch(grant);
        } catch (IOException failed) {
            failure.compareAndSet(null, failed);
        }
    }
}
