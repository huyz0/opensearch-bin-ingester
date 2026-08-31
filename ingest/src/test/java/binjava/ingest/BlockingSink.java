// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A subscriber sink that holds the pusher thread inside {@code hub.publish}.
 *
 * <p>⚠️ This is what makes the QUEUE budget observable. {@code DefaultIngest}
 * releases a push's bytes only after {@code publish} returns, so while this sink
 * is held the first push's bytes stay charged — which is the only state in which
 * a second push can be refused for lack of budget. Without it a test sees
 * {@code queuedPushBytes == 0} at every check and pins nothing but a per-push
 * size cap.
 */
final class BlockingSink implements java.util.function.Consumer<SubscriptionHub.Push> {

    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger delivered = new AtomicInteger();

    @Override
    public void accept(SubscriptionHub.Push push) {
        entered.countDown();
        try {
            release.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        delivered.incrementAndGet();
    }

    void awaitEntered() throws InterruptedException {
        if (!entered.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("the pusher never reached the subscriber");
        }
    }

    void release() {
        release.countDown();
    }

    int delivered() {
        return delivered.get();
    }
}
