// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The leader's EXPOSED sends run in the background, one drain at a time
 * (M13.27w): a COMMIT or CONFIRM answer wakes it, and wakes during a drain
 * coalesce into one more -- never a thread per answer.
 */
@Timeout(30)
class ExposedDrainTest {

    private static void awaitIdle(ExposedDrain drain) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!drain.idle()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the drain never went idle");
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aWAKEDrainsInTheBackground() throws Exception {
        CountDownLatch drained = new CountDownLatch(1);
        ExposedDrain drain = new ExposedDrain(drained::countDown);

        drain.wake();

        assertThat(drained.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void WAKESDuringADrainCoalesceIntoOneMoreNeverConcurrent() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger mostInside = new AtomicInteger();
        ExposedDrain drain = new ExposedDrain(() -> {
            mostInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
            if (runs.incrementAndGet() == 1) {
                entered.countDown();
                try {
                    open.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                second.countDown();
            }
            inside.decrementAndGet();
        });

        drain.wake();
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        drain.wake();
        drain.wake();
        drain.wake();
        open.countDown();

        assertThat(second.await(10, TimeUnit.SECONDS)).as("the wakes are not lost").isTrue();
        awaitIdle(drain);
        assertThat(runs.get()).as("three wakes during one drain: one more").isEqualTo(2);
        assertThat(mostInside.get()).as("never two at once").isEqualTo(1);
    }

    @Test
    void aDRAINThatThrowsDoesNotStopTheNext() throws Exception {
        CountDownLatch second = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();
        ExposedDrain drain = new ExposedDrain(() -> {
            if (runs.incrementAndGet() == 1) {
                throw new IllegalStateException("one bad drain");
            }
            second.countDown();
        });

        drain.wake();
        awaitIdle(drain);
        assertThat(runs.get()).isEqualTo(1);
        drain.wake();

        assertThat(second.await(10, TimeUnit.SECONDS)).isTrue();
    }
}
