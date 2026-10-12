// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A drain that throws an {@code Error} does not wedge the next (M13.27w
 * review round 1, P1): the count it leaves is reset, so a later wake starts
 * a drain -- never every EXPOSED unsent until a restart.
 */
@Timeout(30)
class ExposedDrainErrorTest {

    @Test
    void aDRAINThatThrowsAnErrorDoesNotWedgeTheNext() throws Exception {
        CountDownLatch second = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();
        ExposedDrain drain = new ExposedDrain(() -> {
            if (runs.incrementAndGet() == 1) {
                throw new AssertionError("one fatal drain");
            }
            second.countDown();
        });

        drain.wake();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!drain.idle()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the drain stayed wedged");
            }
            Thread.onSpinWait();
        }
        drain.wake();

        assertThat(second.await(10, TimeUnit.SECONDS)).isTrue();
    }
}
