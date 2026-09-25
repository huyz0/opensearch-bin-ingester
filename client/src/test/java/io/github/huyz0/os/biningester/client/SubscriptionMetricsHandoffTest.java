// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

class SubscriptionMetricsHandoffTest {

    @Test
    void counterSeedAndListenerInstallationCannotLoseOrDoubleCountAnEvent() throws Exception {
        Class<?> metricsType = Class.forName(
                "io.github.huyz0.os.biningester.client.SubscriptionMetrics");
        Object metrics = metricsType.getConstructor().newInstance();
        Class<?> counterType = Class.forName(
                "io.github.huyz0.os.biningester.client.SubscriptionMetrics$Counter");
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object reconnects = Enum.valueOf((Class<? extends Enum>) counterType,
                "SUBSCRIPTION_RECONNECTS");
        Method increment = metricsType.getMethod("increment", counterType);
        Method count = metricsType.getMethod("count", counterType);
        increment.invoke(metrics, reconnects);
        increment.invoke(metrics, reconnects);

        CountDownLatch seedEntered = new CountDownLatch(1);
        CountDownLatch releaseSeed = new CountDownLatch(1);
        CountDownLatch eventStarted = new CountDownLatch(1);
        CountDownLatch eventFinished = new CountDownLatch(1);
        AtomicBoolean firstDelivery = new AtomicBoolean(true);
        AtomicLong exported = new AtomicLong();
        LongConsumer sink = delta -> {
            exported.addAndGet(delta);
            if (firstDelivery.compareAndSet(true, false)) {
                seedEntered.countDown();
                await(releaseSeed);
            }
        };
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<Object, LongConsumer> listeners = new java.util.HashMap<>();
        listeners.put(reconnects, sink);
        Method install = metricsType.getMethod("install", Map.class);
        Thread registration = Thread.ofPlatform().start(() -> invoke(install, metrics, listeners));
        assertThat(seedEntered.await(5, TimeUnit.SECONDS)).isTrue();

        Thread event = Thread.ofPlatform().start(() -> {
            eventStarted.countDown();
            invoke(increment, metrics, reconnects);
            eventFinished.countDown();
        });
        assertThat(eventStarted.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(eventFinished.await(100, TimeUnit.MILLISECONDS))
                    .as("event update waits until the source total is seeded and listener attached")
                    .isFalse();
        } finally {
            releaseSeed.countDown();
        }
        registration.join(TimeUnit.SECONDS.toMillis(5));
        event.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(registration.isAlive()).isFalse();
        assertThat(event.isAlive()).isFalse();
        assertThat(count.invoke(metrics, reconnects)).isEqualTo(3L);
        assertThat(exported.get()).isEqualTo(3L);
    }

    private static void invoke(Method method, Object receiver, Object... arguments) {
        try {
            method.invoke(receiver, arguments);
        } catch (ReflectiveOperationException failed) {
            throw new AssertionError(failed);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for the counter registration race");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
