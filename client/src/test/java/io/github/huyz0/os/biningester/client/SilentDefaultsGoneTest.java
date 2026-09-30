// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * M13.6a and M13.6c (M12 harvest R5): nothing in this module quietly supplies
 * what its caller should say. `ConsumerDeliveryQueues`' two-argument constructor
 * defaulted the catch-up lane to never backing off, so a queue built through
 * it lost M12.26's quantum hand-over without saying so.
 */
class SilentDefaultsGoneTest {

    @Test
    void everyConsumerDeliveryQueuesConstructorIsToldTheCatchUpBackoff() {
        List<Constructor<?>> silent = Arrays.stream(
                        ConsumerDeliveryQueues.class.getDeclaredConstructors())
                .filter(c -> !Arrays.asList(c.getParameterTypes()).contains(BooleanSupplier.class))
                .toList();

        assertThat(silent).as("a ConsumerDeliveryQueues constructor that defaults the backoff")
                .isEmpty();
    }

    /**
     * ⚠️ NO CLOCK-LESS BACKOFF (M13.6c): without a clock a backoff was a debt
     * only waiting paid, so a catch-up lane that yielded its turn on one was
     * never retried while live kept the reader busy (M12.26). `DEFAULT` was
     * clock-less, and every client built on it got that mode silently.
     */
    @Test
    void aFetchRetryAlwaysHasAClockAndThereIsNoDefaultOne() {
        assertThat(Arrays.stream(SegmentFetchRetry.class.getDeclaredConstructors())
                .filter(c -> !Arrays.asList(c.getParameterTypes()).contains(LongSupplier.class))
                .toList()).as("a SegmentFetchRetry constructor without a clock").isEmpty();
        assertThat(Arrays.stream(SegmentFetchRetry.class.getDeclaredFields())
                .filter(f -> Modifier.isStatic(f.getModifiers())
                        && f.getType() == SegmentFetchRetry.class)
                .toList()).as("a static default policy").isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new SegmentFetchRetry(
                Duration.ofSeconds(1), Duration.ofSeconds(2), 2, wait -> { }, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void everyConsumerClientConstructorIsToldItsRetry() {
        assertThat(Arrays.stream(ConsumerClient.class.getConstructors())
                .filter(c -> !Arrays.asList(c.getParameterTypes()).contains(SegmentFetchRetry.class))
                .toList()).as("a ConsumerClient constructor that picks its own retry").isEmpty();
    }
}
