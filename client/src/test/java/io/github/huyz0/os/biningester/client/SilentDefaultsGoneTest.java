// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * M13.6a (M12 harvest R5): no constructor of this module quietly supplies what
 * its caller should say. `ConsumerDeliveryQueues`' two-argument constructor
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
}
