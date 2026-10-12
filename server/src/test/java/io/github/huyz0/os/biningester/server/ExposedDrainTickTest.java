// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The drain is woken every replica timeout as well as by answers (M13.27y):
 * a waiting REPLICA is re-sent while no COMMIT or CONFIRM arrives, and the
 * ticks stop when the node does.
 */
@Timeout(30)
class ExposedDrainTickTest {

    @Test
    void TICKSWakeTheDrainWithNoAnswer() throws Exception {
        CountDownLatch drained = new CountDownLatch(3);
        ExposedDrain drain = new ExposedDrain(drained::countDown);

        try (ExposedDrain.Ticks ticks = drain.tickEvery(Duration.ofMillis(5))) {
            assertThat(drained.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(ticks.running()).isTrue();
        }
    }

    @Test
    void CLOSINGTheTicksStopsThem() throws Exception {
        ExposedDrain drain = new ExposedDrain(() -> { });
        ExposedDrain.Ticks ticks = drain.tickEvery(Duration.ofMillis(5));

        ticks.close();

        assertThat(ticks.running()).as("the ticking thread has ended").isFalse();
    }
}
