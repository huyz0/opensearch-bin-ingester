// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A departing pod's wait for its next RELEASE (M13.31g; ADR-0081 §9): over at
 * once when one already came since the count it holds, and woken by one taken
 * while it waits -- never the whole bound, which here is an hour.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastReleasesTest {

    private static final Duration HOUR = Duration.ofHours(1);

    @Test
    void aRELEASEAlreadyTakenEndsTheWaitAtOnce() throws Exception {
        FastReleases releases = new FastReleases();
        long seen = releases.count();
        releases.released();

        assertThat(releases.awaitAfter(seen, HOUR)).isTrue();
    }

    @Test
    void aRELEASETakenWhileWaitingWakesIt() throws Exception {
        FastReleases releases = new FastReleases();
        long seen = releases.count();
        java.util.concurrent.atomic.AtomicBoolean woken =
                new java.util.concurrent.atomic.AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                woken.set(releases.awaitAfter(seen, HOUR));
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        // ⚠️ PARKED IN THE WAIT before the RELEASE, or this proves nothing.
        while (waiter.getState() != Thread.State.TIMED_WAITING) {
            Thread.onSpinWait();
        }

        releases.released();
        waiter.join();

        assertThat(woken).isTrue();
    }
}
