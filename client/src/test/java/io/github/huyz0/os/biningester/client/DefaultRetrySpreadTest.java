// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The default first retry spreads a drained ingester's consumers across ten
 * 100 ms windows (M8.16, M8 criterion 13).
 *
 * <p>⚠️ **A DRAIN WAKES EVERY POLL AT ONCE**, so where the consumers land next
 * is decided by this jitter alone. Criterion 13 allows no 100 ms window more
 * than 20% of the reconnects.
 */
class DefaultRetrySpreadTest {

    @Test
    void theDEFAULTFloorsFirstRetrySpreadsOverAFULLSECONDWithNoWindowOverTHEBudget() {
        int draws = 20_000;
        int[] windows = new int[20];
        for (int i = 0; i < draws; i++) {
            long wait = HttpSubscriptionTransport.jitteredMillis(
                    HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR);
            assertThat(wait).as("the jitter is [0.5, 1.5] times the floor")
                    .isBetween(500L, 1500L);
            windows[(int) (wait / 100)]++;
        }
        for (int w = 0; w < windows.length; w++) {
            assertThat(windows[w] / (double) draws)
                    .as("⚠️ THE 100 ms WINDOW AT %d ms CARRIES AT MOST 20%% OF THE RECONNECTS", w * 100)
                    .isLessThanOrEqualTo(0.20);
        }
        long used = java.util.Arrays.stream(windows).filter(n -> n > 0).count();
        assertThat(used).as("⚠️ AND THEY USE AT LEAST TEN WINDOWS, not one or two")
                .isGreaterThanOrEqualTo(10);
    }
}
