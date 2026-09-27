// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The in-flight {@code _bulk} budget on the ingest configuration (M10.8,
 * ADR-0074 decision 6): 256 unless named, and never below one.
 */
class IngestConfigAdmissionBudgetTest {

    private static IngestConfig withBudget(int budget) {
        IngestConfig d = IngestConfig.defaults("cluster-a");
        return new IngestConfig(d.intervalFloor(), d.maxSegmentBytes(), d.trustDomain(),
                d.maxQueuedPushBytes(), d.intervalCeiling(), d.fillRatioLowThreshold(),
                d.fillRatioHighThreshold(), d.intervalLengthenDelay(),
                d.intervalShortenDelay(), false, LaneSet.defaults(), budget);
    }

    @Test
    void aBudgetThatAdmitsNothingIsRefused() {
        for (int budget : new int[] {0, -1, Integer.MIN_VALUE}) {
            assertThatThrownBy(() -> withBudget(budget)).as("%d", budget)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxInFlightBulk");
        }
        assertThat(withBudget(1).maxInFlightBulk()).isEqualTo(1);
    }

    @Test
    void theBudgetIs256UnlessNamed() {
        assertThat(IngestConfig.defaults("cluster-a").maxInFlightBulk()).isEqualTo(256);
    }
}
