// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

/**
 * {@code flush_timer} is read strictly (M13.23 review round 3, P4).
 *
 * <p>⚠️ OPENSEARCH'S OWN PARSER IS LENIENT IN BOTH DIRECTIONS: it accepts
 * {@code 5S} and {@code " 5s "}, truncates {@code 1500micros} to 1 ms, and
 * refuses the positive {@code 999micros} as zero -- each measured against
 * opensearch-common 3.8.0 in review.
 */
class FastSettingsTimerTest {

    private static final IndexRegistration BASE =
            IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "orders", 1);

    private static long millis(String timer) {
        return FastSettings.apply(BASE, Settings.builder().put(FastSettings.FLUSH_TIMER, timer)
                .build()).flushTimerMillis();
    }

    @Test
    void wholeMILLISECONDSSecondsMinutesAndHoursAreRead() {
        assertThat(millis("250ms")).isEqualTo(250);
        assertThat(millis("2s")).isEqualTo(2_000);
        assertThat(millis("1m")).isEqualTo(60_000);
        assertThat(millis("1h")).isEqualTo(3_600_000);
    }

    @Test
    void anythingELSEIsRefusedNamingTheSetting() {
        for (String timer : new String[] {"5S", " 5s", "5s ", "1500micros", "999micros", "1.5s",
                "05s", "5"}) {
            Settings settings = Settings.builder().put(FastSettings.FLUSH_TIMER, timer).build();

            assertThatThrownBy(() -> FastSettings.apply(BASE, settings))
                    .as("flush_timer [%s]", timer)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("index.ingestion_source.param.flush_timer");
        }
    }
}
