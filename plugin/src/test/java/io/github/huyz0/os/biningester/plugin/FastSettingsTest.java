// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

/**
 * The parse itself (M13.23 review round 2, T3 and T4): every refusal names the
 * index setting an operator typed, and the quorum is matched exactly.
 *
 * <p>⚠️ A REFUSAL THAT NAMES A JAVA FIELD IS ONE AN OPERATOR CANNOT ACT ON.
 * The record also refuses a non-positive timer, so a parser that skipped its
 * own range check would still refuse -- with "flushTimerMillis is never 0",
 * which appears in no setting, document or error an operator has seen.
 */
class FastSettingsTest {

    private static final IndexRegistration BASE =
            IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "orders", 1);

    @Test
    void aNONPositiveTimerIsRefusedNamingTheSetting() {
        for (String timer : new String[] {"0ms", "-1"}) {
            Settings settings = Settings.builder().put(FastSettings.FLUSH_TIMER, timer).build();

            assertThatThrownBy(() -> FastSettings.apply(BASE, settings))
                    .as("flush_timer [%s]", timer)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("index.ingestion_source.param.flush_timer");
        }
    }

    @Test
    void theQUORUMIsExactlyOneTwoOrThree() {
        for (String quorum : new String[] {" 2", "2 ", "03", "+2"}) {
            Settings settings = Settings.builder()
                    .put(FastSettings.WAL, "true").put(FastSettings.WAL_QUORUM, quorum).build();

            assertThatThrownBy(() -> FastSettings.apply(BASE, settings))
                    .as("wal_quorum [%s] is not a value anyone wrote on purpose", quorum)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("index.ingestion_source.param.wal_quorum");
        }
    }
}
