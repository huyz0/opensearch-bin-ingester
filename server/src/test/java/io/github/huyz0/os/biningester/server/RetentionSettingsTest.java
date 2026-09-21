// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Retention as configuration (M8.5, FR-9, NFR-13).
 *
 * <p>⚠️ **THE FLOOR's DEFAULT IS A REQUIREMENT, NOT A CHOICE.** NFR-13 sets the
 * consumer outage budget at 6 hours, and M8.4 left a 1-hour placeholder in
 * {@code Assembly} -- a sixth of the agreed budget, which would have deleted
 * data an OpenSearch cluster down for two hours was still entitled to read.
 */
class RetentionSettingsTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    private static Map<String, String> with(String key, String value) {
        Map<String, String> settings = minimal();
        settings.put(key, value);
        return settings;
    }

    @Test
    void theDEFAULTFloorIsNFR13sSIXHours() {
        RetentionConfig retention = ServerProperties.parse(minimal()).retention();

        assertThat(retention.minRetention())
                .as("⚠️ NFR-13: consumer outage tolerance = retention, default 6 h -- as a "
                        + "LITERAL, so a default that drifts is red here rather than in a "
                        + "post-mortem")
                .isEqualTo(Duration.ofHours(6));
        assertThat(retention.copyExpiry())
                .as("⚠️ AND A COPY OUTLIVES THE FLOOR, or it is retired while its data is "
                        + "still inside the outage budget")
                .isGreaterThan(retention.minRetention());
        assertThat(retention.maxRetention()).isGreaterThan(retention.minRetention());
    }

    @Test
    void eachRETENTIONSettingIsCARRIEDThroughToTheConfig() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.RETENTION_MIN, "PT7H");
        settings.put(ServerProperties.RETENTION_MAX, "P3D");
        settings.put(ServerProperties.RETENTION_REPORT_TIMEOUT, "PT2M");
        settings.put(ServerProperties.RETENTION_COPY_EXPIRY, "PT15H");
        settings.put(ServerProperties.RETENTION_PASS_INTERVAL, "PT30S");

        RetentionConfig retention = ServerProperties.parse(settings).retention();

        assertThat(retention.minRetention()).isEqualTo(Duration.ofHours(7));
        assertThat(retention.maxRetention()).isEqualTo(Duration.ofDays(3));
        assertThat(retention.reportTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(retention.copyExpiry()).isEqualTo(Duration.ofHours(15));
        assertThat(retention.passInterval()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void aCEILINGAtOrBelowTheFLOORIsREFUSEDNamingBothKeys() {
        // ⚠️ IT WOULD DELETE EVERYTHING THE MOMENT IT PASSED THE FLOOR, read or
        // not, alarming each time.
        Map<String, String> settings = with(ServerProperties.RETENTION_MAX, "PT6H");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.RETENTION_MAX)
                .hasMessageContaining(ServerProperties.RETENTION_MIN);
    }

    @Test
    void aCOPYExpiryInsideTheFLOORIsREFUSEDNamingBothKeys() {
        // ⚠️ THE CONFIGURATION THAT QUIETLY TURNS THE OUTAGE BUDGET OFF: a copy
        // retired inside the window loses data it is still entitled to read.
        Map<String, String> settings = with(ServerProperties.RETENTION_COPY_EXPIRY, "PT5H");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.RETENTION_COPY_EXPIRY)
                .hasMessageContaining(ServerProperties.RETENTION_MIN);
    }

    @Test
    void aCOPYExpiryInsideTheREPORTTimeoutIsREFUSED() {
        // ⚠️ OTHERWISE THE UNFRESH STATE IS UNREACHABLE: a copy would be gone
        // before it could be called stale.
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.RETENTION_MIN, "PT1S");
        settings.put(ServerProperties.RETENTION_REPORT_TIMEOUT, "PT1H");
        settings.put(ServerProperties.RETENTION_COPY_EXPIRY, "PT30M");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.RETENTION_REPORT_TIMEOUT);
    }

    @Test
    void aNONPositivePassIntervalIsREFUSED() {
        assertThatThrownBy(() -> ServerProperties.parse(
                with(ServerProperties.RETENTION_PASS_INTERVAL, "PT0S")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.RETENTION_PASS_INTERVAL);
    }
}
