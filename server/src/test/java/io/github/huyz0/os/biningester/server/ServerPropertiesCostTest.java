// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The top-K cost line's interval (M11.5): five minutes when absent, {@code PT0S}
 * off, and a negative or malformed one refused naming the key.
 */
class ServerPropertiesCostTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
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

    @Test
    void theIntervalIsReadDefaultsToFiveMinutesAndZeroTurnsItOff() {
        Map<String, String> set = minimal();
        set.put(ServerProperties.COST_TOP_K_INTERVAL, "PT1M");
        Map<String, String> off = minimal();
        off.put(ServerProperties.COST_TOP_K_INTERVAL, "PT0S");

        assertThat(ServerProperties.parse(set).costTopKInterval()).isEqualTo(Duration.ofMinutes(1));
        assertThat(ServerProperties.parse(minimal()).costTopKInterval())
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(ServerProperties.parse(off).costTopKInterval())
                .as("⚠️ ZERO IS AN ANSWER HERE, where every other duration refuses it")
                .isZero();
    }

    @Test
    void aNegativeOrMalformedIntervalIsAConfigurationErrorNamingTheKey() {
        for (String bad : new String[] {"-PT1M", "5m", " "}) {
            Map<String, String> set = minimal();
            set.put(ServerProperties.COST_TOP_K_INTERVAL, bad);

            assertThatThrownBy(() -> ServerProperties.parse(set)).as("'%s'", bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.COST_TOP_K_INTERVAL);
        }
    }
}
