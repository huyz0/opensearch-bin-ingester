// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * M12.9 (M11.5 P1b): the top-K cost interval's ceiling, as the server's
 * properties parser applies it; its one-second floor is M11.5's, pinned by
 * {@code ServerPropertiesCostFloorTest}. ⚠️ THE REPORTER's HALF MOVED TO
 * {@code :ingest}'s {@code CostTopKIntervalTest} (M13.14, M12.9 review T2): it
 * exercises nothing of this module.
 */
class CostTopKIntervalBoundsTest {

    @Test
    void anIntervalPastTheCeilingIsAConfigurationErrorNamingTheKey() {
        for (String bad : List.of("P2D", "PT24H0.001S")) {
            Map<String, String> settings = minimal();
            settings.put(ServerProperties.COST_TOP_K_INTERVAL, bad);

            assertThatThrownBy(() -> ServerProperties.parse(settings)).as(bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.COST_TOP_K_INTERVAL);
        }
    }

    /** ⚠️ THE CEILING ITSELF IS TAKEN (M13.14, M12.9 review T1): at most, not below. */
    @Test
    void theParserTakesTheCeilingItself() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.COST_TOP_K_INTERVAL, "PT24H");

        assertThat(ServerProperties.parse(settings).costTopKInterval())
                .isEqualTo(CostTopKReporter.MAX_INTERVAL);
    }

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
}
