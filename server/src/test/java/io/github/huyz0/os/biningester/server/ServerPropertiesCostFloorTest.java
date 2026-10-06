// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The top-K cost interval has a floor of one second (M11.5, review round 1): a
 * sub-millisecond interval is due on every scheduler wake and floods the log.
 */
class ServerPropertiesCostFloorTest {

    private static Map<String, String> with(String interval) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.COST_TOP_K_INTERVAL, interval);
        return settings;
    }

    @Test
    void anIntervalUnderOneSecondIsRefusedNamingTheKeyAndOneSecondIsAccepted() {
        for (String bad : new String[] {"PT0.0005S", "PT0.999S"}) {
            assertThatThrownBy(() -> ServerProperties.parse(with(bad))).as("'%s'", bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.COST_TOP_K_INTERVAL);
        }
        assertThat(ServerProperties.parse(with("PT1S")).costTopKInterval())
                .isEqualTo(Duration.ofSeconds(1));
    }
}
