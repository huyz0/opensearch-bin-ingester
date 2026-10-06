// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The operator's active lane set (M10.6, ADR-0074, M10 criterion 8): read
 * from settings, and refused at configuration -- with the key named -- when
 * NFR-1's lane term could not hold it.
 */
class ServerPropertiesLaneTest {

    private static Map<String, String> minimal() {
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
        return settings;
    }

    @Test
    void theActiveSetIsReadFromSettingsAndDefaultsToMinusTwoToTwo() {
        Map<String, String> set = minimal();
        set.put(ServerProperties.LANES_ACTIVE, "-1,0,1");

        assertThat(ServerProperties.parse(set).ingest().lanes().contains((byte) 1)).isTrue();
        assertThat(ServerProperties.parse(set).ingest().lanes().contains((byte) 2)).isFalse();
        assertThat(ServerProperties.parse(minimal()).ingest().lanes().contains((byte) 2))
                .isTrue();
    }

    @Test
    void aSetTheCostBoundCannotHoldIsAConfigurationErrorNamingTheKey() {
        for (String bad : new String[] {"0,3", "-8,-7,-6,-5,-4,-3,-2,-1,0", "1,2", "0,x"}) {
            Map<String, String> set = minimal();
            set.put(ServerProperties.LANES_ACTIVE, bad);

            assertThatThrownBy(() -> ServerProperties.parse(set)).as(bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.LANES_ACTIVE);
        }
    }
}
