// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The pod's in-flight {@code _bulk} budget (M10.8, ADR-0074 decision 6): read
 * from settings, 256 when absent, and refused at configuration -- with the key
 * named -- when it could admit nothing.
 */
class ServerPropertiesAdmissionTest {

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
    void theBudgetIsReadFromSettingsAndDefaultsTo256() {
        Map<String, String> set = minimal();
        set.put(ServerProperties.MAX_IN_FLIGHT_BULK, " 12 ");

        assertThat(ServerProperties.parse(set).ingest().maxInFlightBulk()).isEqualTo(12);
        assertThat(ServerProperties.parse(minimal()).ingest().maxInFlightBulk()).isEqualTo(256);
    }

    @Test
    void aBudgetThatAdmitsNothingOrIsNotAnIntIsAConfigurationErrorNamingTheKey() {
        for (String bad : new String[] {"0", "-1", "x", "2147483648", "1.5", " "}) {
            Map<String, String> set = minimal();
            set.put(ServerProperties.MAX_IN_FLIGHT_BULK, bad);

            assertThatThrownBy(() -> ServerProperties.parse(set)).as("'%s'", bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.MAX_IN_FLIGHT_BULK);
        }
    }
}
