// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The {@code EndpointSlice} watch as configuration (M8.13, NFR-9).
 */
class MembershipSettingsTest {

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

    @Test
    void noAPIServerMeansNOWatchWhichIsTheBehaviourBEFOREM813() {
        assertThat(ServerProperties.parse(minimal()).membership()).isEmpty();
    }

    @Test
    void aCOMPLETEWatchConfigurationIsPARSED() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.MEMBERSHIP_API, "https://kubernetes.default.svc");
        settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");
        settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");
        settings.put(ServerProperties.MEMBERSHIP_TOKEN_FILE, "/var/run/token");

        assertThat(ServerProperties.parse(settings).membership()).contains(new MembershipConfig(
                "https://kubernetes.default.svc", "ingest", "ingester",
                Optional.of("/var/run/token")));
    }

    @Test
    void anAPIServerWithoutANAMESPACEOrSERVICEIsREFUSEDNamingTheKey() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.MEMBERSHIP_API, "https://kubernetes.default.svc");
        settings.put(ServerProperties.MEMBERSHIP_SERVICE, "ingester");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MEMBERSHIP_NAMESPACE);
    }

    @Test
    void halfAWATCHWithNoAPIServerIsREFUSEDNotIGNORED() {
        // ⚠️ An operator who wrote a namespace meant to turn the watch on; a
        // node that silently ran without it would fail over at the TTL while
        // the manifest said otherwise.
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.MEMBERSHIP_NAMESPACE, "ingest");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MEMBERSHIP_API);
    }
}
