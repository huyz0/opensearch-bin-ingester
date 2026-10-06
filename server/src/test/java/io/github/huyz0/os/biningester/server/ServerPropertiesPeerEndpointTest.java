// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The advertised endpoint speaks what the peer listener does (ADR-0084
 * decision 2; M13.52d): every peer dials it, so a {@code mutual} pod
 * advertising {@code http://} -- or an {@code off} one {@code https://} --
 * would be unreachable by every other.
 */
class ServerPropertiesPeerEndpointTest {

    private static Map<String, String> settings(String tls, String endpoint) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, endpoint);
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, tls);
        settings.put(ServerProperties.PEER_PORT, "9443");
        if (tls.equals("mutual")) {
            settings.put(ServerProperties.PEER_TLS_CERT, "/etc/peer/tls.crt");
            settings.put(ServerProperties.PEER_TLS_KEY, "/etc/peer/tls.key");
            settings.put(ServerProperties.PEER_TLS_CA, "/etc/peer/ca.crt");
        }
        return settings;
    }

    @Test
    void aMUTUALPodAdvertisingHttpIsRefused() {
        assertThatThrownBy(() -> ServerProperties.parse(settings("mutual", "http://pod1:9443")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.ENDPOINT).hasMessageContaining("https://");
    }

    @Test
    void anOFFPodAdvertisingHttpsIsRefused() {
        assertThatThrownBy(() -> ServerProperties.parse(settings("off", "https://pod1:9443")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.ENDPOINT).hasMessageContaining("http://");
    }
}
