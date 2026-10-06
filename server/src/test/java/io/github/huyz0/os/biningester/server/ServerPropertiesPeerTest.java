// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The peer listener's settings (ADR-0084; M13.52b): {@code peer.tls} and
 * {@code peer.port} required, never defaulted; with {@code mutual} the three
 * PEM files required, with {@code off} refused.
 */
class ServerPropertiesPeerTest {

    private static Map<String, String> settings() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        return settings;
    }

    private static Map<String, String> mutual() {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.PEER_TLS, "mutual");
        settings.put(ServerProperties.ENDPOINT, "https://localhost:9443");
        settings.put(ServerProperties.PEER_PORT, "9443");
        settings.put(ServerProperties.PEER_TLS_CERT, "/etc/peer/tls.crt");
        settings.put(ServerProperties.PEER_TLS_KEY, "/etc/peer/tls.key");
        settings.put(ServerProperties.PEER_TLS_CA, "/etc/peer/ca.crt");
        return settings;
    }

    @Test
    void PEERTLSAndPEERPortAreRequiredNeverDefaulted() {
        for (String key : new String[] {ServerProperties.PEER_TLS, ServerProperties.PEER_PORT}) {
            Map<String, String> without = settings();
            without.remove(key);
            assertThatThrownBy(() -> ServerProperties.parse(without)).as(key)
                    .isInstanceOf(ConfigurationException.class).hasMessageContaining(key);
        }
    }

    @Test
    void PEERTLSIsMutualOrOffAndNothingElse() {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.PEER_TLS, "on");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS).hasMessageContaining("on");
    }

    @Test
    void PEERPortIsAPort() {
        for (String bad : new String[] {"-1", "65536", "x"}) {
            Map<String, String> settings = settings();
            settings.put(ServerProperties.PEER_PORT, bad);
            assertThatThrownBy(() -> ServerProperties.parse(settings)).as(bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.PEER_PORT);
        }
    }

    @Test
    void OFFParsesWithItsPortAndNoFiles() {
        assertThat(ServerProperties.parse(settings()).peer())
                .isEqualTo(new PeerConfig(PeerConfig.Mode.OFF, 0, Optional.empty()));
    }

    @Test
    void MUTUALParsesWithItsThreeFiles() {
        assertThat(ServerProperties.parse(mutual()).peer()).isEqualTo(new PeerConfig(
                PeerConfig.Mode.MUTUAL, 9443, Optional.of(new PeerConfig.Files(
                        "/etc/peer/tls.crt", "/etc/peer/tls.key", "/etc/peer/ca.crt"))));
    }

    @Test
    void MUTUALWithoutAnyOneFileIsRefusedNamingIt() {
        for (String key : new String[] {ServerProperties.PEER_TLS_CERT,
                ServerProperties.PEER_TLS_KEY, ServerProperties.PEER_TLS_CA}) {
            Map<String, String> settings = mutual();
            settings.remove(key);
            assertThatThrownBy(() -> ServerProperties.parse(settings)).as(key)
                    .isInstanceOf(ConfigurationException.class).hasMessageContaining(key);
        }
    }

    @Test
    void OFFWithAFileIsRefusedRatherThanIgnored() {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.PEER_TLS_CA, "/etc/peer/ca.crt");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PEER_TLS_CA)
                .hasMessageContaining("off");
    }
}
