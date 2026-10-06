// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.ServerSocket;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A producer port already held refuses the start naming it (M13.52c review
 * round 2, T4): Helidon cannot say which of its two sockets failed, so the
 * refusal names both ports and both keys -- never the peer port alone.
 */
class PeerListenerBindTest {

    @Test
    void aPRODUCERPortAlreadyHeldIsNamedByItsOwnKey() throws Exception {
        try (ServerSocket held = new ServerSocket(0)) {
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, "pod1");
            settings.put(ServerProperties.POD_UID, "uid-pod1");
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "memory");
            settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(held.getLocalPort()));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, "0");
            ServerConfig config = ServerProperties.parse(settings);

            assertThatThrownBy(() -> IngesterNode.start(config, Clock.systemUTC()).close())
                    .hasMessageContaining("port " + held.getLocalPort() + " ("
                            + ServerProperties.HTTP_PORT + ")");
        }
    }
}
