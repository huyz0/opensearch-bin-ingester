// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The watch stops before readiness is withdrawn and the door closes -- only a
 * ready pod joins (ADR-0081 §1; M13.27n review round 1, P2; round 2, T8).
 */
class IngesterNodeWatchOrderTest {

    @Test
    void theWATCHStopsBeforeReadinessFailsAndTheListenerStops() throws Exception {
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
        IngesterNode node = IngesterNode.start(ServerProperties.parse(settings),
                Clock.systemUTC());

        node.close();

        List<String> events = node.shutdownJournal();
        int watch = events.indexOf(FastPeer.WATCH_STOPPED);
        assertThat(watch).as("journaled").isNotNegative();
        assertThat(watch).isLessThan(events.indexOf(
                io.github.huyz0.os.biningester.http.DrainGate.READINESS_FAILED));
        assertThat(watch).isLessThan(events.indexOf(FrontDoor.LISTENER_STOPPED));
    }
}
