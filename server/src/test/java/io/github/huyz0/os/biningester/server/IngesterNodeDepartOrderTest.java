// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.Roster;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower departs after its last flush and before its graph -- its leases
 * -- closes, and journals how it went (M13.27p review round 1, P3, T3).
 */
class IngesterNodeDepartOrderTest {

    @TempDir
    Path dir;

    private IngesterNode leader;
    private IngesterNode follower;

    @AfterEach
    void stop() throws Exception {
        if (follower != null) {
            follower.close();
        }
        if (leader != null) {
            leader.close();
        }
    }

    private IngesterNode start(String pod) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            try (ServerSocket probe = new ServerSocket(0)) {
                port = probe.getLocalPort();
            }
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, pod);
            settings.put(ServerProperties.POD_UID, "uid-" + pod);
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.LEASE_RENEW, "PT1S");
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + port);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private boolean listed(String uid) throws Exception {
        long latest;
        try (InputStream in = leader.assembly().store().get(Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = leader.assembly().store().get(Roster.key("bins/cluster-a",
                latest))) {
            return Roster.decode(in.readAllBytes()).member(uid).isPresent();
        }
    }

    @Test
    void aFOLLOWERDepartsAfterItsLastFlushAndBeforeItsGraphCloses() throws Exception {
        leader = start("pod1");
        follower = start("pod2");
        await().atMost(Duration.ofSeconds(20)).until(() -> listed("uid-pod2"));

        follower.close();
        List<String> events = follower.shutdownJournal();
        follower = null;

        int departed = events.indexOf(FastDeparture.JOURNALED + FastDeparture.Result.DEPARTED);
        assertThat(departed).as("journaled: %s", events).isNotNegative();
        assertThat(events.lastIndexOf(Assembly.FLUSHED)).as("after the last flush")
                .isLessThan(departed);
        assertThat(events.indexOf(Assembly.GRAPH_CLOSED)).as("before the leases are released")
                .isGreaterThan(departed);
    }
}
