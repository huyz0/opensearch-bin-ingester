// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A stopped node's watch has stopped, and a same-zone follower's JOIN is
 * answered as same-zone bytes (M13.27n review round 1, T2, T4, T6).
 */
class IngesterNodeWatchTest {

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

    private Map<String, String> settings(String pod, String az) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, pod);
        settings.put(ServerProperties.POD_UID, "uid-" + pod);
        settings.put(ServerProperties.POD_AZ, az);
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "local-fs");
        settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
        settings.put(ServerProperties.LEASE_RENEW, "PT1S");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    /**
     * ⚠️ RETRIED ON A TAKEN PORT (T6): the endpoint must name the port before
     * the node binds it, so a port another fork took between the probe and the
     * bind is tried again rather than failing the test.
     */
    private IngesterNode startOnAFreePort(Map<String, String> settings) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            try (ServerSocket probe = new ServerSocket(0)) {
                port = probe.getLocalPort();
            }
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + port);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    /**
     * ⚠️ THE TERM LATEST NAMES, never epoch 1 (M13.27n review round 2, T10): a
     * leader whose first start lost its port gave term 1 back and leads 2.
     */
    private long latest() throws Exception {
        try (InputStream in = leader.assembly().store().get(Roster.latestKey("bins/cluster-a"))) {
            return Roster.decodeLatest(in.readAllBytes());
        }
    }

    private boolean listed(String uid) throws Exception {
        try (InputStream in = leader.assembly().store().get(Roster.key("bins/cluster-a", latest()))) {
            return Roster.decode(in.readAllBytes()).member(uid).isPresent();
        }
    }

    @Test
    void aSTOPPEDNodesWatchHasStopped() throws Exception {
        leader = startOnAFreePort(settings("pod1", "az-a"));
        FastPeer peer = leader.fastPeer();
        assertThat(peer.watching()).isTrue();

        leader.close();
        leader = null;

        assertThat(peer.watching()).isFalse();
    }

    @Test
    void aSAMEZoneJoinIsAnsweredAsSameZoneBytes() throws Exception {
        leader = startOnAFreePort(settings("pod1", "az-a"));
        follower = startOnAFreePort(settings("pod2", "az-a"));

        await().atMost(Duration.ofSeconds(20)).until(() -> listed("uid-pod2"));

        assertThat(leader.crossAzBytes().sameAzBytes(CrossAzBytes.Transport.FAST_FRAME))
                .as("the JOINED, to the asker's own zone").isPositive();
        assertThat(leader.crossAzBytes().crossAzBytes(CrossAzBytes.Transport.FAST_FRAME))
                .isZero();
    }
}
