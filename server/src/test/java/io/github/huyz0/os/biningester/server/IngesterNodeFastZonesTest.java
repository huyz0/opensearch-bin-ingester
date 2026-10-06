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
 * Two started nodes in two zones count every fast frame at its peer's zone
 * (M13.64 review round 1, T1): the follower's JOIN and its departure's
 * DEPART and HELD, and the leader's answers -- none of them unknown.
 */
class IngesterNodeFastZonesTest {

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

    private IngesterNode start(String pod, String az) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            int peerPort;
            try (ServerSocket probe = new ServerSocket(0);
                        ServerSocket peerProbe = new ServerSocket(0)) {
                port = probe.getLocalPort();
                peerPort = peerProbe.getLocalPort();
            }
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, pod);
            settings.put(ServerProperties.POD_UID, "uid-" + pod);
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, "0");
            settings.put(ServerProperties.POD_AZ, az);
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.LEASE_RENEW, "PT1S");
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
        try (InputStream in = leader.assembly().store().get(
                Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = leader.assembly().store().get(
                Roster.key("bins/cluster-a", latest))) {
            return Roster.decode(in.readAllBytes()).member(uid).isPresent();
        }
    }

    @Test
    void aFOLLOWERsJoinAndDepartureAreCountedAtTheLeadersZone() throws Exception {
        leader = start("pod1", "az-a");
        follower = start("pod2", "az-b");
        await().atMost(Duration.ofSeconds(20)).until(() -> listed("uid-pod2"));
        CrossAzBytes sent = follower.crossAzBytes();
        assertThat(sent.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .as("the premise: the JOIN crossed").isPositive();
        assertThat(sent.unknownPeerBytes()).as("the JOIN, at az-a").isZero();
        assertThat(leader.crossAzBytes().unknownPeerBytes()).as("the JOINED, at az-b").isZero();
        long joined = sent.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL);

        follower.close();
        follower = null;

        assertThat(sent.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .as("the premise: the departure sent frames").isGreaterThan(joined);
        assertThat(sent.unknownPeerBytes()).as("the DEPART and HELD, at az-a").isZero();
    }
}
