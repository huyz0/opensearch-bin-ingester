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
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A pod that does not lead joins the term it learns from the lease, with no
 * commit to make it look (ADR-0081 §1, amended by M13.27n), and the leader's
 * answer is counted against the joiner's zone (M13.27h review round 1, P1).
 */
class IngesterNodeJoinsTest {

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

    /**
     * ⚠️ RETRIED ON A TAKEN PORT (M13.27n review round 1, T6): the endpoint
     * must name the port before the node binds it.
     */
    private IngesterNode startOnAFreePort(String pod, String az) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            try (ServerSocket probe = new ServerSocket(0)) {
                port = probe.getLocalPort();
            }
            try {
                return IngesterNode.start(ServerProperties.parse(settings(pod, az, port)),
                        Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private Map<String, String> settings(String pod, String az, int port) {
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
        settings.put(ServerProperties.ENDPOINT, "http://localhost:" + port);
        settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
        settings.put(ServerProperties.LEASE_RENEW, "PT1S");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
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

    private Optional<Roster.Member> listed(String uid) throws Exception {
        try (InputStream in = leader.assembly().store().get(Roster.key("bins/cluster-a", latest()))) {
            return Roster.decode(in.readAllBytes()).member(uid);
        }
    }

    @Test
    void anIDLEFollowerJoinsTheLeadersTermOnItsOwn() throws Exception {
        leader = startOnAFreePort("pod1", "az-a");
        assertThat(leader.assembly().heldTerm()).isNotNull();

        follower = startOnAFreePort("pod2", "az-b");

        await().atMost(Duration.ofSeconds(20)).until(() -> listed("uid-pod2").isPresent());
        assertThat(listed("uid-pod2").orElseThrow().state()).isEqualTo(Roster.State.ROSTERED);
        assertThat(leader.crossAzBytes().crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .as("the JOINED sent to az-b").isPositive();
        assertThat(follower.crossAzBytes().crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .as("the JOIN sent to az-a").isPositive();
    }
}
