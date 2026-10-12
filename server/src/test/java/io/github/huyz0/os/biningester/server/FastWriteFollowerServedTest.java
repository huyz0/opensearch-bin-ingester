// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The fast write across two nodes (M13.85; ADR-0081 §2): a follower in
 * another zone writes a {@code wal_quorum = 2} index through the leader of the
 * term it joined -- its own copy required, journaled with its offsets and
 * CONFIRMed, so the two zones complete the quorum -- and is answered once the
 * leader's EXPOSED reaches it.
 */
class FastWriteFollowerServedTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));

    @TempDir
    Path dir;

    private IngesterNode leader;
    private IngesterNode follower;
    private IngesterNode third;

    @AfterEach
    void stop() throws Exception {
        if (third != null) {
            third.close();
        }
        if (follower != null) {
            follower.close();
        }
        if (leader != null) {
            leader.close();
        }
    }

    /** ⚠️ The endpoint names the peer port before the node binds it: retried on a taken one. */
    private IngesterNode startOnAFreePort(String pod, String az) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            int peerPort;
            try (ServerSocket probe = new ServerSocket(0);
                    ServerSocket peerProbe = new ServerSocket(0)) {
                port = probe.getLocalPort();
                peerPort = peerProbe.getLocalPort();
            }
            try {
                return IngesterNode.start(ServerProperties.parse(settings(pod, az, port,
                        peerPort)), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private Map<String, String> settings(String pod, String az, int port, int peerPort) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, pod);
        settings.put(ServerProperties.POD_UID, "uid-" + pod);
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
        settings.put(ServerProperties.POD_AZ, az);
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "local-fs");
        settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
        settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
        settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
        settings.put(ServerProperties.LEASE_RENEW, "PT1S");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal-" + pod).toString());
        return settings;
    }

    private Roster latestRoster() throws Exception {
        var store = leader.assembly().store();
        long latest;
        try (InputStream in = store.get(Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = store.get(Roster.key("bins/cluster-a", latest))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    private static List<SegmentRecord> docs(int n) {
        List<SegmentRecord> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(new SegmentRecord("doc-" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[] {(byte) i}));
        }
        return docs;
    }

    @Test
    void aFOLLOWERsAppendIsExposedOnTwoZonesCopiesAndAnsweredAtItsOffsets() throws Exception {
        leader = startOnAFreePort("pod1", "az-a");
        follower = startOnAFreePort("pod2", "az-b");
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(250, true, 2);
        leader.assembly().catalog().register(orders);
        follower.assembly().catalog().register(orders);
        UUID index = RunKey.ofIndexUuid(UUID_B64, 0).indexId();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster();
            return r.member("uid-pod2").isPresent() && r.termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(index));
        });

        AppendResult got = follower.assembly().ingest().append(PRODUCER, "orders", 0,
                docs(2)::forEach);

        assertThat(got).isEqualTo(new AppendResult(2, 0, 1));
        assertThat(follower.fastDisk().journal().orElseThrow().held())
                .as("the writer's copy, required in another zone at q = 2")
                .singleElement().satisfies(e -> assertThat(e.firstOffset()).isZero());
        assertThat(leader.fastDisk().journal().orElseThrow().held())
                .as("the leader holds every entry of its term").hasSize(1);
    }

    @Test
    void aTHIRDZonesCopyArrivesAsAReplicaFromTheLeader() throws Exception {
        leader = startOnAFreePort("pod1", "az-a");
        follower = startOnAFreePort("pod2", "az-b");
        third = startOnAFreePort("pod3", "az-c");
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(250, true, 3);
        leader.assembly().catalog().register(orders);
        follower.assembly().catalog().register(orders);
        UUID index = RunKey.ofIndexUuid(UUID_B64, 0).indexId();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster();
            return r.member("uid-pod2").isPresent() && r.member("uid-pod3").isPresent()
                    && r.termRecord().stream().anyMatch(t -> t.walQuorum().containsKey(index));
        });

        AppendResult got = follower.assembly().ingest().append(PRODUCER, "orders", 0,
                docs(1)::forEach);

        assertThat(got).isEqualTo(new AppendResult(1, 0, 0));
        assertThat(third.fastDisk().journal().orElseThrow().held())
                .as("q = 3: the leader sent az-c its copy").hasSize(1);
    }
}
