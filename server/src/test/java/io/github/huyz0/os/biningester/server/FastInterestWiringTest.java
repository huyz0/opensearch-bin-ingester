// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
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
 * A node sends its subscribers' fast streams to its leader (M13.30g): a
 * follower with a subscriber on a fast stream registers INTEREST through its
 * exposure drain, so the leader publishes that stream's exposed entries to it
 * -- counted at the leader as sent or dropped (the follower's PUBLISH handler
 * is M13.30i), which only an INTEREST taken makes.
 *
 * <p>⚠️ The registry is process-wide, so every count here is a delta.
 */
class FastInterestWiringTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));

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
            settings.put(ServerProperties.FAST_JOURNAL_DIR,
                    dir.resolve("journal-" + pod).toString());
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
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

    private static long publishes() {
        return FastMetrics.publishes(true) + FastMetrics.publishes(false);
    }

    @Test
    void aFOLLOWERsSubscriberOnAFastStreamIsPublishedTo() throws Exception {
        leader = startOnAFreePort("pod1", "az-a");
        follower = startOnAFreePort("pod2", "az-b");
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(250, true, 1);
        leader.assembly().catalog().register(orders);
        follower.assembly().catalog().register(orders);
        RunKey stream = RunKey.ofIndexUuid(UUID_B64, 0);
        UUID index = stream.indexId();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster();
            return r.member("uid-pod2").isPresent() && r.termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(index));
        });
        follower.assembly().hub().subscribe(stream, SubscriptionHub.assembling(push -> { }));
        long before = publishes();

        // ⚠️ the follower's drain ticks every replica timeout, so its INTEREST
        // reaches the leader within a few; each write here is another exposure.
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            leader.assembly().ingest().append(PRODUCER, "orders", 0, List.of(new SegmentRecord(
                    "doc", OpType.INDEX, OptionalLong.empty(), new byte[] {1}))::forEach);
            return publishes() - before >= 1;
        });

        assertThat(publishes() - before).isPositive();
    }
}
