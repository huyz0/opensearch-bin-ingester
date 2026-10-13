// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.http.CatchUpService;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower node forwards a consumer's catch-up to the chain's leader
 * (M13.32i) -- where it answered {@code 503}, serving no chain -- and never
 * forwards one that reached its peer path, which a stale view on two pods
 * would otherwise bounce between them.
 */
class CatchUpForwardWiringTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";

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
    private IngesterNode startOnAFreePort(String pod) throws Exception {
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
            settings.put(ServerProperties.POD_AZ, "az-a");
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

    private static int catchUp(int port, String path, RunKey stream) throws Exception {
        String route = path.replace("{indexUuid}", stream.indexId().toString())
                .replace("{partition}", Integer.toString(stream.partitionId()));
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                "http://localhost:" + port + route)).timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(new CatchUpRequestFrame(
                        new UUID(4, 4), List.of(new CatchUpRequestFrame.Stream(stream, 0)))
                        .encode())).build(), HttpResponse.BodyHandlers.ofByteArray())
                .statusCode();
    }

    @Test
    void aFOLLOWERForwardsAConsumersCatchUpButNotOneOnItsPeerPath() throws Exception {
        leader = startOnAFreePort("pod1");
        follower = startOnAFreePort("pod2");
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(250, true, 1);
        leader.assembly().catalog().register(orders);
        follower.assembly().catalog().register(orders);
        RunKey stream = RunKey.ofIndexUuid(UUID_B64, 0);
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster();
            return r.member("uid-pod2").isPresent() && r.termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(stream.indexId()));
        });

        assertThat(catchUp(follower.port(), CatchUpService.PATH, stream))
                .as("forwarded to the leader").isEqualTo(200);
        assertThat(catchUp(follower.peerPort(), CatchUpService.PEER_PATH, stream))
                .as("its peer path answers for itself").isEqualTo(503);
    }
}
