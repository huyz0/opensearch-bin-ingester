// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.Roster;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two pods under {@code peer.tls = mutual} (ADR-0084; M13.52d): the follower
 * joins the leader's term over mutual TLS, with its own certificate from the
 * domain's CA, and then accepts a write. ⚠️ The 202 alone does not show the
 * forward's channel -- a forward that failed would defer to the inbox and
 * answer the same; the join is what this holds, and IngesterNodePeerClientsTest
 * dials with each client the node built.
 */
class IngesterNodeMutualTest {

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

    private static String fixture(String name) throws Exception {
        return Path.of(IngesterNodeMutualTest.class.getResource("/peer-tls/" + name).toURI())
                .toString();
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
            settings.put(ServerProperties.POD_AZ, az);
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.LEASE_RENEW, "PT1S");
            settings.put(ServerProperties.ENDPOINT, "https://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            settings.put(ServerProperties.PEER_TLS, "mutual");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.PEER_TLS_CERT, fixture(pod + ".pem"));
            settings.put(ServerProperties.PEER_TLS_KEY, fixture(pod + ".key"));
            settings.put(ServerProperties.PEER_TLS_CA, fixture("ca.pem"));
            ServerConfig config = ServerProperties.parse(settings);
            try {
                return IngesterNode.start(config, Clock.systemUTC(), System::nanoTime,
                        Optional::empty, List.of(), Main.peerTls(config.peer(), message -> { }));
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
    void aFOLLOWERJoinsAndForwardsOverMutualTls() throws Exception {
        leader = start("pod1", "az-a");
        follower = start("pod2", "az-b");

        await().atMost(Duration.ofSeconds(20)).until(() -> listed("uid-pod2"));

        WebClient producer = WebClient.builder().baseUri("http://localhost:" + follower.port())
                .readTimeout(Duration.ofSeconds(30)).build();
        try (HttpClientResponse register = producer.post("/ctl/register").submit(
                new io.github.huyz0.os.biningester.format.IndexRegistration(
                        "AAAAAAAAAAAAAAAAAAAAAQ", "logs", List.of(), 1, 1, 1, 1).encode())) {
            assertThat(register.status().code()).as("the premise: registered").isEqualTo(204);
        }
        try (HttpClientResponse bulk = producer.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"doc-0\",\"_version\":1}}\n{\"n\":0}\n")) {
            assertThat(bulk.status().code())
                    .as("accepted by the follower of a term it joined over mutual TLS")
                    .isEqualTo(202);
        }
    }
}
