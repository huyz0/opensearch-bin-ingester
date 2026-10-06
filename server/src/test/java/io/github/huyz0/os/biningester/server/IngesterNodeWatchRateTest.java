// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
 * A follower's watch reads the lease about once per renew interval, never
 * faster (ADR-0081 §1, amended by M13.27n; its review round 3, T9): the node
 * starts the watch with the configured interval, bounded here from above.
 */
class IngesterNodeWatchRateTest {

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

    @Test
    void aFOLLOWERReadsTheLeaseAboutOncePerRenewInterval() throws Exception {
        leader = start("pod1");
        long started = System.nanoTime();
        follower = start("pod2");

        await().atMost(Duration.ofSeconds(20)).until(() -> follower.fastPeer().leaseReads() >= 3);
        long reads = follower.fastPeer().leaseReads();
        long elapsedSeconds = Duration.ofNanos(System.nanoTime() - started).toSeconds();

        assertThat(reads).as("one read per 1 s interval, %d s in", elapsedSeconds)
                .isLessThanOrEqualTo(elapsedSeconds + 2);
    }
}
