// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A node's drain uploads by the catalog's tightest {@code flush_timer}
 * (ADR-0081 §7; M13.31c review round 1, T2): an index whose timer is an hour
 * has its first upload at once, and its second write waits.
 *
 * <p>⚠️ The registry is process-wide, so every count here is a delta.
 */
class FastFlushTimerServedTest {

    private static final String FAST_UUID = "nVzgup36TLqWp7VBBREj1w";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private IngesterNode startOnAFreePort() throws Exception {
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
            settings.put(ServerProperties.POD_ID, "pod1");
            settings.put(ServerProperties.POD_UID, "uid-pod1");
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders");
            settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private boolean recorded(java.util.UUID index) throws Exception {
        var store = node.assembly().store();
        long latest;
        try (InputStream in = store.get(Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = store.get(Roster.key("bins/cluster-a", latest))) {
            return Roster.decode(in.readAllBytes()).termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(index));
        }
    }

    @Test
    void aFASTIndexWhoseTimerIsAnHourHasItsSecondUploadWait() throws Exception {
        node = startOnAFreePort();
        node.assembly().catalog().register(new IndexRegistration(FAST_UUID, "orders",
                List.of(), 1, 1, 1, 1).withFastSettings(3_600_000, true, 1));
        java.util.UUID index = RunKey.ofIndexUuid(FAST_UUID, 0).indexId();
        await().atMost(Duration.ofSeconds(20)).until(() -> recorded(index));
        long uploads = FastMetrics.uploads(true);

        node.assembly().ingest().append(PRODUCER, "orders", 0, List.of(new SegmentRecord(
                "doc-1", OpType.INDEX, OptionalLong.empty(), new byte[] {1}))::forEach);
        await().atMost(Duration.ofSeconds(20))
                .until(() -> FastMetrics.uploads(true) - uploads == 1);
        node.assembly().ingest().append(PRODUCER, "orders", 0, List.of(new SegmentRecord(
                "doc-2", OpType.INDEX, OptionalLong.empty(), new byte[] {2}))::forEach);

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                .until(() -> FastMetrics.uploads(true) - uploads == 1);
    }
}
