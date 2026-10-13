// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
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
 * A subscriber served proxy in another zone reads a fast batch before its
 * upload (M13.30q; ADR-0081 §6, ADR-0082 §6) -- end to end over two nodes:
 * the follower's INTEREST, the leader's PUBLISH, the follower's hub pushing
 * the subscriber its fast key alone, and its {@code /seg} serving the key from
 * the copy pushed to it (M13.30l) -- or, holding none, from its term's
 * leader's journal (M13.30p), the wiring M13.30l's review left unpinned.
 *
 * <p>⚠️ {@code flush_timer} is a minute, so an entry after the term's first
 * upload stays exposed and un-uploaded while it is read.
 */
class FastProxySubscriberTest {

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

    private HttpResponse<byte[]> seg(IngesterNode node, String key) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                "http://localhost:" + node.port() + SegmentFetchRoute.PATH + "?"
                        + SegmentFetchRoute.KEY_PARAM + "="
                        + URLEncoder.encode(key, StandardCharsets.UTF_8))).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static List<String> ids(byte[] segment, RunKey stream) throws Exception {
        SegmentReader reader = SegmentReader.open(segment);
        return reader.read(reader.find(stream).orElseThrow()).stream()
                .map(SegmentRecord::id).toList();
    }

    @Test
    void aSUBSCRIBERInAnotherZoneReadsAFastBatchBeforeItsUpload() throws Exception {
        leader = startOnAFreePort("pod1", "az-a");
        follower = startOnAFreePort("pod2", "az-b");
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(60_000, true, 1);
        leader.assembly().catalog().register(orders);
        follower.assembly().catalog().register(orders);
        RunKey stream = RunKey.ofIndexUuid(UUID_B64, 0);
        UUID index = stream.indexId();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster();
            return r.member("uid-pod2").isPresent() && r.termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(index));
        });
        List<SubscriptionHub.Push> pushed = new java.util.concurrent.CopyOnWriteArrayList<>();
        follower.assembly().hub().subscribe(stream, new SubscriptionHub.Subscriber() {
            @Override
            public io.github.huyz0.os.biningester.ingest.SegmentSink open(
                    List<SubscriptionHub.Push> pushes) {
                pushed.addAll(pushes);
                return (buffer, offset, length) -> { };
            }

            @Override
            public String az() {
                return "az-c";
            }
        });
        int[] n = {0};

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            leader.assembly().ingest().append(PRODUCER, "orders", 0, List.of(new SegmentRecord(
                    "doc-" + n[0]++, OpType.INDEX, OptionalLong.empty(),
                    new byte[] {1}))::forEach);
            return pushed.stream().anyMatch(p -> p.segmentKey().contains("/fast/"));
        });
        SubscriptionHub.Push push = pushed.stream()
                .filter(p -> p.segmentKey().contains("/fast/")).reduce((a, b) -> b)
                .orElseThrow();

        assertThat(push.via()).as("another zone: the key alone").isEqualTo(FetchMode.PROXY);
        var copies = follower.assembly().defaultIngest().pushedCopies();
        byte[] kept = copies.copy(push.segmentKey()).orElseThrow();
        HttpResponse<byte[]> copy = seg(follower, push.segmentKey());
        assertThat(copy.statusCode()).isEqualTo(200);
        // ⚠️ its review round 1, T1: the copy's own bytes -- the leader's
        // journal would answer the same records in a segment created later.
        assertThat(copy.body()).as("from the copy pushed to it").isEqualTo(kept);
        List<String> pushedIds = ids(copy.body(), stream);
        assertThat(pushedIds).hasSize(push.recordCount());

        copies.release(stream, Long.MAX_VALUE);
        // ⚠️ its review round 1, T2: gone, so the next read cannot be the copy.
        assertThat(copies.copy(push.segmentKey())).isEmpty();
        HttpResponse<byte[]> forwarded = seg(follower, push.segmentKey());

        assertThat(forwarded.statusCode()).as("holding none, from the leader's journal")
                .isEqualTo(200);
        assertThat(ids(forwarded.body(), stream)).isEqualTo(pushedIds);
    }
}
