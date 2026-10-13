// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
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
 * A node wires publication (M13.30d review round 1, T3): its peer listener
 * routes an INTEREST to the term it leads, and its drain publishes what a
 * fast write's exposure owes the interested pod -- here the node itself,
 * which leads, so its own subscriber is handed the entry under its fast key
 * by the drain's publish (M13.30k), which only that wiring makes.
 */
class FastPublishWiringTest {

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
    void aNODETakesAnINTERESTAndPublishesAFastWritesExposure() throws Exception {
        node = startOnAFreePort();
        node.assembly().catalog().register(new IndexRegistration(FAST_UUID, "orders",
                List.of(), 1, 1, 1, 1).withFastSettings(250, true, 1));
        RunKey stream = RunKey.ofIndexUuid(FAST_UUID, 0);
        await().atMost(Duration.ofSeconds(20)).until(() -> recorded(stream.indexId()));
        long epoch = LocalSequencer.underneath(node.assembly().heldTerm()).orElseThrow().epoch();

        FastFrame.Body answer = FastFrame.decode(new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked()).exchange("http://localhost:" + node.peerPort(),
                FastFrame.encode(epoch, "uid-stranger", "uid-pod1",
                        new FastPublishFrame.Interest(List.of(stream), List.of())))).body();
        List<String> keys = new java.util.concurrent.CopyOnWriteArrayList<>();
        node.assembly().hub().subscribe(stream, pushes -> {
            pushes.forEach(p -> keys.add(p.segmentKey()));
            return (buffer, offset, length) -> { };
        });

        // ⚠️ M13.30k, intentional: from a pod not rostered, so it is refused
        // by the INTEREST handler -- reached, not a 501 -- and sets no interest:
        // only the drain's own interest (interestOwn) can make the entry owed
        // here (M13.30k review round 1, T1: from this pod's own UID it set it).
        assertThat(((FastFrame.Refused) answer).reason()).as("routed to the INTEREST handler")
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        // ⚠️ M13.30k, intentional: this pod leads, so what its drain publishes
        // to its own subscriber goes to its own hub, under the entry's fast key,
        // rather than as a PUBLISH frame to itself, which the counters counted.
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            node.assembly().ingest().append(PRODUCER, "orders", 0, List.of(new SegmentRecord(
                    "doc", OpType.INDEX, OptionalLong.empty(), new byte[] {1}))::forEach);
            return keys.stream().anyMatch(k -> k.contains("/fast/"));
        });
    }

    @Test
    void aNODEServesTheFastEntryRouteOnItsPeerListener() throws Exception {
        // ⚠️ M13.30o: registered -- a request naming no pod is the route's 400,
        // where an unrouted path would be a 404.
        node = startOnAFreePort();

        var answer = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:"
                        + node.peerPort() + io.github.huyz0.os.biningester.http.FastEntryService
                                .PATH + "?key=k")).GET().build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());

        assertThat(answer.statusCode()).isEqualTo(400);
    }

    @Test
    void aLEADINGNodeAnswersACatchUpOnItsPeerListenerStatingItsFastTerm() throws Exception {
        // ⚠️ M13.32h: where a follower forwards one (M13.32i).
        node = startOnAFreePort();
        long epoch = LocalSequencer.underneath(node.assembly().heldTerm()).orElseThrow().epoch();
        RunKey stream = RunKey.ofIndexUuid(FAST_UUID, 0);
        UUID request = new UUID(4, 4);

        var answer = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:"
                        + node.peerPort() + "/ctl/catch-up/" + stream.indexId() + "/"
                        + stream.partitionId())).POST(java.net.http.HttpRequest.BodyPublishers
                                .ofByteArray(new io.github.huyz0.os.biningester.format
                                        .CatchUpRequestFrame(request, List.of(
                                                new io.github.huyz0.os.biningester.format
                                                        .CatchUpRequestFrame.Stream(stream, 0)))
                                        .encode())).build(),
                java.net.http.HttpResponse.BodyHandlers.ofByteArray());

        assertThat(answer.statusCode()).isEqualTo(200);
        assertThat(answer.headers().firstValue(
                io.github.huyz0.os.biningester.http.FastEntryService.EPOCH_HEADER))
                .hasValue(Long.toString(epoch));
    }
}
