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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Millisecond visibility, whichever pods write (M13.32d; SPEC criterion 14,
 * ADR-0081 §6): two writers on one partition -- the leader and a follower in
 * another zone -- at {@code q = 3}, and a consumer on a third pod receives
 * each batch from the leader's publication, in offset order and with none
 * missing, under its fast key, before its segment is uploaded and without a
 * catch-up -- and a catch-up asked of that third pod, a follower, repairs a
 * gap from the leader's journal before the upload.
 *
 * <p>⚠️ ZERO ADDED WAITS ON AN INJECTED CLOCK: every node runs on one clock,
 * halted once the fleet has formed and the consumer is pushed its first fast
 * batch, so no measured delivery can wait on a timer -- {@code flush_timer},
 * {@code min_upload_interval} or any other -- and every batch still arrives.
 * (Halted from the start, no pod ever joins the roster.)
 * ⚠️ The replicas' completion order is the fleet's own, not injected:
 * exposure in frontier order whatever the order is pinned by the leader's
 * unit tests.
 */
@org.junit.jupiter.api.Timeout(value = 150, unit = java.util.concurrent.TimeUnit.SECONDS,
        threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
class FastVisibilityTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));
    private final HaltableClock clock = new HaltableClock();

    /** The system's time until {@link #halt}, then that instant for good. */
    private static final class HaltableClock extends Clock {
        private volatile Instant halted;

        void halt() {
            halted = Instant.now();
        }

        @Override
        public Instant instant() {
            Instant at = halted;
            return at != null ? at : Instant.now();
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            throw new UnsupportedOperationException();
        }
    }

    @TempDir
    Path dir;

    private final List<IngesterNode> nodes = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        for (IngesterNode node : nodes.reversed()) {
            node.close();
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
                IngesterNode node = IngesterNode.start(ServerProperties.parse(settings), clock);
                nodes.add(node);
                return node;
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private Roster latestRoster(IngesterNode node) throws Exception {
        var store = node.assembly().store();
        long latest;
        try (InputStream in = store.get(Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = store.get(Roster.key("bins/cluster-a", latest))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    private static List<SegmentRecord> batch(String writer, int i) {
        return List.of(new SegmentRecord(writer + "-" + i + "a", OpType.INDEX,
                OptionalLong.empty(), new byte[] {1}), new SegmentRecord(writer + "-" + i + "b",
                OpType.INDEX, OptionalLong.empty(), new byte[] {2}));
    }

    @Test
    void aCONSUMEROnAThirdPodGetsBothWritersBatchesInOffsetOrderBeforeTheUpload()
            throws Exception {
        IngesterNode leader = startOnAFreePort("pod1", "az-a");
        IngesterNode writer = startOnAFreePort("pod2", "az-b");
        IngesterNode third = startOnAFreePort("pod3", "az-c");
        // ⚠️ a minute's flush_timer, and a clock that never reaches it.
        IndexRegistration orders = new IndexRegistration(UUID_B64, "orders", List.of(),
                1, 1, 1, 1).withFastSettings(60_000, true, 3);
        for (IngesterNode node : nodes) {
            node.assembly().catalog().register(orders);
        }
        RunKey stream = RunKey.ofIndexUuid(UUID_B64, 0);
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Roster r = latestRoster(leader);
            return r.member("uid-pod2").isPresent() && r.member("uid-pod3").isPresent()
                    && r.termRecord().stream()
                            .anyMatch(t -> t.walQuorum().containsKey(stream.indexId()));
        });
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        third.assembly().hub().subscribe(stream, pushes -> {
            pushed.addAll(pushes);
            return (buffer, offset, length) -> { };
        });
        // ⚠️ the third pod's INTEREST goes on its drain's tick: a write or
        // two may be exposed before it, which a catch-up would repair. Measured
        // from the first fast batch it is pushed.
        int[] warm = {0};
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            leader.assembly().ingest().append(PRODUCER, "orders", 0,
                    batch("warm", warm[0]++)::forEach);
            return pushed.stream().anyMatch(p -> p.segmentKey().contains("/fast/"));
        });
        long from = pushed.stream().filter(p -> p.segmentKey().contains("/fast/"))
                .mapToLong(p -> p.firstOffset() + p.recordCount()).max().orElseThrow();
        // the warm-up's own tail: its last writes may still be on their way.
        await().atMost(Duration.ofSeconds(30)).until(() -> pushed.stream()
                .anyMatch(p -> p.firstOffset() + p.recordCount() == 2L * warm[0]));
        long start = 2L * warm[0];
        clock.halt();
        int perWriter = 10;

        ExecutorService writers = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (IngesterNode node : List.of(leader, writer)) {
                String name = node == leader ? "leader" : "writer";
                done.add(writers.submit(() -> {
                    for (int i = 0; i < perWriter; i++) {
                        node.assembly().ingest().append(PRODUCER, "orders", 0,
                                batch(name, i)::forEach);
                    }
                    return null;
                }));
            }
            for (Future<?> each : done) {
                each.get();
            }
        } finally {
            writers.shutdownNow();
        }
        long end = start + 2L * 2 * perWriter;
        await().atMost(Duration.ofSeconds(30)).until(() -> pushed.stream()
                .anyMatch(p -> p.firstOffset() + p.recordCount() == end));

        List<SubscriptionHub.Push> measured = pushed.stream()
                .filter(p -> p.firstOffset() >= start).toList();
        assertThat(from).as("interest established before the measured writes")
                .isLessThanOrEqualTo(start);
        assertThat(measured).as("each under its fast key: published, not uploaded")
                .allSatisfy(p -> assertThat(p.segmentKey()).contains("/fast/"));
        long next = start;
        for (SubscriptionHub.Push p : measured) {
            assertThat(p.firstOffset()).as("in offset order, none missing").isEqualTo(next);
            next = p.firstOffset() + p.recordCount();
        }
        assertThat(next).isEqualTo(end);

        // ⚠️ AND A GAP REPAIRED BEFORE THE UPLOAD, through the consumer's own
        // pod -- a follower, which forwards it to the leader (M13.32i), whose
        // journal serves past its chain (M13.32c); the clock still halted.
        List<io.github.huyz0.os.biningester.format.SubscriptionEvent> caught =
                catchUp(third, stream);
        long repaired = 0;
        for (var e : caught) {
            assertThat(e.firstOffset()).as("the catch-up in offset order, none missing")
                    .isEqualTo(repaired);
            repaired = e.firstOffset() + e.recordCount();
        }
        assertThat(repaired).as("through the last write").isEqualTo(end);
        assertThat(caught.getLast().segmentKey()).as("from the journal, not uploaded")
                .contains("/fast/");
    }

    private static List<io.github.huyz0.os.biningester.format.SubscriptionEvent> catchUp(
            IngesterNode node, RunKey stream) throws Exception {
        UUID request = new UUID(4, 4);
        String route = io.github.huyz0.os.biningester.http.CatchUpService.PATH
                .replace("{indexUuid}", stream.indexId().toString())
                .replace("{partition}", Integer.toString(stream.partitionId()));
        var answer = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:"
                        + node.port() + route)).timeout(Duration.ofSeconds(30))
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(
                                new io.github.huyz0.os.biningester.format.CatchUpRequestFrame(
                                        request, List.of(new io.github.huyz0.os.biningester
                                                .format.CatchUpRequestFrame.Stream(stream, 0)))
                                        .encode())).build(),
                java.net.http.HttpResponse.BodyHandlers.ofInputStream());
        assertThat(answer.statusCode()).isEqualTo(200);
        List<io.github.huyz0.os.biningester.format.SubscriptionEvent> events = new ArrayList<>();
        try (InputStream in = answer.body()) {
            byte[] frame;
            while ((frame = io.github.huyz0.os.biningester.client.StreamFraming.readFrame(in))
                    != null) {
                try {
                    events.add(io.github.huyz0.os.biningester.format.CatchUpEventFrame
                            .decode(frame).event());
                } catch (java.io.IOException notAnEvent) {
                    io.github.huyz0.os.biningester.format.CatchUpEndFrame.decode(frame);
                }
            }
        }
        return events;
    }
}
