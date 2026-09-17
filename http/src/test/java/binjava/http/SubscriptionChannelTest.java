// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.Delivery;
import binjava.client.HttpSubscriptionTransport;
import binjava.format.ConsumerProgress;
import binjava.format.FetchMode;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import binjava.ingest.IndexCatalog;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The subscription channel, over a real socket (M8.21, M5.6e, M6.15).
 *
 * <p>⚠️ **THE CHANNEL CARRIES THREE MESSAGES AND EVERY ONE OF THEM HAS BEEN
 * UNWIRED SINCE THE MILESTONE THAT BUILT IT**: a push (M5), a registration
 * (M6.7, whose {@code onReconnect} M6.15 records as called by nothing), and a
 * progress frame (M7.3). What this file pins is that they travel, and what
 * happens when they cannot.
 */
class SubscriptionChannelTest {

    private static final UUID LOGS = UUID.randomUUID();
    private static final RunKey STREAM = new RunKey(LOGS, 3);

    private WebServer server;
    private HttpSubscriptionTransport transport;

    @AfterEach
    void stop() {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private static String uuidToBase64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    private String start(SubscriptionHub hub, IndexCatalog catalog, WatermarkTable watermarks) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new SubscriptionService(hub, catalog, watermarks,
                                Clock.systemUTC())))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private static WatermarkTable table() {
        return new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofHours(2),
                Duration.ofMinutes(30));
    }

    private HttpSubscriptionTransport connect(String endpoint, Runnable onReconnect) {
        transport = new HttpSubscriptionTransport(endpoint, onReconnect,
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5));
        return transport;
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aPUSHReachesTheSUBSCRIBEROverTheSocket() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, () -> { }).subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            publish(hub, "seg-1", 5000, 100);

            await(() -> !got.isEmpty(), "a delivery");
            assertThat(got.get(0).key()).isEqualTo(STREAM);
            assertThat(got.get(0).segmentKey()).isEqualTo("seg-1");
            assertThat(got.get(0).firstOffset())
                    .as("⚠️ THE OFFSET AND THE COUNT ARE THE DELIVERY. A transport that "
                            + "carried the key and dropped these would look like a working "
                            + "subscription and produce a consumer that reads the wrong "
                            + "records for ever")
                    .isEqualTo(5000);
            assertThat(got.get(0).recordCount()).isEqualTo(100);
        }
    }

    @Test
    void aREGISTRATIONReachesTheCATALOG() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        String endpoint = start(new SubscriptionHub(), catalog, table());

        connect(endpoint, () -> { }).register(new IndexRegistration(
                uuidToBase64Url(LOGS), "logs", List.of("logs-alias"), 4, 4, 1, 1));

        assertThat(catalog.resolve("logs")).isPresent();
        assertThat(catalog.resolve("logs-alias"))
                .as("⚠️ THE ALIASES TRAVEL TOO: a registration that lost them leaves every "
                        + "write to an alias refused at `pendingTimeout` (M6.6)")
                .isPresent();
    }

    @Test
    void aPROGRESSFrameReachesTheWATERMARKTable() throws Exception {
        WatermarkTable watermarks = table();
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), watermarks);

        connect(endpoint, () -> { }).report(new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(uuidToBase64Url(LOGS), 3, "alloc-a", 900))));

        assertThat(watermarks.of(STREAM).consumedUpTo())
                .as("⚠️ THE FRAME THAT DECIDES WHAT GC MAY DELETE. A transport that accepted "
                        + "it and sent nothing would leave every watermark frozen while the "
                        + "deployment looked healthy, and the first anyone would hear is "
                        + "data hitting `maxRetention` hours later")
                .isEqualTo(900);
    }

    @Test
    void RECONNECTINGFiresTheCALLBACKM6_15HasBeenWaitingFor() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        java.util.concurrent.atomic.AtomicInteger reconnects =
                new java.util.concurrent.atomic.AtomicInteger();

        try (var ignored = connect(endpoint, reconnects::incrementAndGet)
                .subscribe(STREAM, delivery -> { })) {
            await(() -> reconnects.get() >= 1, "the first connection");

            // ⚠️ THE INGESTER RESTARTS, WHICH IS WHAT A ROLLING DEPLOY IS. Its
            // catalog is in memory (M6.7), so the node that comes back knows no
            // index's shape -- and every routed write to this node's indices is
            // refused when its wait expires, with nothing naming the cause.
            // `IndexRegistrar.onReconnect()` is what recovers that, and until
            // this transport existed nothing called it (M6.15).
            // ⚠️ THE PORT IS READ BEFORE THE STOP: `server.port()` on a
            // stopped server is not the port it had, so rebinding "the same
            // port" would bind a random one and the client would reconnect to
            // nothing for ever.
            int port = server.port();
            server.stop();
            server = WebServer.builder().port(port)
                    .routing(HttpRouting.builder().register(
                            new SubscriptionService(hub, new IndexCatalog(), table(),
                                    Clock.systemUTC())))
                    .build().start();

            await(() -> reconnects.get() >= 2, "a reconnect after the ingester restarted");
        }
    }

    @Test
    void aPUSHBETWEENTwoPollsIsSTILLDelivered() throws Exception {
        // ⚠️ THE DEFECT THAT PRODUCED SERVER-SIDE SESSIONS, MEASURED. The first
        // implementation subscribed to the hub for the duration of ONE POLL, so
        // a push published in the gap between two polls reached nobody: no gap,
        // no error, just records the consumer would never be delivered -- and
        // since a consumer's position is its own (ADR-0005), nothing downstream
        // would have noticed either.
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, () -> { }).subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            // ⚠️ PUBLISHED WHILE THE CONSUMER IS BETWEEN POLLS, which is where
            // the gap was: the handshake poll has returned and the long one has
            // not been issued yet for at least a moment.
            for (int i = 0; i < 5; i++) {
                publish(hub, "seg-gap-" + i, 100 + i, 1);
                Thread.sleep(20);
            }
            await(() -> got.size() >= 5, "every push published between polls");
        }
    }

    @Test
    void anIDLESessionIsSWEPTRatherThanHeldForEver() throws Exception {
        // ⚠️ A NODE THAT RELOCATES A SHARD AWAY ABANDONS A SESSION EVERY TIME.
        // Without a sweep each one holds a hub subscription and a queue for the
        // life of the process. ⚠️ THE EXPIRY IS INJECTED because at ninety
        // seconds this case could only watch a session NOT be swept -- which
        // review MEASURED passing with the sweep deleted.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(), table(),
                Duration.ofMillis(100), Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        String endpoint = "http://localhost:" + server.port();

        var handle = connect(endpoint, () -> { }).subscribe(STREAM, delivery -> { });
        await(() -> service.sessionCount() == 1, "a session");
        handle.close();
        transport.close();
        transport = null;
        Thread.sleep(300);

        // ⚠️ THE SWEEP RUNS ON A POLL, so another consumer's poll is what
        // reclaims an abandoned session -- which is the shape that needs no
        // timer and no clock seam in a request handler.
        var other = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5),
                Duration.ofMillis(50));
        try (var ignored = other.subscribe(new RunKey(UUID.randomUUID(), 0), d -> { })) {
            // ⚠️ WAIT FOR THE SECOND CONSUMER TO HAVE POLLED, then assert the
            // count. Without that wait this case passes the instant after the
            // handle closes -- the abandoned session ALONE is one -- and review
            // MEASURED exactly that: the sweep could be deleted and the
            // assertion still held. ⚠️ AND THE COUNT NEVER REACHES TWO, because
            // the sweep runs BEFORE the new session is created: 1 -> 0 -> 1.
            await(() -> other.reconnects() > 0, "the second consumer's first poll");
            assertThat(service.sessionCount())
                    .as("⚠️ ONE, NOT TWO: the abandoned session is gone. Without the sweep "
                            + "it would still be here, and a node that relocates shards "
                            + "leaks one every time")
                    .isEqualTo(1);
        } finally {
            other.close();
        }
    }

    @Test
    void aBUSYStreamCostsONEPollPerROUNDTripRatherThanOnePerPUSH() throws Exception {
        // ⚠️ THE DRAIN. One poll per push makes a busy stream cost a request
        // per segment -- which is what long-polling exists to avoid, and what
        // an ingester serving a thousand consumers would feel first.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(),
                table(), Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        String endpoint = "http://localhost:" + server.port();
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, () -> { }).subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            long pollsBefore = service.pollCount();
            for (int i = 0; i < 6; i++) {
                publish(hub, "seg-burst-" + i, 200 + i, 1);
            }
            await(() -> got.size() >= 6, "the whole burst");

            assertThat(service.pollCount() - pollsBefore)
                    .as("⚠️ SIX PUSHES, NOT SIX POLLS. The answer carries everything already "
                            + "queued, so a burst costs one round trip plus whatever arrived "
                            + "after it")
                    .isLessThan(6);
        }
    }

    @Test
    void theRECONNECTCallbackFiresONCEPerCONNECTIONNotOncePerPOLL() throws Exception {
        // ⚠️ `onReconnect` RE-REGISTERS EVERY INDEX ON THE NODE. Firing it per
        // poll turns a quiet stream into a message storm -- and at the 25 s
        // default a test sees one poll, which is why the wait is injected here.
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        java.util.concurrent.atomic.AtomicInteger fired =
                new java.util.concurrent.atomic.AtomicInteger();

        transport = new HttpSubscriptionTransport(endpoint, fired::incrementAndGet,
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5),
                Duration.ofMillis(60));
        try (var ignored = transport.subscribe(STREAM, delivery -> { })) {
            await(() -> fired.get() >= 1, "the first connection");
            Thread.sleep(600);
            assertThat(fired.get())
                    .as("⚠️ MANY POLLS, ONE CALLBACK. Ten-odd polls happen in this window at "
                            + "a 60 ms wait, and a callback per poll would be ten "
                            + "registrations of every index on the node")
                    .isEqualTo(1);
        }
    }

    @Test
    void aWEDGEDConsumerDoesNOTStopDeliveriesToTheOTHERS() throws Exception {
        // ⚠️ THE HUB'S PUBLISHING THREAD IS SHARED BY EVERY SUBSCRIBER ON THE
        // NODE. A session that BLOCKED on a full queue would stop deliveries to
        // all of them -- one wedged consumer taking the node's whole read path
        // with it -- which is why the session offers and drops instead.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(),
                table(), Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        String endpoint = "http://localhost:" + server.port();

        // One consumer opens a session and then stops polling; its queue fills.
        var wedged = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofMillis(50));
        var wedgedHandle = wedged.subscribe(STREAM, d -> { });
        await(() -> service.sessionCount() == 1, "the wedged session");
        wedgedHandle.close();
        wedged.close();
        Thread.sleep(200);

        List<Delivery> healthy = new CopyOnWriteArrayList<>();
        try (var ignored = connect(endpoint, () -> { }).subscribe(STREAM, healthy::add)) {
            await(() -> transport.reconnects() > 0, "the healthy stream");
            // ⚠️ MORE THAN THE QUEUE HOLDS, so the wedged session's queue is
            // full for most of them.
            for (int i = 0; i < SubscriptionService.QUEUE_DEPTH + 10; i++) {
                publish(hub, "seg-flood-" + i, 300 + i, 1);
            }
            await(() -> healthy.size() >= 10, "deliveries to the healthy consumer");
        }
    }

    @Test
    void aQUIETPollAnswersEMPTYRatherThanFailing() throws Exception {
        // ⚠️ AN EMPTY 200 IS EVERY IDLE POLL, THE COMMON CASE -- and reading it
        // with `entity().as(byte[])` throws `No entity`, which the reader loop
        // would treat as a failure and reconnect for ever. MEASURED.
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), table());
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, () -> { }).subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            long after = transport.reconnects();
            Thread.sleep(500);
            assertThat(transport.reconnects())
                    .as("⚠️ NO RECONNECT ON A QUIET STREAM. A transport that treated an "
                            + "empty answer as a failure would reconnect in a loop, and "
                            + "every one of those fires `onReconnect` -- which re-registers "
                            + "every index on the node, for ever")
                    .isEqualTo(after);
            assertThat(got).isEmpty();
        }
    }

    @Test
    void aCLOSEDSubscriptionSTOPSReading() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        List<Delivery> got = new CopyOnWriteArrayList<>();

        var handle = connect(endpoint, () -> { }).subscribe(STREAM, got::add);
        await(() -> transport.reconnects() > 0, "the stream to open");
        handle.close();
        Thread.sleep(200);
        int before = got.size();
        publish(hub, "seg-after-close", 1, 1);
        Thread.sleep(500);

        assertThat(got.size())
                .as("⚠️ A HANDLE THAT DOES NOT STOP THE READER IS A CONSUMER THAT KEEPS "
                        + "TAKING DELIVERIES FOR A SHARD THIS NODE NO LONGER HOSTS, and the "
                        + "records go to a listener whose shard moved away")
                .isEqualTo(before);
    }

    @Test
    void aREGISTRATIONTheIngesterREFUSESThrowsRatherThanBeingSwallowed() throws Exception {
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), table());
        server.stop();
        server = null;

        // ⚠️ BOTH CALLERS RETRY, so a transport that swallowed would leave the
        // ingester never learning any index's shape while the node counted its
        // pushes as delivered (M6.7's own three-attempt rule).
        assertThatThrownBy(() -> connect(endpoint, () -> { })
                .register(new IndexRegistration(uuidToBase64Url(LOGS), "logs", List.of(),
                        4, 4, 1, 1)))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void aPROGRESSFrameTheIngesterREFUSESThrowsToo() throws Exception {
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), table());
        server.stop();
        server = null;

        assertThatThrownBy(() -> connect(endpoint, () -> { })
                .report(new ConsumerProgress(List.of(
                        new ConsumerProgress.Entry(uuidToBase64Url(LOGS), 3, "alloc-a", 1)))))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void aMALFORMEDRegistrationIsREFUSEDAndTheCATALOGIsUNCHANGED() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        String endpoint = start(new SubscriptionHub(), catalog, table());

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.post(SubscriptionService.REGISTER_PATH)
                .submit(new byte[] {1, 2, 3})) {
            assertThat(response.status().code()).isEqualTo(400);
        }
        assertThat(catalog.size()).isZero();
    }

    @Test
    void aMALFORMEDProgressFrameLEAVESTheWatermarkWhereItWas() throws Exception {
        WatermarkTable watermarks = table();
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), watermarks);

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.post(SubscriptionService.PROGRESS_PATH)
                .submit(new byte[] {9, 9, 9, 9})) {
            assertThat(response.status().code()).isEqualTo(400);
        }
        assertThat(watermarks.of(STREAM).known())
                .as("⚠️ AN UNMOVED WATERMARK KEEPS DATA, which is the safe direction: a "
                        + "frame applied from bytes nobody sent could advance it past what a "
                        + "shard has indexed")
                .isFalse();
    }

    /**
     * ⚠️ THROUGH THE HUB'S REAL PUBLISH PATH, not by handing a `Push` to a
     * subscriber: what is under test is the ingester reading what the hub
     * actually produces, and a hand-built push would let the two disagree.
     */
    private static void publish(SubscriptionHub hub, String segmentKey, long firstOffset,
            int records) throws Exception {
        binjava.format.CommitDelta delta = new binjava.format.CommitDelta(1,
                List.of(new binjava.format.SegmentCommit(segmentKey,
                        List.of(new binjava.format.RunCommit(STREAM, records, firstOffset)),
                        new binjava.format.SegmentCommit.Attribution("pod1", "inc-1", 0))));
        try (var store = new binjava.binstore.backend.MemoryBinStore()) {
            store.put(segmentKey, binjava.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
            hub.publish(delta, segmentKey, new byte[] {1, 2, 3},
                    new binjava.ingest.SegmentServing(
                            new binjava.ingest.FetchPolicy(new binjava.ingest.FetchPolicyConfig(
                                    Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                            store.capabilities(), new binjava.ingest.SegmentProxy(store)));
        }
    }
}
