// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.Delivery;
import binjava.client.HttpSubscriptionTransport;
import binjava.format.FetchMode;
import binjava.format.RunKey;
import binjava.ingest.IndexCatalog;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What the subscription channel REFUSES, and what bounds it (M8.21).
 *
 * <p>⚠️ **SPLIT FROM {@code SubscriptionChannelTest} AT THE 700-LINE CAP**, and
 * the seam is a real one rather than an arithmetic one: that file pins that the
 * three messages TRAVEL, and this one pins every number that stops a peer, a
 * consumer or a caller from choosing this process's memory, threads or CPU. All
 * five bounds here were added or corrected after review measured them missing
 * or unpinned.
 */
class SubscriptionBoundsTest {

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
    void aFRAMEClaimingMoreThanTheCapIsREFUSED() throws Exception {
        // ⚠️ FOUR BYTES AN ATTACKER CHOOSES. Without the bound, a length of
        // 0x7FFFFFFF allocates two gigabytes before a single byte of it has
        // arrived -- and an OutOfMemoryError escapes the reader loop past every
        // catch it has.
        byte[] lying = new byte[] {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 1, 2, 3};
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().get("/sub/{a}/{b}",
                        (req, res) -> res.send(lying))).build().start();
        List<Delivery> got = new CopyOnWriteArrayList<>();

        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(),
                () -> { }, Duration.ofMillis(20), Duration.ofMillis(100),
                Duration.ofSeconds(5), Duration.ofMillis(50));
        try (var ignored = transport.subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() >= 1, "a first poll");
            long after = transport.reconnects();
            assertThat(got).isEmpty();

            // ⚠️ THE READER MUST STILL BE ALIVE, and that is what the bound
            // buys: without it `new byte[0x7FFFFFFF]` throws an
            // OutOfMemoryError, which is an ERROR -- it escapes the reader's
            // `catch (IOException | RuntimeException)`, the virtual thread
            // dies, and the consumer stops for ever with no delivery and no
            // reconnect. Asserting only that nothing was delivered cannot tell
            // that from a reader that refused the frame and carried on.
            await(() -> transport.reconnects() > after, "the reader to carry on polling");
        }
    }


    @Test
    void anANSWERBiggerThanTheCapIsREFUSEDRatherThanBuffered() throws Exception {
        // ⚠️ A PEER CHOOSING THIS NODE'S HEAP. ⚠️ **THE ANSWER CARRIES A
        // PERFECTLY VALID EVENT FIRST**, which is what makes this case
        // discriminate: with the cap, the answer is refused whole and nothing
        // is delivered; without it, the first frame parses and the delivery
        // arrives. An oversized answer of GARBAGE would deliver nothing either
        // way and pin nothing -- measured.
        //
        // ⚠️ **AND THE CAP IS INJECTED SMALL ON PURPOSE.** Against the real
        // 32 MiB cap this case passes whether the length is checked BEFORE the
        // body is buffered or after it, because 32 MiB fits in the test heap --
        // review measured exactly that, on an implementation that buffered
        // first. At 64 KiB the body is still produced whole by the server, and
        // what is pinned is that the reader refused it on the way in.
        byte[] oneEvent = validFrame();
        int cap = 64 << 10;
        int padding = cap + 1024;
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().get("/sub/{a}/{b}", (req, res) -> {
                    byte[] body = new byte[oneEvent.length + padding];
                    System.arraycopy(oneEvent, 0, body, 0, oneEvent.length);
                    res.send(body);
                })).build().start();
        List<Delivery> got = new CopyOnWriteArrayList<>();

        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(),
                () -> { }, Duration.ofMillis(20), Duration.ofMillis(100),
                Duration.ofSeconds(5), Duration.ofMillis(50), cap);
        try (var ignored = transport.subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() >= 1, "a first poll");
            long after = transport.reconnects();
            assertThat(got)
                    .as("⚠️ THE WHOLE ANSWER IS REFUSED, INCLUDING ITS VALID FIRST FRAME. A "
                            + "reader that parsed what it could would be buffering whatever "
                            + "a peer chose to send before deciding")
                    .isEmpty();
            // ⚠️ AND THE READER SURVIVES IT: a refusal is an IOException the
            // poll loop catches, where the allocation this bound prevents is an
            // Error that kills the subscription's thread in silence.
            await(() -> transport.reconnects() > after, "the reader to carry on polling");
        }
    }


    @Test
    void aNEWSubscriberPastTheSESSIONCapIsREFUSED() throws Exception {
        // ⚠️ THE `sub` ID IS THE CALLER'S, ON A ROUTE THIS DEPLOYMENT DOES NOT
        // AUTHENTICATE, and a session is a hub subscription plus a 64-push
        // queue held until the idle expiry. A caller sending a fresh id per
        // poll allocates both faster than any sweep can reclaim them -- the
        // sweep cannot help, because every one of those sessions was just
        // polled. The cap is the whole defence.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(), table(),
                SubscriptionService.IDLE_EXPIRY, Clock.systemUTC(), 2);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        String endpoint = "http://localhost:" + server.port();
        WebClient client = WebClient.builder().baseUri(endpoint).build();
        String path = HttpSubscriptionTransport.SUBSCRIBE_PREFIX + STREAM.indexId() + "/"
                + STREAM.partitionId();

        for (int i = 0; i < 2; i++) {
            try (var answer = client.get(path).queryParam("wait", "50")
                    .queryParam("sub", "sub-" + i).request()) {
                assertThat(answer.status().code()).isEqualTo(200);
            }
        }
        try (var refused = client.get(path).queryParam("wait", "50")
                .queryParam("sub", "sub-past-the-cap").request()) {
            assertThat(refused.status().code())
                    .as("⚠️ 503, NOT A THIRD SESSION. Without the cap this node allocates a "
                            + "subscription and a queue for every id a caller invents")
                    .isEqualTo(503);
        }
        // ⚠️ AND AN ALREADY-ADMITTED SUBSCRIBER IS STILL SERVED. Review
        // MEASURED the admission test with its `containsKey` half deleted: the
        // case stayed green, and a full ingester would then answer 503 to
        // every one of its OWN subscribers -- the whole channel stopping
        // because one caller was refused.
        try (var again = client.get(path).queryParam("wait", "50")
                .queryParam("sub", "sub-0").request()) {
            assertThat(again.status().code())
                    .as("⚠️ 200 FOR A SESSION THAT ALREADY EXISTS, cap or no cap")
                    .isEqualTo(200);
        }
        assertThat(service.sessionCount()).isEqualTo(2);
    }


    @Test
    void aCONSUMERWhoseIngesterIsDOWNBacksOffRatherThanHOTLooping() throws Exception {
        // ⚠️ THE RETRY LOOP MUST USE THE BACKOFF IT COMPUTES. Review MEASURED
        // `if (true) { return backoff; }` at the head of `sleepAndGrow` -- no
        // sleep, no growth -- leaving every other case in this file green: the
        // T0 cases call `jitteredMillis` and `grow` directly, and nothing
        // pinned that the loop calls them. A consumer whose ingester is down
        // then hot-loops against it, once per subscription, which is the one
        // failure a retry exists to prevent.
        java.util.concurrent.atomic.AtomicInteger attempts =
                new java.util.concurrent.atomic.AtomicInteger();
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().get("/sub/{a}/{b}", (req, res) -> {
                    attempts.incrementAndGet();
                    res.status(500).send("down");
                })).build().start();

        // ⚠️ A 50 ms FLOOR AND AN 800 ms CEILING OVER 1.2 s, which is chosen to
        // pin the GROWTH and not only the sleep: without doubling, 1.2 s at a
        // flat 50 ms floor is around twenty attempts, and with it, five. Review
        // MEASURED the first version of this case -- 600 ms at a 100 ms floor,
        // asserting twelve -- staying green with `grow` never called.
        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(),
                () -> { }, Duration.ofMillis(50), Duration.ofMillis(800),
                Duration.ofSeconds(5), Duration.ofMillis(50));
        try (var ignored = transport.subscribe(STREAM, delivery -> { })) {
            await(() -> attempts.get() >= 1, "a first attempt");
            Thread.sleep(1200);
            assertThat(attempts.get())
                    .as("⚠️ A HANDFUL, NOT THOUSANDS, AND THE BACKOFF GROWS. With the sleep "
                            + "removed this is bounded only by how fast the loop can open a "
                            + "socket; with the sleep but no doubling it is about twenty")
                    .isLessThanOrEqualTo(8);
            assertThat(attempts.get())
                    .as("⚠️ AND IT DOES KEEP TRYING, or a consumer never comes back")
                    .isGreaterThanOrEqualTo(2);
        }
    }


    @Test
    void theWAITAConsumerAsksForIsCLAMPEDByTheIngester() {
        // ⚠️ BOUNDED BY THE SERVER, NOT BY THE CALLER, and asserted here rather
        // than over the socket because review MEASURED the clamp removable with
        // all twenty socket cases green. `?wait=0` turns long polling into a
        // busy loop against the ingester -- the cost-relevant half, since this
        // route's whole budget is one request per subscription per 30 s -- and
        // `?wait=3600000` holds a thread and a connection for an hour.
        assertThat(SubscriptionService.waitFor("0"))
                .as("⚠️ A CALLER CANNOT ASK FOR A BUSY LOOP")
                .isEqualTo(Duration.ofMillis(SubscriptionService.MIN_WAIT_MILLIS));
        assertThat(SubscriptionService.waitFor("-5"))
                .isEqualTo(Duration.ofMillis(SubscriptionService.MIN_WAIT_MILLIS));
        assertThat(SubscriptionService.waitFor("3600000"))
                .as("⚠️ NOR FOR AN HOUR OF THIS NODE'S THREAD")
                .isEqualTo(Duration.ofMillis(SubscriptionService.MAX_WAIT_MILLIS));
        assertThat(SubscriptionService.waitFor("1200"))
                .as("⚠️ AND A REASONABLE ASK IS HONOURED, or the clamp is just a constant")
                .isEqualTo(Duration.ofMillis(1200));
        assertThat(SubscriptionService.waitFor((String) null))
                .isEqualTo(Duration.ofMillis(SubscriptionService.MAX_WAIT_MILLIS));
    }


    @Test
    void anUNREADABLEWaitIsTheCALLERSMistakeAndNotA500() throws Exception {
        // ⚠️ `Long::parseLong` ON `?wait=abc` ANSWERS 500 -- an ingester fault
        // for a consumer's typo, and one that would be read as this node being
        // sick. There is a right answer here and it is the default wait.
        assertThat(SubscriptionService.waitFor("abc"))
                .isEqualTo(Duration.ofMillis(SubscriptionService.MAX_WAIT_MILLIS));

        // ⚠️ A PUSH IS QUEUED FIRST SO THE POLL ANSWERS AT ONCE. `wait=abc`
        // clamps to the 30 s MAXIMUM, so a quiet poll would make this case take
        // thirty seconds to assert a status code -- and it did, until review
        // measured 30.021 s of it.
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = start(hub, new IndexCatalog(), table());
        WebClient client = WebClient.builder().baseUri(endpoint).build();
        String path = HttpSubscriptionTransport.SUBSCRIBE_PREFIX + STREAM.indexId() + "/"
                + STREAM.partitionId();
        try (var opened = client.get(path).queryParam("wait", "50")
                .queryParam("sub", "s").request()) {
            assertThat(opened.status().code()).isEqualTo(200);
        }
        publish(hub, "seg-wait", 7L, 1);
        try (var answer = client.get(path)
                .queryParam("wait", "abc").queryParam("sub", "s").request()) {
            assertThat(answer.status().code())
                    .as("⚠️ NOT 500: an unreadable wait is clamped, like every other bound")
                    .isEqualTo(200);
        }
    }

    /** One legal event, length-prefixed, as the ingester would write it. */
    private static byte[] validFrame() {
        byte[] event = new binjava.format.SubscriptionEvent("s", 1L, 1L, STREAM, "seg-x",
                7L, 3, FetchMode.PROXY, new byte[0]).encode();
        byte[] framed = new byte[4 + event.length];
        java.nio.ByteBuffer.wrap(framed).order(java.nio.ByteOrder.BIG_ENDIAN).putInt(event.length);
        System.arraycopy(event, 0, framed, 4, event.length);
        return framed;
    }


    @Test
    void aPOLLWithNOSubscriberIdIsREFUSED() throws Exception {
        // ⚠️ AN ID THE SERVER INVENTS IS A NEW SESSION PER POLL, which is the
        // defect sessions exist to fix arriving through the front door.
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), table());

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.get("/sub/" + LOGS + "/3").request()) {
            assertThat(response.status().code()).isEqualTo(400);
        }
    }


    @Test
    void aSUBSCRIBEToAPathThatIsNotAStreamIs400() throws Exception {
        String endpoint = start(new SubscriptionHub(), new IndexCatalog(), table());

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.get("/sub/not-a-uuid/3").request()) {
            assertThat(response.status().code()).isEqualTo(400);
        }
    }


    @Test
    void aRETRYFloorABOVEItsCeilingIsREFUSED() {
        assertThatThrownBy(() -> new HttpSubscriptionTransport("http://x:1", () -> { },
                Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HttpSubscriptionTransport("http://x:1", () -> { },
                Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
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
