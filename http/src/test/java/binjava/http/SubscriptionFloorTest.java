// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.ConsumerClient;
import binjava.client.HttpSubscriptionTransport;
import binjava.client.PositionCollectedException;
import binjava.client.StreamFraming;
import binjava.format.RetainedFloor;
import binjava.format.RunKey;
import binjava.ingest.IndexCatalog;
import binjava.ingest.RetainedFloors;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The retained floor, over the real subscription channel (M8.6, M7.18,
 * ADR-0056).
 *
 * <p>⚠️ **THE WIRE BOTH WAYS, AND THE OLD CONSUMER ON IT.** The frame is new,
 * and every decoder in {@code format} refuses a magic it does not know -- so
 * what makes the change deployable is that a consumer which does not ASK never
 * receives one. That half is asserted as carefully as the half that works.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SubscriptionFloorTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 3);

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

    private String start(RetainedFloors floors) {
        WatermarkTable table = new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1),
                Duration.ofHours(2), Duration.ofMinutes(30));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new SubscriptionService(new SubscriptionHub(),
                                new IndexCatalog(), table, Clock.systemUTC(), floors)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    /** A cache that knows STREAM's floor is 500. */
    private static RetainedFloors knowing(long floor) {
        return new RetainedFloors(() -> java.util.Optional.of(new binjava.format.Checkpoint(1,
                java.util.Map.of(STREAM, new binjava.format.Checkpoint.StreamOffsets(9000, floor)),
                java.util.Map.of())), Clock.systemUTC(), Duration.ofMinutes(1));
    }

    /** The frames of one poll answer, raw. */
    private List<byte[]> poll(String base, String sub, boolean askForFloor) throws Exception {
        var request = WebClient.builder().baseUri(base).build()
                .get("/sub/" + STREAM.indexId() + "/" + STREAM.partitionId())
                .queryParam("wait", "1").queryParam("sub", sub);
        if (askForFloor) {
            request = request.queryParam(SubscriptionService.FLOOR_PARAM, "1");
        }
        // ⚠️ THROUGH THE STREAM: Helidon's `as(byte[])` throws "No entity" on
        // an empty 200, and an empty 200 is the quiet-stream answer -- which
        // is half of what this file asserts (M8.21 measured the same).
        byte[] body;
        try (var response = request.request(); var in = response.inputStream()) {
            body = in.readAllBytes();
        }
        List<byte[]> frames = new ArrayList<>();
        var in = new ByteArrayInputStream(body);
        for (byte[] frame; (frame = StreamFraming.readFrame(in)) != null; ) {
            frames.add(frame);
        }
        return frames;
    }

    @Test
    void aNEWSessionThatAsksIsSentItsStreamsFLOORAsTheFirstFrame() throws Exception {
        String base = start(knowing(500));

        List<byte[]> frames = poll(base, "s-1", true);

        assertThat(frames).hasSize(1);
        assertThat(RetainedFloor.decode(frames.get(0)))
                .as("⚠️ ON A QUIET STREAM TOO: no segment was pushed, and a resume is exactly "
                        + "when a stream is quiet -- a floor that rode on a push would not "
                        + "arrive")
                .isEqualTo(new RetainedFloor(STREAM, 500));
    }

    @Test
    void aSessionThatDoesNOTAskIsNeverSentOne() throws Exception {
        // ⚠️ THE OLD PLUGIN. It does not know this magic and would refuse the
        // whole answer the frame arrived in -- so an ingester upgraded first
        // would stop every plugin that had not been.
        String base = start(knowing(500));

        assertThat(poll(base, "s-1", false)).isEmpty();
    }

    @Test
    void theFloorIsSentToEVERYPollThatAsksNotOnlyTheSessionsFirst() throws Exception {
        // ⚠️ AN EARLIER VERSION SENT IT ONLY ON THE ANSWER THAT CREATED THE
        // SESSION, and review found the hole: that answer can be lost on the
        // network, the retry reuses the same `sub` id, and the session never
        // hears its floor. The consumer asks again, a bounded number of times.
        String base = start(knowing(500));
        poll(base, "s-1", true);

        assertThat(poll(base, "s-1", true))
                .as("the same session, asking again because its first answer was lost")
                .hasSize(1);
        assertThat(poll(base, "s-1", false))
                .as("and a poll that no longer asks is sent none").isEmpty();
    }

    @Test
    void anUNKNOWNFloorSendsNoFrame() throws Exception {
        String base = start(RetainedFloors.unknown());

        assertThat(poll(base, "s-1", true)).isEmpty();
    }

    @Test
    void aREALConsumerOverTheREALTransportRefusesACollectedPosition() throws Exception {
        // ⚠️ M7.18 IN ONE LINE: `ConsumerClient.retainedFrom` had no production
        // caller, so M7.16's refusal could not fire. This is the production
        // transport handing it the floor, and the client refusing with it.
        String base = start(knowing(500));
        transport = new HttpSubscriptionTransport(base, () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofMillis(50));

        try (ConsumerClient client = new ConsumerClient(transport, STREAM, 16)) {
            // ⚠️ WHAT A RESUME DOES: a client asks for a floor only when a shard
            // resumes, and costs the serving pod nothing otherwise.
            client.requestFreshFloor();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (true) {
                try {
                    client.refuseIfCollected(100);
                } catch (PositionCollectedException refused) {
                    assertThat(refused.oldestRetainedOffset()).isEqualTo(500);
                    break;
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the floor never reached the client");
                }
                Thread.onSpinWait();
            }
            // and a position at or above the floor is still fine
            client.refuseIfCollected(500);
        }
    }

    @Test
    void aFLOORForAnotherStreamIsNOTAppliedToThisClient() throws Exception {
        // ⚠️ A CLIENT's REFUSAL IS ABOUT ITS STREAM. Applying another stream's
        // floor would refuse positions that are fine.
        RunKey other = new RunKey(UUID.randomUUID(), 0);
        List<binjava.client.SubscriptionTransport.Listener> captured = new ArrayList<>();
        binjava.client.SubscriptionTransport fake = (key, listener) -> {
            captured.add(listener);
            return () -> { };
        };
        try (ConsumerClient client = new ConsumerClient(fake, STREAM, 16)) {
            captured.get(0).onRetainedFloor(other, 10_000);
            client.refuseIfCollected(100);

            captured.get(0).onRetainedFloor(STREAM, 500);
            assertThatThrownBy(() -> client.refuseIfCollected(100))
                    .isInstanceOf(PositionCollectedException.class);
        }
    }

    @Test
    void theCacheIsAskedONLYByAPollThatAsks() throws Exception {
        // ⚠️ THE READ BOUND IS PER NODE, and it holds only if `SubscriptionService` does
        // not ask the cache on paths that cannot use the answer.
        java.util.concurrent.atomic.AtomicInteger reads =
                new java.util.concurrent.atomic.AtomicInteger();
        RetainedFloors counting = new RetainedFloors(() -> {
            reads.incrementAndGet();
            return java.util.Optional.empty();
        }, Clock.systemUTC(), Duration.ofNanos(1));
        String base = start(counting);

        poll(base, "s-1", false);
        poll(base, "s-1", false);
        assertThat(reads).as("a session that did not ask costs no read").hasValue(0);

        poll(base, "s-2", true);
        poll(base, "s-2", true);
        assertThat(reads)
                .as("each poll that asks consults the cache -- here refreshed every "
                        + "nanosecond so that consulting is visible as a read")
                .hasValue(2);
    }

    /** Starts a stub ingester that records each poll's floor parameter. */
    private List<String> recordingIngester(
            java.util.function.Function<List<String>, byte[]> answer) {
        List<String> asked = new java.util.concurrent.CopyOnWriteArrayList<>();
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().get("/sub/{a}/{b}", (req, res) -> {
                    asked.add(req.query().first(SubscriptionService.FLOOR_PARAM)
                            .orElse("absent"));
                    res.send(answer.apply(List.copyOf(asked)));
                })).build().start();
        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(),
                () -> { }, Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofMillis(20));
        return asked;
    }

    private static void awaitPolls(List<String> asked, int count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (asked.size() < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + asked.size() + " polls: " + asked);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void theTRANSPORTAsksONLYWhileItsClientWantsAFloor() throws Exception {
        // ⚠️ A TAILING CLIENT MUST NOT ASK: every ask can cost the serving pod
        // a store read, and a node whose shards are all tailing must cost none.
        // A resume asks, a dropped answer is asked again, and a floor ends it.
        byte[] floorFrame = framed(new RetainedFloor(STREAM, 500).encode());
        // the stub answers the SECOND ask with the floor: the first is "lost"
        List<String> asked = recordingIngester(sofar ->
                sofar.stream().filter("1"::equals).count() == 2 ? floorFrame : new byte[0]);

        try (ConsumerClient client = new ConsumerClient(transport, STREAM, 16)) {
            awaitPolls(asked, 3);
            assertThat(List.copyOf(asked)).as("⚠️ TAILING: NO ASK").containsOnly("0");

            client.requestFreshFloor();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (List.copyOf(asked).stream().filter("1"::equals).count() < 2
                    || asked.size() < indexOfSecondAsk(asked) + 4) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("polls: " + asked);
                }
                Thread.onSpinWait();
            }
            List<String> seen = List.copyOf(asked);
            int second = indexOfSecondAsk(seen);

            assertThat(seen.stream().filter("1"::equals).count())
                    .as("⚠️ ASKED AGAIN AFTER AN ANSWER WITH NO FLOOR, AND THEN NO MORE")
                    .isEqualTo(2);
            assertThat(seen.subList(second + 1, seen.size()))
                    .as("⚠️ NOT ONCE THE FLOOR HAS ARRIVED").containsOnly("0");
            assertThatThrownBy(() -> client.refuseIfCollected(100))
                    .isInstanceOf(PositionCollectedException.class);
        }
    }

    private static int indexOfSecondAsk(List<String> asked) {
        List<String> seen = List.copyOf(asked);
        int count = 0;
        for (int i = 0; i < seen.size(); i++) {
            if ("1".equals(seen.get(i)) && ++count == 2) {
                return i;
            }
        }
        return Integer.MAX_VALUE - 10;
    }

    @Test
    void aRESUMEThatNeverGetsAFloorSTOPSAskingAfterItsBUDGET() throws Exception {
        // ⚠️ THE REGRESSION REVIEW FOUND, PINNED AS A COUNT. An ingester with
        // no floor for this stream sends none; a consumer that asked until one
        // arrived would ask for ever, and each ask can cost the serving pod a
        // store read per refresh interval -- the idle-pod timer ADR-0056
        // rejects, arriving through the protocol.
        List<String> asked = recordingIngester(sofar -> new byte[0]);

        try (ConsumerClient client = new ConsumerClient(transport, STREAM, 16)) {
            client.requestFreshFloor();
            awaitPolls(asked, ConsumerClient.MAX_FLOOR_ASKS + 10);

            List<String> seen = List.copyOf(asked);
            assertThat(seen.stream().filter("1"::equals).count())
                    .as("⚠️ EXACTLY THE BUDGET, AND THEN SILENCE")
                    .isEqualTo(ConsumerClient.MAX_FLOOR_ASKS);
            assertThat(seen.subList(seen.size() - 5, seen.size())).containsOnly("0");
        }
    }

    private static byte[] framed(byte[] frame) {
        var out = new java.io.ByteArrayOutputStream();
        out.writeBytes(java.nio.ByteBuffer.allocate(4).putInt(frame.length).array());
        out.writeBytes(frame);
        return out.toByteArray();
    }
}
