// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.client.HttpSubscriptionTransport;
import binjava.format.RunKey;
import binjava.ingest.IndexCatalog;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The subscription queues are bounded in BYTES, across every session (M8.36,
 * NFR-6).
 *
 * <p>⚠️ **A BOUND IN PUSHES IS NOT A BOUND IN MEMORY.** An {@code INLINE} push
 * carries a whole segment, and each session assembles its own copy, so 64
 * pushes per session is 64 segments per session, times the session cap. The
 * promise that a slow consumer loses its queued pushes and never the
 * ingester's memory has to be in the unit NFR-6 states.
 */
class SubscriptionByteBudgetTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 3);
    private static final int SEGMENT = 1_000;
    private static final long BUDGET = 4_096;

    private WebServer server;
    private final List<HttpSubscriptionTransport> transports =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        transports.forEach(HttpSubscriptionTransport::close);
        if (server != null) {
            server.stop();
        }
    }

    private String start(SubscriptionService service) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        return "http://localhost:" + server.port();
    }

    private HttpSubscriptionTransport transport(String endpoint) {
        HttpSubscriptionTransport t = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5));
        transports.add(t);
        return t;
    }

    /** A clock a case moves by hand, so the idle sweep is driven, not waited for. */
    private static final class HandClock extends Clock {
        private volatile long millis = 1_000_000L;

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public java.time.Instant instant() {
            return java.time.Instant.ofEpochMilli(millis);
        }

        @Override
        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    private static SubscriptionService service(SubscriptionHub hub, Clock clock, long budget) {
        return new SubscriptionService(hub, new IndexCatalog(),
                new WatermarkTable(clock, Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofMinutes(30)),
                IDLE, clock, SubscriptionService.MAX_SESSIONS,
                binjava.ingest.RetainedFloors.unknown(), new DrainGate(), budget);
    }

    private static final Duration IDLE = Duration.ofSeconds(90);

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    private static void publish(SubscriptionHub hub, String segmentKey, long firstOffset)
            throws Exception {
        byte[] bytes = new byte[SEGMENT];
        binjava.format.CommitDelta delta = new binjava.format.CommitDelta(1,
                List.of(new binjava.format.SegmentCommit(segmentKey,
                        List.of(new binjava.format.RunCommit(STREAM, 1, firstOffset)),
                        new binjava.format.SegmentCommit.Attribution("pod1", "inc-1", 0))));
        try (var store = new binjava.binstore.backend.MemoryBinStore()) {
            store.put(segmentKey, binjava.binstore.Body.ofBytes(bytes));
            hub.publish(delta, segmentKey, bytes,
                    new binjava.ingest.SegmentServing(
                            new binjava.ingest.FetchPolicy(new binjava.ingest.FetchPolicyConfig(
                                    Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                            store.capabilities(), new binjava.ingest.SegmentProxy(store)));
        }
    }

    @Test
    void wedgedSessionsHOLDNoMoreThanTheBUDGETBetweenThem() throws Exception {
        // ⚠️ SESSIONS OPENED DIRECTLY AND NEVER POLLED: over HTTP a closed
        // consumer's long-poll stays parked on the server and takes a push,
        // which is a race and not a wedge.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = service(hub, new HandClock(), BUDGET);
        service.sessionFor("w1@" + STREAM, STREAM);
        service.sessionFor("w2@" + STREAM, STREAM);

        // ⚠️ 2 sessions x 10 segments x 1,000 B = 20,000 B unbounded, well
        // inside 64 pushes each: a bound in pushes would hold every byte.
        for (int i = 0; i < 10; i++) {
            publish(hub, "seg-" + i, i);
        }

        assertThat(service.queuedBytes())
                .as("the premise: something was queued")
                .isGreaterThan(0)
                .as("⚠️ AND NO MORE THAN THE BUDGET, ACROSS EVERY SESSION")
                .isLessThanOrEqualTo(BUDGET);
    }

    @Test
    void aFULLQueueGIVESBackTheBytesOfWhatItDROPS() throws Exception {
        // ⚠️ THE BUDGET HAS ROOM AND THE QUEUE DOES NOT: a push dropped for
        // depth must not stay counted, or a wedged session with small
        // segments leaks a segment of budget per push past the 64th.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = service(hub, new HandClock(), Long.MAX_VALUE / 2);
        service.sessionFor("w1@" + STREAM, STREAM);

        for (int i = 0; i < SubscriptionService.QUEUE_DEPTH + 6; i++) {
            publish(hub, "seg-" + i, i);
        }

        assertThat(service.queuedBytes())
                .as("exactly what the queue holds: %d pushes of %d B",
                        SubscriptionService.QUEUE_DEPTH, SEGMENT)
                .isEqualTo((long) SubscriptionService.QUEUE_DEPTH * SEGMENT);
    }

    @Test
    void aSWEPTSessionGIVESItsBytesBack() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        HandClock clock = new HandClock();
        SubscriptionService service = service(hub, clock, BUDGET);
        service.sessionFor("w1@" + STREAM, STREAM);
        publish(hub, "seg-0", 0);
        publish(hub, "seg-1", 1);
        assertThat(service.queuedBytes()).as("the premise: it holds bytes").isPositive();

        clock.millis += IDLE.toMillis() + 1;
        service.sweepIdle();

        assertThat(service.sessionCount()).as("the premise: it was swept").isZero();
        assertThat(service.queuedBytes())
                .as("⚠️ THE SWEPT SESSION's BYTES GIVEN BACK: otherwise the budget leaks "
                        + "shut, one abandoned consumer at a time")
                .isZero();
    }

    @Test
    void aPUSHArrivingAfterTheRELEASEIsNotCOUNTED() throws Exception {
        // ⚠️ THE SWEEP's RACE: closing the hub subscription does not wait for a
        // delivery already in flight, so a publish can reach a session after
        // its drain. Counted then, those bytes would never come back.
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = service(hub, new HandClock(), BUDGET);
        SubscriptionService.Session session = service.sessionFor("w1@" + STREAM, STREAM);
        session.release();

        session.offer(new SubscriptionHub.Push(STREAM, "seg-late", 1, 0,
                binjava.format.FetchMode.INLINE, new byte[SEGMENT]));

        assertThat(service.queuedBytes()).as("⚠️ A RELEASED SESSION COUNTS NOTHING").isZero();
    }

    @Test
    void aDRAINEDQueueGIVESItsBytesBack() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = service(hub, Clock.systemUTC(), BUDGET);
        String endpoint = start(service);
        List<Object> got = new java.util.concurrent.CopyOnWriteArrayList<>();
        HttpSubscriptionTransport t = transport(endpoint);
        try (var ignored = t.subscribe(STREAM, got::add)) {
            await(() -> service.sessionCount() == 1, "the session");
            // ⚠️ MORE THAN THE BUDGET IN TOTAL, one at a time: a counter that
            // never gave bytes back would stop delivering after the fourth.
            for (int i = 0; i < 8; i++) {
                publish(hub, "seg-" + i, i);
                int want = i + 1;
                await(() -> got.size() >= want, "delivery " + want);
            }
            await(() -> service.queuedBytes() == 0, "the bytes given back");
        }
    }
}
