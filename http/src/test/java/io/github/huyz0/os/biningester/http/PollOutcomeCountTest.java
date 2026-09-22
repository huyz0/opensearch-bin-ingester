// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport.PollFailure;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A poll answered with anything but 200 is COUNTED, by outcome class (M8.37,
 * NFR-11).
 *
 * <p>⚠️ **A WEDGED CONSUMER LOOKED EXACTLY LIKE AN IDLE ONE.** 503 at the
 * session cap, 400 for an unreadable path and 404 for a missing route were all
 * the same silent backoff, with no movement in {@code reconnects()}. One
 * counter per class, and no per-index label (observability.md rule 1).
 */
class PollOutcomeCountTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

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

    private String answering(HttpRouting.Builder routing) {
        server = WebServer.builder().port(0).routing(routing).build().start();
        return "http://localhost:" + server.port();
    }

    private void pollAndAwait(String endpoint, PollFailure expected) {
        transport = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, d -> { });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (transport.pollFailures(expected) < 2) {
            assertThat(System.nanoTime()).as("%s counted", expected).isLessThan(deadline);
            Thread.onSpinWait();
        }
        for (PollFailure other : EnumSet.complementOf(EnumSet.of(expected))) {
            assertThat(transport.pollFailures(other))
                    .as("⚠️ ONLY %s, not %s: one class per outcome", expected, other).isZero();
        }
        if (expected != PollFailure.MALFORMED) {
            // a 200 establishes the stream before its body is read
            assertThat(transport.reconnects()).as("and no stream was established").isZero();
        }
    }

    private String status(Status status) {
        return answering(HttpRouting.builder().get(SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(status).send("no")));
    }

    @Test
    void aThrowingReconnectListenerIsCALLBACKNotMALFORMED() {
        String endpoint = answering(HttpRouting.builder().get(
                SubscriptionService.SUBSCRIBE_PATH, (req, res) -> res.status(Status.OK_200).send("no")));
        transport = new HttpSubscriptionTransport(endpoint,
                () -> { throw new IllegalStateException("listener failed"); },
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, d -> { });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (transport.pollFailures(PollFailure.CALLBACK) < 2) {
            assertThat(System.nanoTime()).as("callback failures counted")
                    .isLessThan(deadline);
            Thread.onSpinWait();
        }
        assertThat(transport.pollFailures(PollFailure.MALFORMED))
                .as("a callback failure is not malformed response data")
                .isZero();
    }

    @Test
    void aThrowingFloorListenerIsCALLBACKNotUNREACHABLE() {
        String endpoint = answering(HttpRouting.builder().get(
                SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(Status.OK_200).send(framed(
                        new io.github.huyz0.os.biningester.format.RetainedFloor(STREAM, 1).encode()))));
        transport = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, new io.github.huyz0.os.biningester.client.SubscriptionTransport.Listener() {
            @Override
            public void onDelivery(io.github.huyz0.os.biningester.client.Delivery delivery) {
            }

            @Override
            public void onRetainedFloor(RunKey key, long oldestRetainedOffset) {
                throw new IllegalStateException("floor listener failed");
            }
        });
        await(PollFailure.CALLBACK, 2);
        assertThat(transport.pollFailures(PollFailure.UNREACHABLE)).isZero();
        assertThat(transport.pollFailures(PollFailure.MALFORMED)).isZero();
    }

    @Test
    void aThrowingFloorAskIsCALLBACKNotUNREACHABLE() {
        String endpoint = answering(HttpRouting.builder().get(
                SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(Status.OK_200).send("no")));
        transport = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, new io.github.huyz0.os.biningester.client.SubscriptionTransport.Listener() {
            @Override
            public void onDelivery(io.github.huyz0.os.biningester.client.Delivery delivery) {
            }

            @Override
            public boolean wantsRetainedFloor(RunKey key) {
                throw new IllegalStateException("floor ask failed");
            }
        });
        await(PollFailure.CALLBACK, 2);
        assertThat(transport.pollFailures(PollFailure.UNREACHABLE)).isZero();
    }

    @Test
    void aThrowingDeliveryListenerIsCALLBACKNotMALFORMED() {
        String endpoint = answering(HttpRouting.builder().get(
                SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(Status.OK_200).send(framed(new SubscriptionEvent(
                        "session", 1, 1, STREAM, "seg", 0, 1, FetchMode.INLINE,
                        new byte[] {1}).encode()))));
        transport = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, delivery -> {
            throw new IllegalStateException("delivery listener failed");
        });
        await(PollFailure.CALLBACK, 2);
        assertThat(transport.pollFailures(PollFailure.MALFORMED)).isZero();
    }

    @Test
    void aCallbackThatClosesBeforeThrowingIsNotCounted() throws Exception {
        CountDownLatch called = new CountDownLatch(1);
        AtomicReference<Thread> reader = new AtomicReference<>();
        transport = new HttpSubscriptionTransport(answering(HttpRouting.builder().get(
                SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(Status.OK_200).send("no"))),
                () -> {
                    reader.set(Thread.currentThread());
                    called.countDown();
                    transport.close();
                    throw new IllegalStateException("closed callback failed");
                }, Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofSeconds(2));
        transport.subscribe(STREAM, d -> { });
        assertThat(called.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(reader.get()).isNotNull();
        reader.get().join(5_000);
        assertThat(reader.get().isAlive()).as("the callback reader finished the catch path").isFalse();
        assertThat(transport.pollFailures(PollFailure.CALLBACK)).isZero();
        assertThat(transport.pollFailures(PollFailure.MALFORMED)).isZero();
    }

    private void await(PollFailure kind, long count) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (transport.pollFailures(kind) < count) {
            assertThat(System.nanoTime()).as("%s counted", kind).isLessThan(deadline);
            Thread.onSpinWait();
        }
    }

    private static byte[] framed(byte[] payload) {
        return ByteBuffer.allocate(4 + payload.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(payload.length).put(payload).array();
    }

    @Test
    void a503IsUNAVAILABLE() {
        pollAndAwait(status(Status.SERVICE_UNAVAILABLE_503), PollFailure.UNAVAILABLE);
    }

    @Test
    void a400IsREFUSED() {
        pollAndAwait(status(Status.BAD_REQUEST_400), PollFailure.REFUSED);
    }

    @Test
    void aMISSINGRouteIsREFUSED() {
        pollAndAwait(answering(HttpRouting.builder()), PollFailure.REFUSED);
    }

    @Test
    void a500IsASERVERError() {
        pollAndAwait(status(Status.INTERNAL_SERVER_ERROR_500), PollFailure.SERVER_ERROR);
    }

    @Test
    void aClosedPortIsUNREACHABLE() throws Exception {
        int port;
        try (ServerSocket free = new ServerSocket(0)) {
            port = free.getLocalPort();
        }
        pollAndAwait("http://localhost:" + port, PollFailure.UNREACHABLE);
    }

    @Test
    void anUNREADABLE200IsMALFORMED() {
        byte[] garbage = {0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8};
        pollAndAwait(answering(HttpRouting.builder().get(SubscriptionService.SUBSCRIBE_PATH,
                (req, res) -> res.status(Status.OK_200).send(garbage))), PollFailure.MALFORMED);
    }
}
