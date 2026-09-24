// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HttpCatchUpExchangeTest {

    private static final UUID REQUEST_ID = UUID.fromString(
            "11111111-2222-3333-4444-555555555555");
    private static final UUID OTHER_REQUEST_ID = UUID.fromString(
            "99999999-8888-7777-6666-555555555555");
    private static final RunKey REQUESTED = new RunKey(UUID.fromString(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), 7);
    private static final RunKey UNREQUESTED = new RunKey(UUID.fromString(
            "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"), 8);

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

    @Test
    void dispatchesOnlyRequestedEventsAndWaitsForTheMatchingEnd() throws Exception {
        SubscriptionEvent accepted = event(REQUESTED, "accepted");
        SubscriptionEvent wrongKey = event(UNREQUESTED, "wrong-key");
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new CatchUpEventFrame(OTHER_REQUEST_ID, event(REQUESTED, "wrong-id"))
                                .encode(),
                        new CatchUpEventFrame(REQUEST_ID, wrongKey).encode(),
                        new CatchUpEndFrame(OTHER_REQUEST_ID).encode(),
                        new CatchUpEventFrame(REQUEST_ID, accepted).encode(),
                        new CatchUpEndFrame(REQUEST_ID).encode()))))
                .build().start();
        transport = connect();
        List<SubscriptionEvent> delivered = new ArrayList<>();

        SubscriptionTransport.CatchUpResult result = transport.requestCatchUp(request(), delivered::add);

        assertThat(result).isEqualTo(SubscriptionTransport.CatchUpResult.COMPLETE);
        assertThat(delivered).containsExactly(accepted);
    }

    @Test
    void dispatchesEachFrameBeforeThePeerFinishesTheExchange() throws Exception {
        CountDownLatch firstEventWritten = new CountDownLatch(1);
        CountDownLatch allowEnd = new CountDownLatch(1);
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService((request, sink) -> {
                    sink.write(new CatchUpEventFrame(REQUEST_ID, event(REQUESTED, "first"))
                            .encode());
                    firstEventWritten.countDown();
                    try {
                        if (!allowEnd.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("test did not release the response");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException("test response was interrupted", interrupted);
                    }
                    sink.write(new CatchUpEndFrame(REQUEST_ID).encode());
                })))
                .build().start();
        transport = connect();
        CountDownLatch delivered = new CountDownLatch(1);
        List<SubscriptionEvent> events = new CopyOnWriteArrayList<>();
        CompletableFuture<SubscriptionTransport.CatchUpResult> exchange =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return transport.requestCatchUp(request(), event -> {
                            events.add(event);
                            delivered.countDown();
                        });
                    } catch (IOException failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                });

        try {
            assertThat(firstEventWritten.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(delivered.await(5, TimeUnit.SECONDS))
                    .as("the first event reaches the sink while the matching end is withheld")
                    .isTrue();
            assertThat(exchange).isNotCompleted();
            assertThat(events).hasSize(1);
        } finally {
            allowEnd.countDown();
        }
        assertThat(exchange.get(5, TimeUnit.SECONDS))
                .isEqualTo(SubscriptionTransport.CatchUpResult.COMPLETE);
    }

    @Test
    void notFoundForCatchUpLeavesLivePollingRunning() throws Exception {
        assertUnsupportedPeerDoesNotDisruptLivePoll(Status.NOT_FOUND_404);
    }

    @Test
    void notImplementedForCatchUpLeavesLivePollingRunning() throws Exception {
        assertUnsupportedPeerDoesNotDisruptLivePoll(Status.NOT_IMPLEMENTED_501);
    }

    @Test
    void methodNotAllowedForCatchUpLeavesLivePollingRunning() throws Exception {
        assertUnsupportedPeerDoesNotDisruptLivePoll(Status.METHOD_NOT_ALLOWED_405);
    }

    @Test
    void responseWithoutMatchingEndFailsInsteadOfCompleting() throws Exception {
        SubscriptionEvent replayed = event(REQUESTED, "truncated");
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new CatchUpEventFrame(REQUEST_ID, replayed).encode()))))
                .build().start();
        transport = connect();
        List<SubscriptionEvent> delivered = new ArrayList<>();

        assertThatThrownBy(() -> transport.requestCatchUp(request(), delivered::add))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("before its matching end frame");
        assertThat(delivered).containsExactly(replayed);
    }

    @Test
    void holdsTheExchangeOnLaneBackpressureBeforeReadingTheEnd() throws Exception {
        CountDownLatch firstEventWritten = new CountDownLatch(1);
        CountDownLatch sendRemainingFrames = new CountDownLatch(1);
        CountDownLatch remainingFramesWritten = new CountDownLatch(1);
        CountDownLatch laneEntered = new CountDownLatch(1);
        CountDownLatch releaseLane = new CountDownLatch(1);
        SubscriptionEvent first = event(REQUESTED, "first");
        SubscriptionEvent second = event(REQUESTED, "second");
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService((request, sink) -> {
                    sink.write(new CatchUpEventFrame(REQUEST_ID, first).encode());
                    firstEventWritten.countDown();
                    await(sendRemainingFrames, "permission to write remaining frames");
                    sink.write(new CatchUpEventFrame(REQUEST_ID, second).encode());
                    sink.write(new CatchUpEndFrame(REQUEST_ID).encode());
                    remainingFramesWritten.countDown();
                })))
                .build().start();
        transport = connect();
        List<SubscriptionEvent> delivered = new CopyOnWriteArrayList<>();
        AtomicInteger count = new AtomicInteger();
        CompletableFuture<SubscriptionTransport.CatchUpResult> exchange =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return transport.requestCatchUp(request(), event -> {
                            delivered.add(event);
                            if (count.incrementAndGet() == 1) {
                                laneEntered.countDown();
                                try {
                                    await(releaseLane, "release of blocked catch-up lane");
                                } catch (IOException failure) {
                                    throw new java.util.concurrent.CompletionException(failure);
                                }
                            }
                        });
                    } catch (IOException failure) {
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                });

        try {
            assertThat(firstEventWritten.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(laneEntered.await(5, TimeUnit.SECONDS)).isTrue();
            sendRemainingFrames.countDown();
            assertThat(remainingFramesWritten.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> exchange.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
        } finally {
            sendRemainingFrames.countDown();
            releaseLane.countDown();
        }
        assertThat(exchange.get(5, TimeUnit.SECONDS))
                .isEqualTo(SubscriptionTransport.CatchUpResult.COMPLETE);
        assertThat(delivered).containsExactly(first, second);
    }

    @Test
    void unknownFrameKindFailsEvenWhenFollowedByMatchingEnd() throws Exception {
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        unknownFrame(127), new CatchUpEndFrame(REQUEST_ID).encode()))))
                .build().start();
        transport = connect();

        assertThatThrownBy(() -> transport.requestCatchUp(request(), event -> { }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected frame kind 127");
    }

    private void assertUnsupportedPeerDoesNotDisruptLivePoll(Status catchUpStatus)
            throws Exception {
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger catchUps = new AtomicInteger();
        SubscriptionEvent live = event(REQUESTED, "live");
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new LegacyPeer(catchUpStatus, polls, catchUps, framed(live.encode()))))
                .build().start();
        transport = connect();
        List<io.github.huyz0.os.biningester.client.Delivery> liveDeliveries =
                new CopyOnWriteArrayList<>();
        try (AutoCloseable ignored = transport.subscribe(REQUESTED, liveDeliveries::add)) {
            await(() -> polls.get() > 0 && !liveDeliveries.isEmpty(), "initial live poll");
            int beforeCatchUp = polls.get();

            SubscriptionTransport.CatchUpResult result = transport.requestCatchUp(
                    request(), event -> { throw new AssertionError("legacy peer returned an event"); });

            assertThat(result).isEqualTo(SubscriptionTransport.CatchUpResult.UNSUPPORTED);
            assertThat(catchUps).hasValue(1);
            await(() -> polls.get() > beforeCatchUp, "live poll after unsupported catch-up");
            assertThat(transport.pollFailures(HttpSubscriptionTransport.PollFailure.REFUSED))
                    .isZero();
        }
    }

    private HttpSubscriptionTransport connect() {
        return new HttpSubscriptionTransport("http://localhost:" + server.port(), () -> { },
                Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofMillis(20));
    }

    private static CatchUpRequestFrame request() {
        return new CatchUpRequestFrame(REQUEST_ID,
                List.of(new CatchUpRequestFrame.Stream(REQUESTED, 41)));
    }

    private static SubscriptionEvent event(RunKey key, String segment) {
        return new SubscriptionEvent("session", 1, 1, key, segment, 41, 1,
                FetchMode.INLINE, new byte[] {1});
    }

    private static byte[] framed(byte[] body) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(output)) {
            data.writeInt(body.length);
            data.write(body);
        }
        return output.toByteArray();
    }

    private static byte[] unknownFrame(int kind) {
        return ByteBuffer.allocate(9)
                .putInt(CatchUpRequestFrame.MAGIC)
                .putInt(CatchUpRequestFrame.VERSION_1)
                .put((byte) kind)
                .array();
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    private static void await(CountDownLatch latch, String what) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting for " + what);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for " + what, interrupted);
        }
    }

    private static final class LegacyPeer implements HttpService {
        private final Status catchUpStatus;
        private final AtomicInteger polls;
        private final AtomicInteger catchUps;
        private final byte[] liveAnswer;

        private LegacyPeer(Status catchUpStatus, AtomicInteger polls, AtomicInteger catchUps,
                byte[] liveAnswer) {
            this.catchUpStatus = catchUpStatus;
            this.polls = polls;
            this.catchUps = catchUps;
            this.liveAnswer = liveAnswer;
        }

        @Override
        public void routing(HttpRules rules) {
            String path = HttpSubscriptionTransport.SUBSCRIBE_PREFIX + "{indexUuid}/{partition}";
            rules.get(path, this::poll).post(path, this::catchUp);
        }

        private void poll(ServerRequest request, ServerResponse response) {
            polls.incrementAndGet();
            response.send(liveAnswer);
        }

        private void catchUp(ServerRequest request, ServerResponse response) {
            catchUps.incrementAndGet();
            response.status(catchUpStatus).send("catch-up unsupported");
        }
    }
}
