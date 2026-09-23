// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.StreamFraming;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.github.huyz0.os.biningester.ingest.DurableCatchUpResponder;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CatchUpServiceTest {

    private static final UUID REQUEST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final RunKey KEY = new RunKey(
            UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), 7);
    private static final String TEST_PATH = HttpSubscriptionTransport.SUBSCRIBE_PREFIX
            + KEY.indexId() + "/" + KEY.partitionId();
    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void requestIsDecodedAndResponseFramesAreLengthFramed() throws Exception {
        var received = new AtomicReference<CatchUpRequestFrame>();
        var calls = new AtomicInteger();
        var event = new CatchUpEventFrame(REQUEST, new SubscriptionEvent("s", 1, 1, KEY,
                "seg", 41, 1, FetchMode.INLINE, new byte[] {1}));
        var end = new CatchUpEndFrame(REQUEST);
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> {
                    calls.incrementAndGet();
                    received.set(request);
                    return List.of(event.encode(), end.encode());
                }))).build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(200);
            var body = response.entity().as(byte[].class);
            var input = new ByteArrayInputStream(body);
            assertThat(StreamFraming.readFrame(input)).isEqualTo(event.encode());
            assertThat(StreamFraming.readFrame(input)).isEqualTo(end.encode());
            assertThat(StreamFraming.readFrame(input)).isNull();
        }
        assertThat(received.get()).isEqualTo(request);
        assertThat(calls).hasValue(1);
    }

    @Test
    void malformedRequestIsRefusedBeforeTheResponderRuns() throws Exception {
        var called = new AtomicReference<Boolean>(false);
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> {
                    called.set(true);
                    return List.of();
                }))).build().start();

        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(new byte[] {1, 2, 3})) {
            assertThat(response.status().code()).isEqualTo(400);
        }
        assertThat(called).hasValue(false);
    }

    @Test
    void oversizedRequestIsRefusedBeforeTheResponderRuns() throws Exception {
        var called = new AtomicReference<Boolean>(false);
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> {
                    called.set(true);
                    return List.of();
                }))).build().start();

        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH)
                .submit(new byte[(int) CatchUpService.MAX_REQUEST_BYTES + 1])) {
            assertThat(response.status().code()).isEqualTo(413);
        }
        assertThat(called).hasValue(false);
    }

    @Test
    void oversizedResponseIsRefusedBeforeItIsCopiedToTheAnswer() throws Exception {
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new byte[(int) CatchUpService.MAX_RESPONSE_BYTES]))))
                .build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(413);
        }
    }

    @Test
    void responderFailureIsTransientServerError() throws Exception {
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> {
                    throw new java.io.IOException("store unavailable");
                }))).build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(503);
        }
    }

    @Test
    void deterministicResponseBudgetFailureIsPayloadTooLarge() throws Exception {
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> {
                    throw new DurableCatchUpResponder.ResponseTooLargeException("too large");
                }))).build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(413);
        }
    }

    @Test
    void oversizedFrameIsRejectedBeforeWritingAnyBytes() {
        var output = new ByteArrayOutputStream();
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                CatchUpService.writeFrameWithinBudget(output, 0,
                        new byte[(int) CatchUpService.MAX_RESPONSE_BYTES]))
                .isInstanceOf(BodyTooLargeException.class);
        assertThat(output.size()).isZero();
    }

    @Test
    void individuallyValidFramesMayExceedTheFormerCumulativeResponseBudget() throws Exception {
        int each = ((int) CatchUpService.MAX_RESPONSE_BYTES - PollAnswer.FRAME_PREFIX_BYTES) / 2 + 1;
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new byte[each], new byte[each]))))
                .build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(200);
            assertThat(response.entity().as(byte[].class).length)
                    .isGreaterThan((int) CatchUpService.MAX_RESPONSE_BYTES);
        }
    }

    @Test
    void responseMaySpanMoreThanEightMiBAcrossSeveralFrames() throws Exception {
        int frameBytes = 4 << 20;
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new byte[frameBytes], new byte[frameBytes]))))
                .build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(TEST_PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(200);
            assertThat(response.entity().as(byte[].class).length)
                    .isGreaterThan((int) CatchUpService.MAX_RESPONSE_BYTES);
        }
    }

    @Test
    void firstFlushedFrameArrivesBeforeResponderCompletes() throws Exception {
        byte[] firstFrame = new byte[] {1, 2, 3};
        byte[] endFrame = new CatchUpEndFrame(REQUEST).encode();
        var responderBlocked = new CountDownLatch(1);
        var allowResponderToFinish = new CountDownLatch(1);
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService((CatchUpService.StreamingResponder) (request, sink) -> {
                    sink.write(firstFrame);
                    responderBlocked.countDown();
                    try {
                        if (!allowResponderToFinish.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("test did not release the responder");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException("responder was interrupted", interrupted);
                    }
                    sink.write(endFrame);
                }))).build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        HttpRequest httpRequest = HttpRequest.newBuilder(
                        URI.create("http://localhost:" + server.port() + TEST_PATH))
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.encode()))
                .build();
        HttpResponse<InputStream> response = HttpClient.newHttpClient()
                .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofInputStream())
                .get(5, TimeUnit.SECONDS);

        try (InputStream body = response.body()) {
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(responderBlocked.await(2, TimeUnit.SECONDS)).isTrue();
            ByteArrayOutputStream expected = new ByteArrayOutputStream();
            PollAnswer.writeFrame(expected, firstFrame);
            CompletableFuture<byte[]> firstBytes = CompletableFuture.supplyAsync(() -> {
                try {
                    return body.readNBytes(expected.size());
                } catch (IOException failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            });
            try {
                assertThat(firstBytes.get(2, TimeUnit.SECONDS)).containsExactly(expected.toByteArray());
                assertThat(allowResponderToFinish.getCount()).isEqualTo(1L);
            } finally {
                allowResponderToFinish.countDown();
            }
        } finally {
            allowResponderToFinish.countDown();
        }
    }
}
