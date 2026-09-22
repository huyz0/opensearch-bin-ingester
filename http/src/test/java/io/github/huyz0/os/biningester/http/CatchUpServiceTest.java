// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.StreamFraming;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
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
                .build().post(CatchUpService.PATH).submit(request.encode())) {
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
                .build().post(CatchUpService.PATH).submit(new byte[] {1, 2, 3})) {
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
                .build().post(CatchUpService.PATH)
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
                .build().post(CatchUpService.PATH).submit(request.encode())) {
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
                .build().post(CatchUpService.PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(503);
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
    void individuallyValidFramesCannotExceedTheCumulativeResponseBudget() throws Exception {
        int each = ((int) CatchUpService.MAX_RESPONSE_BYTES - PollAnswer.FRAME_PREFIX_BYTES) / 2 + 1;
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .register(new CatchUpService(request -> List.of(
                        new byte[each], new byte[each]))))
                .build().start();

        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));
        try (var response = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build().post(CatchUpService.PATH).submit(request.encode())) {
            assertThat(response.status().code()).isEqualTo(413);
        }
    }
}
