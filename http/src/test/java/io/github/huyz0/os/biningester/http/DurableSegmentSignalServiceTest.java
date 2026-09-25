// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DurableSegmentSignalServiceTest {
    private static final long NOW = 1_700_000_000_000L;

    private static EndpointSliceView members() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":["
                + "{\"addresses\":[\"10.0.0.1\"],\"zone\":\"az-a\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"writera\",\"uid\":\"uid-writera\"}},"
                + "{\"addresses\":[\"10.0.0.2\"],\"zone\":\"az-b\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"writerb\",\"uid\":\"uid-writerb\"}}]}} ");
        return view;
    }

    private static String key(String writer) {
        return new SegmentKey("bins/c", NOW, writer, 1, 48).key();
    }

    private static byte[] frame(String writer, String az, String key) {
        return new DurableSegmentSignalFrame(writer, az, key).encode();
    }

    @Test
    void validSourceWriterAzAndKeyAreDeliveredToThePrefetchHandler() throws IOException {
        var calls = new AtomicInteger();
        var received = new AtomicReference<String>();
        DurableSegmentSignalService service = new DurableSegmentSignalService(members(),
                (key, az) -> {
                    calls.incrementAndGet();
                    received.set(key + "@" + az);
                });

        assertThat(service.accept("10.0.0.1", frame("writera", "az-a", key("writera"))))
                .isTrue();
        assertThat(calls).hasValue(1);
        assertThat(received).hasValue(key("writera") + "@az-a");
    }

    @Test
    void aDifferentSocketAddressClaimingAKnownWriterIsRejectedBeforeAnyGet() throws IOException {
        var calls = new AtomicInteger();
        DurableSegmentSignalService service = new DurableSegmentSignalService(members(),
                (key, az) -> calls.incrementAndGet());

        assertThat(service.accept("10.0.0.99", frame("writera", "az-a", key("writera"))))
                .isFalse();
        assertThat(calls).hasValue(0);
    }

    @Test
    void aKeyNamingAnotherPodIsRejectedBeforeAnyGet() throws IOException {
        var calls = new AtomicInteger();
        DurableSegmentSignalService service = new DurableSegmentSignalService(members(),
                (key, az) -> calls.incrementAndGet());

        assertThat(service.accept("10.0.0.1", frame("writera", "az-a", key("writerb"))))
                .isFalse();
        assertThat(calls).hasValue(0);
    }

    @Test
    void aKeyOutsideTheSegmentGrammarIsRejectedBeforeAnyGet() throws IOException {
        var calls = new AtomicInteger();
        DurableSegmentSignalService service = new DurableSegmentSignalService(members(),
                (key, az) -> calls.incrementAndGet());

        assertThat(service.accept("10.0.0.1", frame("writera", "az-a", "external/object")))
                .isFalse();
        assertThat(calls).hasValue(0);
    }

    @Test
    void theClaimedAzMustMatchTheCurrentReadyEndpoint() throws IOException {
        var calls = new AtomicInteger();
        DurableSegmentSignalService service = new DurableSegmentSignalService(members(),
                (key, az) -> calls.incrementAndGet());

        assertThat(service.accept("10.0.0.1", frame("writera", "az-b", key("writera"))))
                .isFalse();
        assertThat(calls).hasValue(0);
    }

    @Test
    void httpRouteAuthenticatesTheDirectSocketPeer() throws Exception {
        var calls = new AtomicInteger();
        var loopback = new EndpointSliceView();
        loopback.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":[{\"addresses\":[\"127.0.0.1\"],\"zone\":\"az-a\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"writera\",\"uid\":\"uid-writera\"}}]}} ");
        var server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new DurableSegmentSignalService(loopback,
                        (key, az) -> calls.incrementAndGet())))
                .build().start();
        try {
            try (var response = WebClient.builder()
                    .baseUri("http://127.0.0.1:" + server.port()).build()
                    .post(DurableSegmentSignalService.PATH)
                    .headers(headers -> headers.set(
                            io.helidon.http.HeaderNames.create("X-Forwarded-For"), "203.0.113.99"))
                    .submit(frame("writera", "az-a", key("writera")))) {
                assertThat(response.status().code()).isEqualTo(204);
            }
        } finally {
            server.stop();
        }
        assertThat(calls).hasValue(1);
    }

    @Test
    void httpRouteBoundsTheBodyBeforeTheHandlerCanRun() throws Exception {
        var calls = new AtomicInteger();
        var loopback = new EndpointSliceView();
        loopback.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":[{\"addresses\":[\"127.0.0.1\"],\"zone\":\"az-a\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"writera\",\"uid\":\"uid-writera\"}}]}} ");
        var server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new DurableSegmentSignalService(loopback,
                        (key, az) -> calls.incrementAndGet())))
                .build().start();
        try {
            try (var response = WebClient.builder()
                    .baseUri("http://127.0.0.1:" + server.port()).build()
                    .post(DurableSegmentSignalService.PATH)
                    .submit(new byte[DurableSegmentSignalFrame.MAX_FRAME_BYTES + 1])) {
                assertThat(response.status().code()).isEqualTo(413);
            }
        } finally {
            server.stop();
        }
        assertThat(calls).hasValue(0);
    }
}
