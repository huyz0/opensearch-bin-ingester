// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The production {@code proxy} fetch: one GET of the ingester's segment route
 * (M10.2, ADR-0073).
 *
 * <p>⚠️ THE STAND-IN SERVER ANSWERS ONE KEY, EXACTLY, and records what it was
 * asked: a key sent un-encoded loses its slashes' meaning to a proxy, and a
 * missing {@code az} makes every byte count as cross-AZ, so both are asserted
 * on the request the server actually received.
 */
class HttpSegmentSourceProxyTest {

    private static final String KEY = "bins/cluster-a/data/2026/09/27/10/seg one+two.bseg";
    private static final byte[] BYTES = "the segment".getBytes(StandardCharsets.UTF_8);

    private HttpServer server;
    private final List<Map<String, String>> asked = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(SegmentFetchRoute.PATH, exchange -> {
            Map<String, String> query = java.util.Arrays.stream(
                    exchange.getRequestURI().getRawQuery().split("&"))
                    .map(p -> p.split("=", 2))
                    .collect(Collectors.toMap(p -> p[0],
                            p -> URLDecoder.decode(p[1], StandardCharsets.UTF_8)));
            Map<String, String> seen = new java.util.HashMap<>(query);
            seen.put("path", exchange.getRequestURI().getRawPath());
            asked.add(seen);
            boolean served = KEY.equals(query.get(SegmentFetchRoute.KEY_PARAM));
            boolean partial = (KEY + "-partial").equals(query.get(SegmentFetchRoute.KEY_PARAM));
            byte[] body = served || partial ? BYTES
                    : "no such segment".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(served ? 200 : partial ? 206 : 404, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void aProxiedSegmentIsFetchedByItsExactKeyNamingTheConsumersZone() throws Exception {
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5), start(), "az-b");

        assertThat(source.fetchSegment(KEY)).isEqualTo(BYTES);
        assertThat(asked).hasSize(1);
        assertThat(asked.getFirst()).containsEntry(SegmentFetchRoute.KEY_PARAM, KEY)
                .containsEntry(SegmentFetchRoute.AZ_PARAM, "az-b");
    }

    @Test
    void anAnswerOtherThan200IsAFailureAndNeverAnEmptySegment() throws Exception {
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5), start(), "az-b");

        assertThatThrownBy(() -> source.fetchSegment(KEY + "-absent"))
                .isInstanceOf(IOException.class).hasMessageContaining("404");
    }

    @Test
    void anyAnswerButTwoHundredIsRefusedEvenASuccessfulOne() throws Exception {
        // ⚠️ A 206 with bytes is a success status and still not the segment:
        // a partial body decoded as a whole one is the silent case.
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5), start(), "az-b");

        assertThatThrownBy(() -> source.fetchSegment(KEY + "-partial"))
                .isInstanceOf(IOException.class).hasMessageContaining("206");
    }

    @Test
    void anUnreachableIngesterIsAnIOExceptionNamingTheKey() throws Exception {
        String endpoint = start();
        server.stop(0);
        server = null;
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(2), endpoint, "az-b");

        assertThatThrownBy(() -> source.fetchSegment(KEY))
                .as("a checked failure the consumer handles, never an unchecked escape")
                .isInstanceOf(IOException.class).hasMessageContaining(KEY);
    }

    @Test
    void aTrailingSlashAndABlankZoneAreNormalisedAsTheSubscriptionDoes() throws Exception {
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5), start() + "/",
                "  ");

        assertThat(source.fetchSegment(KEY)).isEqualTo(BYTES);
        assertThat(asked.getFirst()).containsEntry("path", SegmentFetchRoute.PATH)
                .as("a blank zone is not sent as a zone named \"\"")
                .doesNotContainKey(SegmentFetchRoute.AZ_PARAM);
    }
}
