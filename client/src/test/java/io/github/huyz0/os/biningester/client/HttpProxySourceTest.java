// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The consumer's GET of the ingester's segment route (M10.2, ADR-0073). */
@Timeout(30)
class HttpProxySourceTest {

    private static final String KEY = "bins/cluster-a/data/2026/09/26/10/seg-h96.bseg";

    private HttpServer server;
    private final AtomicInteger gets = new AtomicInteger();
    private volatile int status = 200;
    private volatile String path;
    private volatile String query;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            gets.incrementAndGet();
            path = exchange.getRequestURI().getPath();
            String raw = exchange.getRequestURI().getRawQuery();
            query = raw == null ? "" : URLDecoder.decode(raw, StandardCharsets.UTF_8);
            byte[] body = "segment-bytes".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, status == 200 ? body.length : -1);
            if (status == 200) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void aFetchIsOneGetOfTheRouteNamingTheKeyAndTheZone() throws Exception {
        HttpProxySource source = new HttpProxySource(endpoint(), Duration.ofSeconds(5), "az-b");

        assertThat(new String(source.fetch(KEY), StandardCharsets.UTF_8))
                .isEqualTo("segment-bytes");
        assertThat(gets.get()).as("one GET, no probe and no retry").isEqualTo(1);
        assertThat(path).isEqualTo(HttpProxySource.PATH);
        assertThat(query).contains(HttpProxySource.KEY_PARAM + "=" + KEY)
                .contains(HttpSubscriptionTransport.AZ_PARAM + "=az-b");
    }

    @Test
    void aConsumerWithNoZoneSendsNone() throws Exception {
        new HttpProxySource(endpoint(), Duration.ofSeconds(5), null).fetch(KEY);
        assertThat(query).doesNotContain(HttpSubscriptionTransport.AZ_PARAM + "=");

        new HttpProxySource(endpoint(), Duration.ofSeconds(5), " ").fetch(KEY);
        assertThat(query).doesNotContain(HttpSubscriptionTransport.AZ_PARAM + "=");
    }

    @Test
    void anythingButTwoHundredIsAFailureNeverAnEmptySegment() {
        status = 503;
        HttpProxySource source = new HttpProxySource(endpoint(), Duration.ofSeconds(5), "az-a");

        assertThatThrownBy(() -> source.fetch(KEY))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("503");
    }

    @Test
    void anUnreachableIngesterIsAnIOException() {
        int port = server.getAddress().getPort();
        server.stop(0);
        HttpProxySource source = new HttpProxySource("http://127.0.0.1:" + port,
                Duration.ofSeconds(2), "az-a");

        assertThatThrownBy(() -> source.fetch(KEY)).isInstanceOf(IOException.class);
    }
}
