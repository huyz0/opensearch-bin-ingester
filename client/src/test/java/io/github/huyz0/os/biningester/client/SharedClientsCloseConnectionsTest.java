// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every consumer-side {@code WebClient} used from more than one thread closes
 * its connection after each request (M10.36): Helidon 4.3.0 re-queues a
 * kept-alive connection BEFORE it starts the idle monitor that reads one byte,
 * so a second thread taking it in that window loses its answer's first byte
 * ("Protocol is not HTTP: TTP", M10.35). A connection never re-queued cannot
 * be taken in that window. ⚠️ ASSERTED ON THE WIRE, as the header the request
 * carries, because the race itself is a few instructions wide and a stress
 * case would be red only sometimes.
 */
class SharedClientsCloseConnectionsTest {

    private HttpServer server;
    private final List<String> connection = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String serve(int status, byte[] body) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String header = exchange.getRequestHeaders().getFirst("Connection");
            connection.add(header == null ? "(none)" : header.toLowerCase(java.util.Locale.ROOT));
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void theNodesSegmentSourceClosesEachRouteAndGrantFetchsConnection() throws Exception {
        String endpoint = serve(200, new byte[] {1, 2, 3});
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5), endpoint, "az-a");

        source.fetchSegment("bins/c/data/seg.bseg");
        source.fetch(new Grant(endpoint + "/object", Instant.EPOCH.plusSeconds(60)));

        assertThat(connection)
                .as("⚠️ ONE CLIENT FOR EVERY RUN's PROXY FETCH ON A NODE: no connection is "
                        + "returned to a queue another thread can take it from")
                .containsExactly("close", "close");
    }

    @Test
    void theTransportsRegistrationAndProgressPostsCloseTheirConnections() throws Exception {
        String endpoint = serve(204, new byte[0]);
        HttpSubscriptionTransport transport = new HttpSubscriptionTransport(endpoint, () -> { },
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(5));
        try {
            transport.register(IndexRegistration.unsplit("AAAAAAAAQACAAAAAAAAAqg", "logs", 4));
            transport.report(new ConsumerProgress(List.of(
                    new ConsumerProgress.Entry("AAAAAAAAQACAAAAAAAAAqg", 0, "alloc-a", 1L))));
        } finally {
            transport.close();
        }

        assertThat(connection)
                .as("the registrar's and the reporter's threads share this client")
                .containsExactly("close", "close");
    }
}
