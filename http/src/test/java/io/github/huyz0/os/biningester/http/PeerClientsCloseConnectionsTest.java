// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every ingester-side {@code WebClient} used from more than one thread closes
 * its connection after each request (M10.36; the race is M10.35's -- see
 * {@code SharedClientsCloseConnectionsTest} in {@code client}): the sequencer
 * transport and the durable-segment hint are per-endpoint clients shared by
 * every request thread, and the EndpointSlice watch is on Helidon's JVM-wide
 * cache. ⚠️ ASSERTED ON THE WIRE, deterministically, as the header each
 * request carries.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class PeerClientsCloseConnectionsTest {

    private HttpServer server;
    private final List<String> connection = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private int serve(int status) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String header = exchange.getRequestHeaders().getFirst("Connection");
            connection.add(header == null ? "(none)" : header.toLowerCase(java.util.Locale.ROOT));
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
        return server.getAddress().getPort();
    }

    @Test
    void theSequencerTransportClosesEachRequestsConnection() throws Exception {
        int port = serve(500);
        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            try {
                transport.drain("http://127.0.0.1:" + port, "podb");
            } catch (Exception refused) {
                // the answer does not matter here; the request's header does
            }
        }
        assertThat(connection).as("⚠️ ONE CLIENT PER PEER, SHARED BY EVERY REQUEST THREAD")
                .containsExactly("close");
    }

    @Test
    void theDurableSegmentHintClosesEachRequestsConnection() throws Exception {
        int port = serve(204);
        DurableSegmentSignalSender sender = new DurableSegmentSignalSender(
                new CrossAzBytes("az-a"), port);
        String key = new SegmentKey("bins/c", 1_700_000_000_000L, "writera", 1, 48).key();

        sender.send(new DurableSegmentSignalFrame("writera", "az-a", key),
                List.of(new EndpointSliceView.Endpoint("nodeb1", "127.0.0.1", "az-b")));

        assertThat(connection).containsExactly("close");
    }

    @Test
    void theEndpointSliceWatchClosesItsConnection() throws Exception {
        int port = serve(200);
        try (EndpointSliceWatch watch = new EndpointSliceWatch("http://127.0.0.1:" + port,
                "ingest", "ingester", Optional::empty, new EndpointSliceView()).start()) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (connection.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }
        assertThat(connection).isNotEmpty().allMatch("close"::equals);
    }
}
