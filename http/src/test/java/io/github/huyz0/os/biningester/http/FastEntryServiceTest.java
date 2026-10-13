// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The leader's exposed entry by fast key (M13.30o; ADR-0082 §2, §6): its bytes
 * with the epoch they were served under, {@code 404} for a key it does not
 * hold exposed, and its caller named and bound to its certificate as a frame's
 * sender is -- refused under mutual TLS without one.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastEntryServiceTest {

    private static HttpResponse<byte[]> get(WebServer server, String query) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                "http://localhost:" + server.port() + FastEntryService.PATH + "?" + query))
                .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static WebServer serving(PeerBinding binding) {
        Map<String, FastEntryService.Served> held = Map.of("k",
                new FastEntryService.Served(7, new byte[] {4, 5}));
        return WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new FastEntryService(key -> Optional.ofNullable(held.get(key)), binding)))
                .build().start();
    }

    @Test
    void anEXPOSEDEntryIsServedWithItsEpochAndAnyOtherKeyIsA404() throws Exception {
        WebServer server = serving(PeerBinding.off());
        try {
            HttpResponse<byte[]> served = get(server, "key=k&pod=uid-b");
            HttpResponse<byte[]> absent = get(server, "key=other&pod=uid-b");
            HttpResponse<byte[]> unnamed = get(server, "key=k");

            assertThat(served.statusCode()).isEqualTo(200);
            assertThat(served.body()).containsExactly(4, 5);
            assertThat(served.headers().firstValue(FastEntryService.EPOCH_HEADER))
                    .hasValue("7");
            assertThat(absent.statusCode()).isEqualTo(404);
            assertThat(unnamed.statusCode()).as("the asking pod named").isEqualTo(400);
        } finally {
            server.stop();
        }
    }

    @Test
    void aCALLERWithNoCertificateIsRefusedUnderMutualTls() throws Exception {
        WebServer server = serving(PeerBinding.within("bins/cluster-a"));
        try {
            assertThat(get(server, "key=k&pod=uid-b").statusCode()).isEqualTo(403);
        } finally {
            server.stop();
        }
    }
}
