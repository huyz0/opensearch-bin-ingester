// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@code /seg} serves a fast key (ADR-0082 §6; M13.30l): from the copy pushed
 * to this pod, {@code 404} without one, never by asking the store -- the key
 * names no object -- and a fast key of another deployment is no key of this
 * one.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentFetchFastKeyTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final RunKey S = RunKey.ofIndexUuid("nVzgup36TLqWp7VBBREj1w", 0);

    private static HttpResponse<byte[]> get(WebServer server, String key) throws Exception {
        URI uri = URI.create("http://localhost:" + server.port() + SegmentFetchRoute.PATH
                + "?" + SegmentFetchRoute.KEY_PARAM + "="
                + URLEncoder.encode(key, StandardCharsets.UTF_8));
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(uri).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void aFASTKeyIsServedFromItsCopyAndIsA404WithoutOneTheStoreNeverAsked() throws Exception {
        String held = FastKey.of(PREFIX, 3, 0, S, 10);
        String gone = FastKey.of(PREFIX, 3, 0, S, 20);
        String foreign = FastKey.of("bins/cluster-b", 3, 0, S, 10);
        AtomicInteger store = new AtomicInteger();
        Map<String, byte[]> copies = Map.of(held, new byte[] {7, 8, 9});
        WebServer server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new SegmentFetchService(PREFIX,
                        (key, sink) -> store.incrementAndGet(),
                        key -> store.incrementAndGet() < 0, new CrossAzBytes("az-a"),
                        key -> Optional.ofNullable(copies.get(key)))))
                .build().start();
        try {
            HttpResponse<byte[]> served = get(server, held);
            HttpResponse<byte[]> missing = get(server, gone);
            HttpResponse<byte[]> other = get(server, foreign);

            assertThat(served.statusCode()).isEqualTo(200);
            assertThat(served.body()).containsExactly(7, 8, 9);
            assertThat(missing.statusCode()).isEqualTo(404);
            assertThat(other.statusCode()).as("another deployment's").isEqualTo(400);
            assertThat(store).as("never the store").hasValue(0);
        } finally {
            server.stop();
        }
    }
}
