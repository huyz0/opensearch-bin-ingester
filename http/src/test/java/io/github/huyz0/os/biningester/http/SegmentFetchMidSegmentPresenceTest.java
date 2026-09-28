// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A store failure AFTER the first byte is sent never reaches
 * {@code answerFailure} (M11.14, H9): the response is committed, so asking
 * whether the object exists is a store request that can change nothing, and
 * a {@code 404} it chose would remember a segment that exists as absent.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentFetchMidSegmentPresenceTest {

    private static final String PREFIX = "bins/cluster-a";

    @Test
    void aFailureAfterTheFirstByteAsksNothingOfTheStore() throws Exception {
        byte[] part = new byte[70_000];
        AtomicInteger asked = new AtomicInteger();
        WebServer server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new SegmentFetchService(PREFIX,
                        (key, sink) -> {
                            sink.write(part, 0, part.length);
                            throw new IOException("the store failed mid-segment");
                        },
                        key -> {
                            asked.incrementAndGet();
                            return false;
                        }, new CrossAzBytes("az-a"))))
                .build().start();
        try {
            String key = new SegmentKey(PREFIX, 1_790_000_000_000L, "pod7", 1, 96).key();
            URI uri = URI.create("http://localhost:" + server.port() + SegmentFetchRoute.PATH
                    + "?" + SegmentFetchRoute.KEY_PARAM + "="
                    + URLEncoder.encode(key, StandardCharsets.UTF_8));
            HttpClient http = HttpClient.newHttpClient();

            assertThatThrownBy(() -> http.send(HttpRequest.newBuilder(uri).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray()))
                    .as("the premise: the stream is aborted, not answered")
                    .isInstanceOf(IOException.class);
            assertThat(asked)
                    .as("⚠️ COMMITTED: presence is asked only for a failure before any byte")
                    .hasValue(0);
        } finally {
            server.stop();
        }
    }
}
