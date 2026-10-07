// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.ForwardingIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.http.HeaderNames;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every body refused for its size closes the connection (M13.49, as M13.19
 * did for commits): kept alive, the server read the rest of the entity it
 * refused before the connection could carry anything else -- measured with
 * 256 MiB at 10 s (catch-up), 14 s (register), 13 s (progress) and 4 s (the
 * durable-segment hint), and a 512 MiB bulk at 10.4 s, where closing answered
 * in 63-425 ms and 5.2 s. Each body here is just past its cap, small enough to
 * be read to the refusal, so the status and the header are what is asserted.
 */
class OversizedBodyClosesTest {

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private WebClient start(HttpService service) {
        server = WebServer.builder().port(0).routing(HttpRouting.builder().register(service))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    private static SubscriptionService subscriptions() {
        return new SubscriptionService(new SubscriptionHub(), new IndexCatalog(),
                new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofMinutes(30)), Clock.systemUTC());
    }

    private static void refusedAndClosed(WebClient client, String path, long cap) {
        byte[] body = new byte[(int) cap + (100 << 10)];
        try (var response = client.post(path).submit(body)) {
            assertThat(response.status().code()).isEqualTo(413);
            assertThat(response.headers().value(HeaderNames.CONNECTION))
                    .as("the connection closed, not read to the end").hasValue("close");
        }
    }

    @Test
    void aCATCHUPRequestPastItsCapClosesTheConnection() {
        refusedAndClosed(start(new CatchUpService(request -> List.of())),
                CatchUpService.PATH.replace("{indexUuid}", "AAAAAAAAAAAAAAAAAAAAAA")
                        .replace("{partition}", "0"), CatchUpService.MAX_REQUEST_BYTES);
    }

    @Test
    void aREGISTRATIONPastItsCapClosesTheConnection() {
        refusedAndClosed(start(subscriptions()), SubscriptionService.REGISTER_PATH,
                SubscriptionService.MAX_FRAME_BYTES);
    }

    @Test
    void aPROGRESSReportPastItsCapClosesTheConnection() {
        refusedAndClosed(start(subscriptions()), SubscriptionService.PROGRESS_PATH,
                SubscriptionService.MAX_FRAME_BYTES);
    }

    @Test
    void aDURABLESegmentHintPastItsCapClosesTheConnection() {
        refusedAndClosed(start(new DurableSegmentSignalService(new EndpointSliceView(),
                        (key, az) -> { })), DurableSegmentSignalService.PATH,
                io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame.MAX_FRAME_BYTES);
    }

    @Test
    void anACCEPTEDBulkKeepsItsConnection() {
        // ⚠️ ONLY A 413 CLOSES (review round 1, T1): every producer's bulk
        // paying a new connection would be the cost of closing on everything.
        WebClient client = start(new BulkService(new ForwardingIngest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource source) throws IOException {
                int[] n = {0};
                source.forEachRecord(r -> n[0]++);
                return new AppendResult(n[0], 0L, n[0] - 1L);
            }

            @Override
            public void close() {
            }
        }, new Principal("cluster-a", "producer-1", Set.of("logs"))));
        try (var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"a\"}}\n1\n")) {
            assertThat(response.status().code()).isEqualTo(202);
            assertThat(response.headers().value(HeaderNames.CONNECTION))
                    .as("kept alive").isNotEqualTo(java.util.Optional.of("close"));
        }
    }

    @Test
    void aBULKRefusedForItsSizeClosesTheConnection() {
        // ⚠️ THE RECORD CEILING, ~46 MiB, rather than the 256 MiB byte cap: the
        // same 413 through the same answer, at a fraction of the bytes.
        WebClient client = start(new BulkService(new ForwardingIngest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource source) throws IOException {
                int[] n = {0};
                source.forEachRecord(r -> n[0]++);
                return new AppendResult(n[0], 0L, n[0] - 1L);
            }

            @Override
            public void close() {
            }
        }, new Principal("cluster-a", "producer-1", Set.of("logs"))));
        byte[] record = "{\"index\":{\"_id\":\"a\"}}\n1\n".getBytes(StandardCharsets.UTF_8);
        try (var response = client.post("/logs/_bulk").queryParam("partition", "0")
                .outputStream(out -> {
                    for (int i = 0; i <= BulkService.MAX_RECORDS; i++) {
                        out.write(record);
                    }
                    out.close();
                })) {
            assertThat(response.status().code()).isEqualTo(413);
            assertThat(response.headers().value(HeaderNames.CONNECTION))
                    .as("the connection closed, not read to the end").hasValue("close");
        }
    }
}
