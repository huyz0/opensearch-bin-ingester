// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.http.HeaderNames;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every {@code 429} carries {@code Retry-After}, set in one place (M11.6, M11
 * criterion 9; research 14 §3, ADR-0010): a bare 429 is retried at whatever
 * rate the producer's loop runs.
 */
class BulkServiceRetryAfterTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static final class AcceptingIngest implements Ingest {
        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) {
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource source) {
            return append(principal, index, partition, source);
        }

        @Override
        public void close() {
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws IOException {
            try { // the removed default's behaviour: buffered once the append returns (M12.2)
                return append(principal, index, partition, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
                byte lane, RecordSource records, Runnable buffered) throws IOException {
            try { // the removed default's behaviour: buffered once the append returns (M12.2)
                return appendRouted(principal, indexOrAlias, routing, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public String concreteIndex(String indexOrAlias) {
            return indexOrAlias; // no catalog in this double (M12.2)
        }
    }

    @Test
    void aLaneAdmissionRefusalTellsTheProducerWhenToRetry() {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(new AcceptingIngest(),
                        PRINCIPAL, new DrainGate(), admission)))
                .build().start();
        admission.tryAcquire((byte) 0).orElseThrow();

        try (HttpClientResponse refused = WebClient.builder()
                .baseUri("http://localhost:" + server.port()).build()
                .post("/logs/_bulk").queryParam("partition", "0")
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n")) {
            assertThat(refused.status().code()).as("the premise: saturated").isEqualTo(429);
            assertThat(refused.headers().first(HeaderNames.RETRY_AFTER))
                    .as("⚠️ A 429 WITH NO RETRY-AFTER IS RETRIED AT THE PRODUCER's OWN RATE")
                    .hasValue(Long.toString(BulkService.ADMISSION_RETRY_AFTER_SECONDS));
        }
    }
}
