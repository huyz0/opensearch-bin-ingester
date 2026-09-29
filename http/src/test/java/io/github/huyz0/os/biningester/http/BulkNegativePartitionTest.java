// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * M12.19c (M11.24c T1): a negative {@code partition} is refused 400 before
 * anything reaches the ingester. {@code PlacementParser}'s {@code partition < 0}
 * refusal was unpinned, as it had been in {@code BulkService}: without it a
 * producer's -1 reaches the ingest seam as a partition number.
 */
class BulkNegativePartitionTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final String BODY = "{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n";

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Counts every call that reached the seam. */
    private static final class Counting implements Ingest {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) {
            calls.incrementAndGet();
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, RecordSource source) {
            calls.incrementAndGet();
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws IOException {
            try {
                return append(principal, index, partition, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
                byte lane, RecordSource records, Runnable buffered) throws IOException {
            try {
                return appendRouted(principal, indexOrAlias, routing, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public String concreteIndex(String indexOrAlias) {
            return indexOrAlias;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aNegativePartitionIsRefused400AndNeverReachesTheIngester() {
        Counting ingest = new Counting();
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                .build().start();

        try (HttpClientResponse answer = WebClient.builder()
                .baseUri("http://localhost:" + server.port()).build()
                .post("/logs/_bulk").queryParam("partition", "-1").submit(BODY)) {
            assertThat(answer.status().code()).as("⚠️ A PRODUCER's ERROR: 400, not retried")
                    .isEqualTo(400);
            assertThat(answer.as(String.class)).contains("'partition' is not negative");
        }
        assertThat(ingest.calls).as("refused before the seam").hasValue(0);
    }
}
