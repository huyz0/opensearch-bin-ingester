// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.PendingPool;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An explicit-partition {@code _bulk} to an index not yet registered WAITS for
 * its registration as a routed one does, and answers {@code 503} when it does
 * not come -- never the {@code 500} of the composition root's "index is not
 * registered" (M10.30, FR-13, FR-16).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ExplicitPartitionUnregisteredTest {

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

    /** What the assembled pod's ingester does for an index it cannot resolve. */
    private static final class Unresolvable implements Ingest {
        final AtomicInteger called = new AtomicInteger();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) {
            called.incrementAndGet();
            throw new IllegalArgumentException("index is not registered: " + index);
        }

        @Override
        public void close() {
        }
    }

    private WebClient serve(Ingest ingest, IndexCatalog catalog, Duration wait) {
        RoutedIngest routed = new RoutedIngest(ingest, catalog,
                new PendingPool(Clock.systemUTC(), wait, 1 << 20), wait, Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(routed, PRINCIPAL)))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    @Test
    void anExplicitPartitionToAnIndexNeverRegisteredIs503AndNeverReachesTheIngester() {
        Unresolvable ingest = new Unresolvable();
        WebClient client = serve(ingest, new IndexCatalog(), Duration.ofMillis(100));

        try (HttpClientResponse answer = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(BODY)) {
            assertThat(answer.status().code())
                    .as("⚠️ 503, WHICH A PRODUCER RETRIES: a 500 reads as this ingester's bug")
                    .isEqualTo(503);
            assertThat(answer.as(String.class)).contains("logs").contains("not registered");
        }
        assertThat(ingest.called).as("waited for, not passed through to be refused")
                .hasValue(0);
    }

    @Test
    void anExplicitPartitionWaitingForItsIndexIsWrittenWhenTheRegistrationArrives()
            throws Exception {
        AtomicInteger written = new AtomicInteger();
        IndexCatalog catalog = new IndexCatalog();
        // ⚠️ REFUSES AN INDEX ITS CATALOG DOES NOT KNOW, as the assembled pod's does.
        Ingest ingest = new Ingest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource records) throws java.io.IOException {
                if (catalog.resolve(index).isEmpty()) {
                    throw new IllegalArgumentException("index is not registered: " + index);
                }
                records.forEachRecord(r -> written.incrementAndGet());
                return new AppendResult(1, 0L, 0L);
            }

            @Override
            public void close() {
            }
        };
        WebClient client = serve(ingest, catalog, Duration.ofSeconds(30));

        Thread registrar = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catalog.register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs", List.of(),
                    4, 4, 1, 1));
        });
        try (HttpClientResponse answer = client.post("/logs/_bulk")
                .queryParam("partition", "3").submit(BODY)) {
            assertThat(answer.status().code()).isEqualTo(202);
        }
        registrar.join();
        assertThat(written).hasValue(1);
    }
}
