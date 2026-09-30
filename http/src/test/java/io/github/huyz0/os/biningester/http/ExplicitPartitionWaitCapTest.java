// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.ForwardingIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.ingest.PendingPool;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.http.HeaderNames;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M12.10 (M10.30 P1): an explicit-partition write waiting for its index's
 * registration holds a lane admission permit, and nothing capped how many
 * waited per index -- a producer retrying a typo'd index could take lane share
 * from every other index. Past {@link RoutedIngest#MAX_EXPLICIT_WAITERS_PER_INDEX}
 * a write is refused {@code 429} with {@code Retry-After}, through the one
 * emitter.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ExplicitPartitionWaitCapTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final String BODY = "{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n";
    /** Virtual threads, never the ForkJoin common pool a busy suite can starve (M11.25). */
    private static final java.util.concurrent.Executor VIRTUAL =
            task -> Thread.ofVirtual().start(task);

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Accepts every append, once its index is registered and the write is placed. */
    private static final class Accepting extends ForwardingIngest {
        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws IOException {
            int[] n = {0};
            records.forEachRecord(record -> n[0]++);
            return new AppendResult(n[0], 0L, n[0] - 1L);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws IOException {
            try {
                return append(principal, index, partition, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, byte lane, RecordSource records, Runnable buffered) {
            throw new UnsupportedOperationException("explicit partitions only");
        }

        @Override
        public void close() {
        }
    }

    @Test
    void pastTheCapAWriteWaitingForItsIndexIsRefused429AndTheWaitersComplete() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        RoutedIngest routed = new RoutedIngest(new Accepting(), catalog,
                new PendingPool(Clock.systemUTC(), Duration.ofSeconds(30), 1 << 20),
                Duration.ofSeconds(30), Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(routed, PRINCIPAL)))
                .build().start();
        WebClient client = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build();

        List<CompletableFuture<Integer>> waiting = new ArrayList<>();
        for (int i = 0; i < RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX; i++) {
            waiting.add(post(client));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.waitingForRegistration("logs") < RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(routed.waitingForRegistration("logs")).as("the premise: the cap is waiting")
                .isEqualTo(RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX);

        try (HttpClientResponse refused = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(BODY)) {
            assertThat(refused.status().code()).as("one past the cap, refused at once")
                    .isEqualTo(429);
            assertThat(refused.headers().first(HeaderNames.RETRY_AFTER)).hasValue("1");
        }

        catalog.register(new IndexRegistration(base64Url(UUID.randomUUID()), "logs", List.of(),
                4, 4, 1, 1));
        for (CompletableFuture<Integer> write : waiting) {
            assertThat(write.get(10, TimeUnit.SECONDS)).isEqualTo(202);
        }
        assertThat(routed.waitingForRegistration("logs")).as("every slot given back").isZero();
    }

    /**
     * M13.11 (M12.10 review P1, P2, T4): at the cap the write is refused BEFORE
     * ITS BODY IS OPENED -- a malformed body gets the 429, not the 400 its
     * parse would give -- and without a lane permit, and it is counted as a
     * registration-wait refusal, not as the pod's in-flight budget's.
     */
    @Test
    void atTheCapTheBodyIsNotOpenedAndTheRefusalIsCountedAsItsOwn() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        RoutedIngest routed = new RoutedIngest(new Accepting(), catalog,
                new PendingPool(Clock.systemUTC(), Duration.ofSeconds(30), 1 << 20),
                Duration.ofSeconds(30), Clock.systemUTC());
        List<String> told = new java.util.concurrent.CopyOnWriteArrayList<>();
        LaneAdmission admission = new LaneAdmission(256, LaneSet.of((byte) 0));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(routed, PRINCIPAL,
                        new DrainGate(), admission, new IndexQuotas(IndexQuotas.Config.none(),
                                Clock.systemUTC(), name -> true, name -> List.of()),
                        new BulkService.RefusalListener() {
                            @Override
                            public void admissionRefused() {
                                told.add("admission");
                            }

                            @Override
                            public void quotaRefused(String index) {
                                told.add("quota " + index);
                            }

                            @Override
                            public void registrationWaitRefused() {
                                told.add("registration wait");
                            }
                        })))
                .build().start();
        WebClient client = WebClient.builder().baseUri("http://localhost:" + server.port())
                .build();
        List<CompletableFuture<Integer>> waiting = new ArrayList<>();
        for (int i = 0; i < RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX; i++) {
            waiting.add(post(client));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.waitingForRegistration("logs") < RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        int inFlight = admission.inFlight();

        try (HttpClientResponse refused = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit("this is not a bulk body\n")) {
            assertThat(refused.status().code())
                    .as("⚠️ 429, NOT THE PARSE's 400: the body was never opened").isEqualTo(429);
            assertThat(refused.headers().first(HeaderNames.RETRY_AFTER)).hasValue("1");
        }
        assertThat(admission.inFlight()).as("no lane permit was taken for it")
                .isEqualTo(inFlight);
        assertThat(told).as("⚠️ ITS OWN COUNT (review P1, T4), not the in-flight budget's")
                .containsExactly("registration wait");

        catalog.register(new IndexRegistration(base64Url(UUID.randomUUID()), "logs", List.of(),
                4, 4, 1, 1));
        for (CompletableFuture<Integer> write : waiting) {
            assertThat(write.get(10, TimeUnit.SECONDS)).isEqualTo(202);
        }
    }

    private static CompletableFuture<Integer> post(WebClient client) {
        return CompletableFuture.supplyAsync(() -> {
            try (HttpClientResponse response = client.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(BODY)) {
                return response.status().code();
            }
        }, VIRTUAL);
    }

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }
}
