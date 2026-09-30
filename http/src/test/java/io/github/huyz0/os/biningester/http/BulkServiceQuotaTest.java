// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.ForwardingIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.Ingest;
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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Per-index quotas at the front door (M11.8, ADR-0078, M11 criteria 9 and 11):
 * refused {@code 429} with {@code Retry-After} before the body is read, per
 * index, never cut off, and concurrency bounded by the in-flight cap.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkServiceQuotaTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "audit"));
    private static final Clock FROZEN = Clock.fixed(Instant.parse("2026-09-28T10:00:00Z"),
            ZoneOffset.UTC);

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Counts the records it is handed and says they are buffered BEFORE it
     * waits -- the production order (M11.7), where the lane permit goes back
     * and only the index's slot is still held -- and may then be held on a
     * latch, as a durable wait is.
     */
    private static final class Recording extends ForwardingIngest {
        final AtomicInteger records = new AtomicInteger();
        final AtomicInteger entered = new AtomicInteger();
        volatile CountDownLatch hold = new CountDownLatch(0);

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws java.io.IOException {
            return append(principal, index, partition, (byte) 0, source, () -> { });
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource source, Runnable buffered) throws java.io.IOException {
            entered.incrementAndGet();
            source.forEachRecord(r -> records.incrementAndGet());
            buffered.run();
            try {
                hold.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public void close() {
        }

        /**
         * ⚠️ PRODUCTION's ORDER FOR A ROUTED WRITE TOO (M13.13, M12.2 review T2):
         * as {@code RoutedIngest} over {@code DefaultIngest} places it, counts it,
         * runs {@code buffered} and only then waits -- not, as before, a
         * refusal followed by {@code buffered}.
         */
        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
                byte lane, RecordSource records, Runnable buffered) throws IOException {
            return append(principal, indexOrAlias, 0, lane, records, buffered);
        }
    }

    private WebClient serve(Ingest ingest, IndexQuotas quotas) {
        return serve(ingest, quotas, new LaneAdmission(256, LaneSet.of((byte) 0)));
    }

    private WebClient serve(Ingest ingest, IndexQuotas quotas, LaneAdmission admission) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL,
                        new DrainGate(), admission, quotas)))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    private static String body(int records) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
        }
        return body.toString();
    }

    /**
     * ⚠️ REGISTERED DURING ITS OWN APPEND (M12.4 review round 2): a routed write
     * to an index not yet registered waits for it inside the append (ADR-0015),
     * and the front door charged its only chunk BEFORE that append. Its records
     * are still owed once the index is known, so the next request is refused.
     */
    @Test
    void aOneChunkRequestWhoseIndexRegistersDuringItsAppendIsStillCharged() {
        Set<String> known = java.util.concurrent.ConcurrentHashMap.newKeySet();
        Recording recording = new Recording();
        WebClient client = serve(new RegistersDuringAppend(recording, known), new IndexQuotas(
                new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED,
                        Map.of("logs", new IndexQuotas.Limit(0, 2)), 8),
                FROZEN, known::contains, name -> java.util.List.of()));

        try (HttpClientResponse first = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit(body(5))) {
            assertThat(first.status().code()).isEqualTo(202);
        }
        assertThat(known).as("the premise: unknown at admission, known by the end")
                .contains("logs");

        try (HttpClientResponse refused = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(refused.status().code()).as("its 5 records are owed at 2/s")
                    .isEqualTo(429);
        }
    }

    /** Registers the index inside its append, as a routed write's registration wait does. */
    private static final class RegistersDuringAppend extends ForwardingIngest {
        private final Recording delegate;
        private final Set<String> known;

        RegistersDuringAppend(Recording delegate, Set<String> known) {
            this.delegate = delegate;
            this.known = known;
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource source) throws java.io.IOException {
            known.add(index);
            return delegate.append(principal, index, partition, source);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource source, Runnable buffered) throws java.io.IOException {
            known.add(index);
            return delegate.append(principal, index, partition, lane, source, buffered);
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, byte lane, RecordSource records, Runnable buffered)
                throws java.io.IOException {
            return delegate.appendRouted(principal, indexOrAlias, routing, lane, records,
                    buffered);
        }

        @Override
        public void close() {
        }
    }

    @Test
    void anIndexInDebtIsRefusedBeforeItsBodyIsReadAndAnotherIndexIsAdmitted() {
        Recording ingest = new Recording();
        WebClient client = serve(ingest, new IndexQuotas(new IndexQuotas.Config(
                IndexQuotas.Limit.UNLIMITED, Map.of("logs", new IndexQuotas.Limit(0, 2)), 8),
                FROZEN, name -> true, name -> java.util.List.of()));

        try (HttpClientResponse first = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit(body(5))) {
            assertThat(first.status().code())
                    .as("⚠️ ADMITTED WITH TOKENS, TAKEN INTO DEBT, NEVER CUT OFF").isEqualTo(202);
        }
        assertThat(ingest.records).hasValue(5);

        try (HttpClientResponse refused = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(refused.status().code()).isEqualTo(429);
            assertThat(refused.headers().first(HeaderNames.RETRY_AFTER))
                    .as("3 records of debt at 2/s: 1.5 s, rounded up").hasValue("2");
        }
        assertThat(ingest.records).as("⚠️ REFUSED BEFORE ITS BODY WAS APPENDED").hasValue(5);

        try (HttpClientResponse other = client.post("/audit/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(other.status().code()).as("another index, its own bucket").isEqualTo(202);
        }
    }

    @Test
    void concurrentRequestsToAQuotadIndexAdmitAtMostItsCap() throws Exception {
        Recording ingest = new Recording();
        ingest.hold = new CountDownLatch(1);
        WebClient client = serve(ingest, new IndexQuotas(new IndexQuotas.Config(
                new IndexQuotas.Limit(0, 1_000_000), Map.of(), 2), FROZEN, name -> true, name -> java.util.List.of()));

        List<CompletableFuture<Integer>> answers = new ArrayList<>();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < 5; i++) {
            answers.add(CompletableFuture.supplyAsync(() -> {
                try (HttpClientResponse answer = client.post("/logs/_bulk")
                        .queryParam("partition", "0").submit(body(1))) {
                    return answer.status().code();
                }
            }, executor));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (answers.stream().filter(CompletableFuture::isDone).count() < 3
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(answers.stream().filter(CompletableFuture::isDone)
                .map(CompletableFuture::join).toList())
                .as("⚠️ FIVE CONCURRENT WITH THE BUCKET FULL: three refused by the cap of two, "
                        + "though both admitted ones have buffered and returned their lane "
                        + "permits -- the index's slot is held across the durable wait")
                .containsOnly(429).hasSize(3);
        assertThat(ingest.entered).as("and only two reached the ingest").hasValue(2);
        ingest.hold.countDown();
        assertThat(answers.stream().map(CompletableFuture::join).filter(c -> c == 202))
                .hasSize(2);
    }

    @Test
    void aFinishedRequestGivesItsIndexSlotBackAndARefusedOneItsLanePermit() {
        Recording ingest = new Recording();
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        WebClient client = serve(ingest, new IndexQuotas(new IndexQuotas.Config(
                IndexQuotas.Limit.UNLIMITED, Map.of("logs", new IndexQuotas.Limit(0, 3)), 1),
                FROZEN, name -> true, name -> java.util.List.of()), admission);

        for (int i = 0; i < 2; i++) {
            try (HttpClientResponse answer = client.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(body(1))) {
                assertThat(answer.status().code())
                        .as("⚠️ A CAP OF ONE, AND REQUEST %d: the first gave its slot back", i)
                        .isEqualTo(202);
            }
        }
        try (HttpClientResponse into = client.post("/logs/_bulk").queryParam("partition", "0")
                .submit(body(5))) {
            assertThat(into.status().code()).as("the premise: into debt").isEqualTo(202);
        }
        try (HttpClientResponse refused = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(refused.status().code()).as("the premise: refused by quota")
                    .isEqualTo(429);
        }
        assertThat(admission.inFlight())
                .as("⚠️ THE QUOTA REFUSAL GAVE BACK THE POD's ONLY LANE PERMIT").isZero();
        try (HttpClientResponse other = client.post("/audit/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(other.status().code()).isEqualTo(202);
        }
    }

    /**
     * M13.13 (M12.2 review T2): a ROUTED write parked in its durable wait
     * holds no lane permit either -- the front door hands {@code buffered} to
     * the routed form too, and this double now runs it in production's order.
     */
    @Test
    void aRoutedWriteHeldInItsDurableWaitHoldsNoLanePermit() throws Exception {
        Recording ingest = new Recording();
        ingest.hold = new CountDownLatch(1);
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        WebClient client = serve(ingest, new IndexQuotas(IndexQuotas.Config.none(), FROZEN,
                name -> true, name -> java.util.List.of()), admission);

        CompletableFuture<Integer> routed = CompletableFuture.supplyAsync(() -> {
            try (HttpClientResponse answer = client.post("/logs/_bulk")
                    .queryParam("routing", "user-7").submit(body(1))) {
                return answer.status().code();
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (ingest.entered.get() == 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(ingest.entered).as("the premise: the routed write is parked").hasValue(1);
        // ⚠️ POLLED, NOT READ ONCE (M13.13 review T1): `entered` counts before
        // the records are read and `buffered` runs, so a read straight after
        // could catch the permit on its way back. The write cannot complete
        // while the hold is up, so a permit kept across the wait still fails.
        while (admission.inFlight() != 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }

        assertThat(routed).as("the premise: still parked in its durable wait").isNotDone();
        assertThat(admission.inFlight())
                .as("⚠️ THE POD's ONLY LANE PERMIT WAS GIVEN BACK AT BUFFERING").isZero();
        ingest.hold.countDown();
        assertThat(routed.get(30, TimeUnit.SECONDS)).isEqualTo(202);
    }

    @Test
    void aWriteThroughAnAliasSpendsItsIndexsQuota() throws Exception {
        Recording delegate = new Recording();
        IndexCatalog catalog = new IndexCatalog();
        Duration wait = Duration.ofSeconds(5);
        RoutedIngest routed = new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), wait, 1 << 20), wait, Clock.systemUTC());
        catalog.register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "audit",
                List.of("logs"), 4, 4, 1, 1));
        WebClient client = serve(routed, new IndexQuotas(new IndexQuotas.Config(
                IndexQuotas.Limit.UNLIMITED, Map.of("audit", new IndexQuotas.Limit(0, 2)), 8),
                FROZEN, name -> true, name -> java.util.List.of()));

        try (HttpClientResponse viaAlias = client.post("/logs/_bulk")
                .queryParam("partition", "0").submit(body(5))) {
            assertThat(viaAlias.status().code()).isEqualTo(202);
        }
        try (HttpClientResponse direct = client.post("/audit/_bulk")
                .queryParam("partition", "0").submit(body(1))) {
            assertThat(direct.status().code())
                    .as("⚠️ THE ALIAS's WRITE SPENT audit's BUCKET: one index, one quota")
                    .isEqualTo(429);
        }
    }
}
