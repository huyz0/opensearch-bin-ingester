// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The permit a request gives back while it waits is the one it must hold
 * while it buffers (M11.7): a later chunk WAITS for one held elsewhere, and a
 * body refused before any chunk returns the permit it was admitted with.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdmissionPermitReturnTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-0000000000aa");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static final class CountingBuffered implements Ingest {
        private final Ingest delegate;
        final AtomicInteger buffered = new AtomicInteger();

        CountingBuffered(Ingest delegate) {
            this.delegate = delegate;
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws IOException {
            return append(principal, index, partition, (byte) 0, records, () -> { });
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable onBuffered) throws IOException {
            return delegate.append(principal, index, partition, lane, records, () -> {
                buffered.incrementAndGet();
                onBuffered.run();
            });
        }

        @Override
        public void close() {
        }
    }

    private WebClient serve(Ingest ingest, LaneAdmission admission) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL,
                        new DrainGate(), admission)))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    private static void await(AtomicInteger count, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (count.get() < n && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(count.get()).isEqualTo(n);
    }

    @Test
    void aLaterChunkWaitsForAPermitHeldElsewhere() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        CountDownLatch firstPut = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BinStore store = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        firstPut.countDown();
                        release.await();
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(20), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            CountingBuffered counting = new CountingBuffered(ingest);
            WebClient client = serve(counting, admission);
            StringBuilder body = new StringBuilder();
            for (int i = 0; i < BulkService.APPEND_CHUNK_RECORDS + 1; i++) {
                body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
            }

            CompletableFuture<Integer> twoChunks = CompletableFuture.supplyAsync(() -> {
                try (HttpClientResponse answer = client.post("/logs/_bulk")
                        .queryParam("partition", "0").submit(body.toString())) {
                    return answer.status().code();
                }
            }, Executors.newVirtualThreadPerTaskExecutor());
            await(counting.buffered, 1);
            assertThat(firstPut.await(30, TimeUnit.SECONDS)).isTrue();
            // ⚠️ THE ONLY PERMIT, TAKEN WHILE THE FIRST CHUNK WAITS FOR ITS PUT.
            LaneAdmission.Permit elsewhere = admission.tryAcquire((byte) 0).orElseThrow();
            release.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (admission.waiting() < 1 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }

            assertThat(admission.waiting()).as("the second chunk parked for a permit")
                    .isEqualTo(1);
            assertThat(counting.buffered)
                    .as("⚠️ THE SECOND CHUNK WAITS FOR A PERMIT: it buffers nothing without one")
                    .hasValue(1);
            elsewhere.release();
            assertThat(twoChunks.get(30, TimeUnit.SECONDS)).isEqualTo(202);
            assertThat(counting.buffered).hasValue(2);
            assertThat(admission.inFlight()).isZero();
        }
    }

    @Test
    void aBodyRefusedBeforeAnyChunkReturnsItsPermit() throws Exception {
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(20), 8L << 20, "cluster-a"),
                new MemoryBinStore(), "bins/cluster-a", "pod1",
                TestSequencers.leased(new MemoryBinStore(), "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            WebClient client = serve(ingest, admission);

            try (HttpClientResponse empty = client.post("/logs/_bulk")
                    .queryParam("partition", "0").submit("")) {
                assertThat(empty.status().code()).as("the premise: refused").isEqualTo(400);
            }

            assertThat(admission.inFlight())
                    .as("⚠️ NO CHUNK BUFFERED, SO NOTHING GAVE IT BACK BUT THE REQUEST's END")
                    .isZero();
        }
    }
}
