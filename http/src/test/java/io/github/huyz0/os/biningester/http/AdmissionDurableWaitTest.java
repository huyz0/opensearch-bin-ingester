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
 * No admission permit is held across the durable-ack wait (M11.7, M10 review
 * F2, M11 criterion 10): with a budget of ONE and the flush's data PUT held,
 * a second request is admitted while the first waits for durability, and both
 * are {@code 202} once the flush completes.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AdmissionDurableWaitTest {

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

    /** Counts the appends that reached the buffer, on the ingest's own callback. */
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

    private static CompletableFuture<Integer> post(WebClient client, String id) {
        return CompletableFuture.supplyAsync(() -> {
            try (HttpClientResponse answer = client.post("/logs/_bulk")
                    .queryParam("partition", "0")
                    .submit("{\"index\":{\"_id\":\"" + id + "\"}}\n{\"f\":1}\n")) {
                return answer.status().code();
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private static void await(AtomicInteger count, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (count.get() < n && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(count.get()).isEqualTo(n);
    }

    @Test
    void aSecondRequestIsAdmittedWhileTheFirstWaitsForDurability() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger heldPuts = new AtomicInteger();
        BinStore store = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        heldPuts.incrementAndGet();
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
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            CountingBuffered counting = new CountingBuffered(ingest);
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(counting, PRINCIPAL,
                            new DrainGate(), admission)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            CompletableFuture<Integer> first = post(client, "a");
            await(counting.buffered, 1);
            await(heldPuts, 1);
            assertThat(first).as("the premise: the first waits on the held flush").isNotDone();

            CompletableFuture<Integer> second = post(client, "b");
            await(counting.buffered, 2);

            assertThat(second.isDone() ? second.get() : 0)
                    .as("⚠️ ADMITTED, NOT 429: the first holds no permit while it waits")
                    .isNotEqualTo(429);
            assertThat(admission.inFlight()).as("and neither holds one now").isZero();
            release.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS)).isEqualTo(202);
            assertThat(second.get(30, TimeUnit.SECONDS)).isEqualTo(202);
        }
    }

    @Test
    void aBodyOfManyChunksTakesItsPermitBackForEachChunkAndLeavesNoneHeld() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LaneAdmission admission = new LaneAdmission(1, LaneSet.of((byte) 0));
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1",
                TestSequencers.leased(store, "bins/cluster-a", "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            CountingBuffered counting = new CountingBuffered(ingest);
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(counting, PRINCIPAL,
                            new DrainGate(), admission)))
                    .build().start();
            StringBuilder body = new StringBuilder();
            int records = 2 * BulkService.APPEND_CHUNK_RECORDS + 1;
            for (int i = 0; i < records; i++) {
                body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
            }

            try (HttpClientResponse answer = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build()
                    .post("/logs/_bulk").queryParam("partition", "0").submit(body.toString())) {
                assertThat(answer.status().code()).isEqualTo(202);
            }
            assertThat(counting.buffered).as("three chunks, each buffered").hasValue(3);
            assertThat(admission.inFlight())
                    .as("⚠️ A PERMIT TAKEN BACK FOR A LATER CHUNK IS RELEASED TOO")
                    .isZero();
        }
    }
}
