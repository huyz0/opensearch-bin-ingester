// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.binstore.BinStore;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class IngesterMetricExportTest {

    @Test
    void applicationMetricsExposeFailureSourcesAndScrapesDoNotTouchTheStore() throws Exception {
        ServerConfig config = new ServerConfig("poda", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(30),
                Duration.ofSeconds(20), "http://poda:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-poda", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty());
        var store = new MemoryBinStore();
        String prefix = config.prefix();
        Inbox.write(store, prefix, intent(0));
        Inbox.write(store, prefix, intent(1));
        CountDownLatch inboxDrainPaused = new CountDownLatch(1);
        CountDownLatch releaseInboxDrain = new CountDownLatch(1);
        AtomicBoolean failCommit = new AtomicBoolean();
        BinStore faulting = blockTakeoverInboxAndFailCommit(store, prefix,
                inboxDrainPaused, releaseInboxDrain, failCommit);
        try (store; Assembly assembly = Assembly.open(config, faulting, noPeers(), Clock.systemUTC());
                FrontDoor door = FrontDoor.start(assembly, Clock.systemUTC())) {
            IngesterMetrics metrics = assembly.metrics();
            metrics.endpointSliceWatchFailed();
            long endpointFailures = metrics.endpointSliceWatchFailures();
            long intentAttempts = metrics.stuckIntentAttempts();
            metrics.endpointSliceWatchFailed();
            long beforeEmptyBatch = metrics.stuckIntentAttempts();
            assertThatCode(() -> metrics.failedIntentBatch(0))
                    .as("an empty failed batch is a valid no-op")
                    .doesNotThrowAnyException();
            assertThat(metrics.stuckIntentAttempts()).isEqualTo(beforeEmptyBatch);
            assertThat(inboxDrainPaused.await(10, TimeUnit.SECONDS))
                    .as("takeover drain reaches the durable inbox before the injected failure")
                    .isTrue();
            failCommit.set(true);
            releaseInboxDrain.countDown();
            Assertions.assertThatThrownBy(() -> InboxDrain.drain(faulting, prefix,
                    assembly.heldTerm()))
                    .isInstanceOf(IOException.class);
            assertThat(metrics.stuckIntentAttempts())
                    .as("Assembly's takeover drain records its failed per-pod batch")
                    .isEqualTo(intentAttempts + 2);
            var before = assembly.storeCounts();
            HttpClient client = HttpClient.newHttpClient();

            String first = scrape(client, door.port());
            assertThat(assembly.storeCounts()).isEqualTo(before);
            String second = scrape(client, door.port());

            assertThat(assembly.storeCounts()).isEqualTo(before);
            assertThat(first).contains("# TYPE " + IngesterMetrics.ENDPOINTSLICE_WATCH_FAILURES
                    + " counter", "# TYPE " + IngesterMetrics.STUCK_INTENT_ATTEMPTS + " counter");
            assertThat(metricLine(first, IngesterMetrics.ENDPOINTSLICE_WATCH_FAILURES))
                    .startsWith(IngesterMetrics.ENDPOINTSLICE_WATCH_FAILURES
                            + "{scope=\"application\",}");
            assertThat(metricLine(first, IngesterMetrics.STUCK_INTENT_ATTEMPTS))
                    .startsWith(IngesterMetrics.STUCK_INTENT_ATTEMPTS
                            + "{scope=\"application\",}");
            assertThat(sample(first, IngesterMetrics.ENDPOINTSLICE_WATCH_FAILURES))
                    .isEqualTo(endpointFailures + 1);
            assertThat(sample(first, IngesterMetrics.STUCK_INTENT_ATTEMPTS))
                    .isEqualTo(intentAttempts + 2);
            assertThat(sample(second, IngesterMetrics.ENDPOINTSLICE_WATCH_FAILURES))
                    .isEqualTo(endpointFailures + 1);
            assertThat(sample(second, IngesterMetrics.STUCK_INTENT_ATTEMPTS))
                    .isEqualTo(intentAttempts + 2);
        }
    }

    private static CommitRequest intent(long sequence) {
        return new CommitRequest("poda", "incarnation-a", sequence,
                "seg/poda-" + sequence, Map.of(new RunKey(UUID.randomUUID(), 0), 1));
    }

    private static BinStore blockTakeoverInboxAndFailCommit(BinStore delegate, String prefix,
            CountDownLatch paused, CountDownLatch release, AtomicBoolean failCommit) {
        AtomicBoolean firstInboxList = new AtomicBoolean(true);
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("list")
                            && args[0].equals(Inbox.prefixFor(prefix))
                            && firstInboxList.compareAndSet(true, false)) {
                        paused.countDown();
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("test did not release takeover inbox drain");
                        }
                    }
                    if (method.getName().equals("putIfAbsent") && failCommit.get()
                            && args[0].toString().endsWith(".delta")) {
                        throw new IOException("injected takeover commit failure");
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException wrapped) {
                        throw wrapped.getCause();
                    }
                });
    }

    private static String scrape(HttpClient client, int port) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/observe/metrics?scope=application"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    private static long sample(String exposition, String name) {
        String line = metricLine(exposition, name);
        return (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }

    private static String metricLine(String exposition, String name) {
        return exposition.lines()
                .filter(each -> each.startsWith(name + "{") || each.startsWith(name + " "))
                .findFirst().orElseThrow(() -> new AssertionError("missing metric " + name));
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint,
                    CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected");
            }

            @Override
            public void close() {
            }
        };
    }
}
