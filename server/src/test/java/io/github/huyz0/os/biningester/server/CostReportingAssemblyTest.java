// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An assembled pod logs the top-K cost line at its configured interval, under
 * {@code binstore.cost}, from its own ledger (M11.5, the wiring), and a pod
 * configured with {@code PT0S} logs none.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CostReportingAssemblyTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX = "logs";
    private static final String INDEX_UUID = "AAAAAAAAQACAAAAAAAAAqg";
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of(INDEX));

    private static ServerConfig config(Duration topK) {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of(INDEX),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1", topK, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty());
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    private static List<String> capture(Logger logger) {
        List<String> lines = new CopyOnWriteArrayList<>();
        logger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                lines.add(record.getMessage());
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        });
        logger.setLevel(java.util.logging.Level.ALL);
        return lines;
    }

    @Test
    void anAssembledPodLogsTheLineFromItsOwnLedgerAtItsInterval() throws Exception {
        Logger logger = Logger.getLogger("binstore.cost");
        List<String> lines = capture(logger);
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(Duration.ofMillis(100)), shared,
                        noPeers(), Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                    new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                            "doc".getBytes(StandardCharsets.UTF_8))));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (lines.stream().noneMatch(l -> l.contains(INDEX + " $"))
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(lines)
                    .as("⚠️ THE POD's LEDGER, NAMED FROM ITS CATALOG, ON ITS SCHEDULE")
                    .anyMatch(l -> l.startsWith("cost: top 3") && l.contains(INDEX + " $"));
        }
    }

    /**
     * ⚠️ PRICED AT THE STORE's OWN TABLE (M13.7, M11.5 review T2, M12.18 P1): the
     * memory store is free, so the case above passes with any table handed to
     * the reporter -- {@code CostTable.free()} included, which would print every
     * index at $0 in production. Here the store declares S3 Standard's prices,
     * and the line must price the index above zero.
     */
    @Test
    void theLineIsPricedAtTheStoresOwnCostTable() throws Exception {
        Logger logger = Logger.getLogger("binstore.cost");
        List<String> lines = capture(logger);
        try (BinStore memory = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(Duration.ofMillis(100)),
                        priced(memory), noPeers(), Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                    new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                            "doc".getBytes(StandardCharsets.UTF_8))));

            java.util.regex.Pattern priced =
                    java.util.regex.Pattern.compile(INDEX + " \\$([0-9.]+)");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            double dollars = 0;
            while (dollars == 0 && System.nanoTime() < deadline) {
                for (String line : lines) {
                    java.util.regex.Matcher m = priced.matcher(line);
                    if (m.find()) {
                        dollars = Math.max(dollars, Double.parseDouble(m.group(1)));
                    }
                }
                Thread.onSpinWait();
            }
            assertThat(dollars).as("⚠️ THE INDEX PRICED AT THE STORE's TABLE, not at $0")
                    .isPositive();
        }
    }

    /** {@code store}, declaring S3 Standard's prices as its own. */
    private static BinStore priced(BinStore store) {
        io.github.huyz0.os.biningester.binstore.Capabilities free = store.capabilities();
        io.github.huyz0.os.biningester.binstore.Capabilities s3 =
                new io.github.huyz0.os.biningester.binstore.Capabilities(
                        free.conditionalWrites(), free.batchDelete(), free.presignedUrls(),
                        free.maxKeyBytes(), free.minPartSize(),
                        io.github.huyz0.os.biningester.binstore.CostTable.awsS3Standard());
        return (BinStore) java.lang.reflect.Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("capabilities")) {
                        return s3;
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    void aPodConfiguredOffLogsNoLine() throws Exception {
        Logger logger = Logger.getLogger("binstore.cost");
        List<String> lines = capture(logger);
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(Duration.ZERO), shared, noPeers(),
                        Clock.systemUTC())) {
            long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
            while (System.nanoTime() < until) {
                Thread.onSpinWait();
            }
        }
        assertThat(lines).as("PT0S: no thread, no line").isEmpty();
    }
}
