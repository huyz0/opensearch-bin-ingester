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
                RetentionConfig.defaults(), Optional.empty(), "", topK);
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
