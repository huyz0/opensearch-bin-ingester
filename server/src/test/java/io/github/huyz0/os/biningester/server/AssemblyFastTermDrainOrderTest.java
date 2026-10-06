// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The dead pods' inbox drain -- which commits on the term -- starts only after
 * the term start landed (ADR-0081 §5, R3-2; M13.27j review round 1, P1, T1;
 * M13.27o).
 *
 * <p>⚠️ THE DRAIN'S START IS INJECTED, so the test reads {@code LATEST} on the
 * election's own thread at the instant the drain would start -- no thread
 * race and no wait (M13.27j review rounds 2 and 3, T3).
 */
class AssemblyFastTermDrainOrderTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config(String podId) {
        return new ServerConfig(podId, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + podId + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-" + podId,
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty(), PeerConfig.off(0));
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint,
                    CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    private static long latest(BinStore store) throws IOException {
        if (store.stat(Roster.latestKey(PREFIX)).isEmpty()) {
            return -1;
        }
        try (InputStream in = store.get(Roster.latestKey(PREFIX))) {
            return Roster.decodeLatest(in.readAllBytes());
        }
    }

    @Test
    void theINBOXDrainStartsOnlyOnceLatestNamesTheTerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        AtomicLong latestAtDrain = new AtomicLong(-2);
        Clock clock = Clock.systemUTC();

        FleetSequencer fleet = SequencerAssembly.create(config("pod1"), store, noPeers(), clock,
                SequencerAssembly.following(clock), LeaseChallenge.NEVER, LeaseManager::new,
                (s, prefix, chain, serving) -> Thread.ofVirtual().start(() -> { }),
                new IngesterMetrics(), (s, prefix, term, failed) -> {
                    try {
                        latestAtDrain.set(latest(store));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
        try {
            assertThat(latestAtDrain).as("LATEST already named term 1 when the drain started")
                    .hasValue(1);
        } finally {
            fleet.close();
        }
    }
}
