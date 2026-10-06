// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A takeover's term start lands before anything else the term starts -- its
 * chain backfill and the dead pods' inbox drain, which commits on the term --
 * so no default-path commit precedes step 3 (ADR-0081 §5; M13.27d review
 * round 2, T5).
 */
class AssemblyFastTermOrderTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config(String podId) {
        return new ServerConfig(podId, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + podId + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-" + podId,
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty());
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
    void theTERMStartLandsBeforeTheTakeoversBackfillStarts() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (Assembly first = Assembly.open(config("pod1"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(first.heldTerm()).isNotNull();
        }
        AtomicLong latestAtBackfill = new AtomicLong(-2);

        try (Assembly second = Assembly.openForTest(config("pod2"), store, noPeers(),
                Clock.systemUTC(), (s, prefix, chain, serving) -> {
                    try {
                        latestAtBackfill.set(latest(store));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    return Thread.ofVirtual().start(() -> { });
                })) {
            assertThat(latestAtBackfill).as("LATEST already names term 2").hasValue(2);
        }
    }
}
