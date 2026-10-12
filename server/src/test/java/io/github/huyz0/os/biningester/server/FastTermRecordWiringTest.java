// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * The term record fed from this pod's catalog (M13.27l; ADR-0081 §4): its
 * first element at the term start, and every change after it through the
 * node's tick -- each a value recorded before any batch is assigned under it.
 */
class FastTermRecordWiringTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final UUID INDEX = new UUID(4, 4);

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty(), PeerConfig.off(0));
    }

    private static final SequencerTransport NO_PEERS = new SequencerTransport() {
        @Override
        public CommitDelta send(String endpoint, CommitRequest request) {
            throw new UnsupportedOperationException("no peer expected: " + endpoint);
        }

        @Override
        public void close() {
        }
    };

    private static FleetSequencer fleet(MemoryBinStore store,
            Supplier<Map<UUID, Integer>> walQuorums) throws Exception {
        Clock clock = Clock.systemUTC();
        return SequencerAssembly.create(config(), store, NO_PEERS, clock,
                SequencerAssembly.following(clock), LeaseChallenge.NEVER, LeaseManager::new,
                (s, prefix, chain, serving) -> Thread.ofVirtual().start(() -> { }),
                new IngesterMetrics(), (s, prefix, term, failed) -> { }, walQuorums);
    }

    private static List<Roster.TermRecord> termRecord(MemoryBinStore store) throws Exception {
        try (InputStream in = store.get(Roster.key(PREFIX, 1))) {
            return Roster.decode(in.readAllBytes()).termRecord();
        }
    }

    @Test
    void theTERMStartRecordsTheCatalogsFastIndices() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (FleetSequencer fleet = fleet(store, () -> Map.of(INDEX, 2))) {
            assertThat(termRecord(store)).first()
                    .satisfies(t -> assertThat(t.walQuorum()).containsEntry(INDEX, 2));
        }
    }

    @Test
    void aCATALOGChangeAfterTheStartIsRecordedByTheTick() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (FleetSequencer fleet = fleet(store, Map::of)) {
            FastLeaderFrames.recordCatalog(fleet.heldTerm(), Map.of(INDEX, 3));

            assertThat(termRecord(store)).last()
                    .satisfies(t -> assertThat(t.walQuorum()).containsEntry(INDEX, 3));
        }
    }
}
