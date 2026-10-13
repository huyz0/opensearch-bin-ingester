// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * Each term's inbox drains write an intent's runs of a stream it holds fast
 * through the node's redirect (M13.34m) -- told BEFORE the takeover's own
 * drain, the only one a pod that died deferring gets.
 */
class AssemblyDrainRedirectTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final UUID FAST = new UUID(5, 5);

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

    @Test
    void theTAKEOVERsDrainWritesADeadPodsFastRunThroughTheRedirect() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        RunKey fast = new RunKey(FAST, 0);
        Inbox.write(store, PREFIX, new CommitRequest("podb", "i1", 0, "seg/0", Map.of(fast, 3)));
        List<Set<RunKey>> asked = new CopyOnWriteArrayList<>();
        Clock clock = Clock.systemUTC();

        FleetSequencer fleet = SequencerAssembly.create(config("pod1"), store, noPeers(), clock,
                SequencerAssembly.following(clock), LeaseChallenge.NEVER, LeaseManager::new,
                (s, prefix, chain, serving) -> Thread.ofVirtual().start(() -> { }),
                new IngesterMetrics(), (s, prefix, term, failed) -> {
                    try {
                        InboxDrain.drain(s, prefix, term);
                    } catch (IOException refused) {
                        // the intent stays: asked stays empty
                    }
                }, () -> Map.of(FAST, 1), (intent, runs) -> asked.add(Set.copyOf(runs)));
        try {
            assertThat(asked).containsExactly(Set.of(fast));
            assertThat(Inbox.pending(store, PREFIX)).isEmpty();
        } finally {
            fleet.close();
        }
    }
}
