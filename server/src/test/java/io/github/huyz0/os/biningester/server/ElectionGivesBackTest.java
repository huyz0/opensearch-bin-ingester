// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A term whose start fails after the lease is won gives the lease back
 * (M13.76): the election catches only an {@code IOException}, so an unchecked
 * failure in a step after {@code LocalSequencer.start} -- here the dead pods'
 * drain starter -- left the lease renewed by a term nobody held, and no pod
 * could lead until the process ended.
 */
class ElectionGivesBackTest {

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

    private static LeaseManager pod(MemoryBinStore store, String pod, Clock clock) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, pod, "http://" + pod + ":8080",
                "uid-" + pod, Duration.ofSeconds(10), Duration.ofSeconds(3)), clock);
    }

    @Test
    void aTERMWhoseDrainStarterThrowsGivesTheLeaseBackAndRethrows() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Clock clock = Clock.systemUTC();

        assertThatThrownBy(() -> SequencerAssembly.create(config("pod1"), store, noPeers(),
                clock, SequencerAssembly.following(clock), LeaseChallenge.NEVER,
                LeaseManager::new,
                (s, prefix, chain, serving) -> Thread.ofVirtual().start(() -> { }),
                new IngesterMetrics(), (s, prefix, term, failed) -> {
                    throw new IllegalStateException("the drain could not start");
                }))
                .as("the start's failure surfaces, not swallowed")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("the drain could not start");

        assertThat(pod(store, "pod2", clock).tryAcquire())
                .as("the lease given back at once, not held and renewed for ever")
                .isPresent();
    }

    @Test
    void aTAKEOVERWhoseBackfillStarterFailsWithAnErrorGivesTheLeaseBack() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Clock clock = Clock.systemUTC();
        // ⚠️ EPOCH 1 TAKEN AND GIVEN BACK FIRST: only a takeover backfills.
        LeaseManager first = pod(store, "pod0", clock);
        assertThat(first.tryAcquire()).as("the premise: epoch 1 taken").isPresent();
        first.release();

        assertThatThrownBy(() -> SequencerAssembly.create(config("pod1"), store, noPeers(),
                clock, SequencerAssembly.following(clock), LeaseChallenge.NEVER,
                LeaseManager::new, (s, prefix, chain, serving) -> {
                    throw new OutOfMemoryError("unable to create a thread");
                }, new IngesterMetrics(), (s, prefix, term, failed) -> { }))
                .isInstanceOf(OutOfMemoryError.class);

        assertThat(pod(store, "pod2", clock).tryAcquire())
                .as("an Error given back too")
                .isPresent();
    }
}
