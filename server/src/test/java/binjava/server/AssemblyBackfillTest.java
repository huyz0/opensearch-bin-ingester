// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import binjava.ingest.IngestConfig;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.LocalSequencer;
import binjava.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover through the composition root backfills the chain below its
 * replay (M8.42).
 *
 * <p>⚠️ **THE TRIGGER IS THE ROOT's, NOT {@code LocalSequencer.start}'s**: M4.9
 * bounds recovery to a small constant and its tests pin it to the request, so
 * the backfill runs where the root publishes a term. This is the case that
 * reds if the root stops doing it.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AssemblyBackfillTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config(String pod) {
        return new ServerConfig(pod, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + pod + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"));
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

    @Test
    void aSUCCESSORsChainREACHESTheFloorAndHoldsThePREDECESSORsPreCheckpointDeltas()
            throws Exception {
        RunKey stream = new RunKey(UUID.randomUUID(), 0);
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            try (Assembly first = Assembly.open(config("pod1"), shared, noPeers(),
                    Clock.systemUTC())) {
                LocalSequencer term = LocalSequencer.underneath(first.heldTerm()).orElseThrow();
                // ⚠️ K COMMITS, so the shipped policy checkpoints and a successor's
                // replay starts ABOVE the first of them.
                for (long i = 0; i < LocalSequencer.CHECKPOINT_EVERY_DELTAS + 5; i++) {
                    term.commit(new CommitRequest("p", "i", i, "seg/" + i, Map.of(stream, 1)));
                }
            }
            try (Assembly second = Assembly.open(config("pod2"), shared, noPeers(),
                    Clock.systemUTC())) {
                LocalSequencer term = LocalSequencer.underneath(second.heldTerm()).orElseThrow();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!term.chain().snapshot().fromFloor()) {
                    assertThat(System.nanoTime()).as("the backfill never landed")
                            .isLessThan(deadline);
                    Thread.sleep(20);
                }

                assertThat(term.chain().snapshot().deltas())
                        .as("⚠️ THE PREDECESSOR's FIRST COMMIT, below the checkpoint the "
                                + "replay started at, is in the keep list")
                        .anyMatch(d -> d.segments().stream()
                                .anyMatch(s -> s.segmentKey().equals("seg/0")));
            }
        }
    }
}
