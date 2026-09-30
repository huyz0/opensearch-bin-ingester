// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The composition root wires chain GC to the term it holds, over the chain's
 * own prefix (M8.39).
 *
 * <p>⚠️ **THE PREFIX IS THE WIRING's WHOLE RISK**: chain GC over any other
 * prefix builds keys that do not exist, deletes nothing, and reports nothing
 * wrong. So the case asserts the delta OBJECT is gone.
 */
class AssemblyChainGcTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
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

    private static String deltaKey(long epoch, long sequence) {
        return String.format(Locale.ROOT, "%s/ctl/log/0/%016x/%016x.delta", PREFIX, epoch,
                sequence);
    }

    @Test
    void theTERMsChainGcCOLLECTSUnderTheCHAINsPrefix() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(), shared, noPeers(),
                        Clock.systemUTC())) {
            LocalSequencer local = LocalSequencer.underneath(assembly.heldTerm()).orElseThrow();
            RunKey stream = new RunKey(UUID.randomUUID(), 0);
            // ⚠️ K DELTAS, straight on the term, so the shipped policy writes a
            // checkpoint: chain GC collects nothing a checkpoint does not cover.
            Set<String> segments = new HashSet<>();
            CommitDelta first = null;
            for (long i = 0; i < LocalSequencer.CHECKPOINT_EVERY_DELTAS; i++) {
                String segment = "seg/" + i;
                segments.add(segment);
                CommitDelta delta = local.commit(
                        new CommitRequest("p", "i", i, segment, Map.of(stream, 1)));
                first = first == null ? delta : first;
            }
            assertThat(shared.stat(deltaKey(local.epoch(), first.sequence())))
                    .as("the premise: the key this case builds is the delta's").isPresent();
            local.chain().forgetCollected(segments);

            assembly.retentionTerm().orElseThrow().chainGc().accept(shared);

            assertThat(shared.stat(deltaKey(local.epoch(), first.sequence())))
                    .as("⚠️ THE OBJECT IS GONE: chain GC ran, over this chain's prefix")
                    .isEmpty();
        }
    }
}
