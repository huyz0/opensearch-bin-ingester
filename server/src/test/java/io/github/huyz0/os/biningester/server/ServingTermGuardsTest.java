// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M12.19c (M11.24b T1): the serving guard {@code ServingTerm} carried out of
 * {@code Assembly}, pinned at the unit tier -- a held term that no longer
 * SERVES gives no retention term and no catch-up.
 *
 * <p>⚠️ NOT PINNED HERE: the catch-up's refusal of an INCOMPLETE chain. A
 * chain is incomplete once it holds more than {@code ChainMemory}'s cap,
 * 100,000 deltas, and {@code LocalSequencer}'s public {@code start} does not
 * take the cap; reaching it from this tier means committing 100,001 deltas.
 */
class ServingTermGuardsTest {

    private static final String PREFIX = "bins/c";
    private static final RunKey STREAM = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ad"), 0);

    private static LeaseConfig config() {
        return new LeaseConfig(PREFIX, "poda", "pod-a:9000", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
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

    private static CommitRequest flush(long flushSeq, String segment) {
        return new CommitRequest("poda", "i1", flushSeq, segment, Map.of(STREAM, 3));
    }

    private static CatchUpRequestFrame catchUp() {
        return new CatchUpRequestFrame(UUID.randomUUID(),
                List.of(new CatchUpRequestFrame.Stream(STREAM, 0)));
    }

    @Test
    void aHeldTermThatNoLongerServesGivesNoRetentionTermAndNoCatchUp() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LeaseManager leases = new LeaseManager(store, config(), Clock.systemUTC());
            FleetSequencer fleet = new FleetSequencer(store, config(), noPeers(),
                    () -> LocalSequencer.start(store, PREFIX, leases, 8));
            try {
                fleet.commitAll(List.of(flush(0, "seg-1")));
                assertThat(ServingTerm.retention(fleet, PREFIX))
                        .as("the premise: a serving term has a retention term").isPresent();
                LocalSequencer local = LocalSequencer.underneath(fleet.heldTerm()).orElseThrow();
                local.close();
                assertThat(local.serving()).as("the premise: held, but closed").isFalse();

                assertThat(ServingTerm.retention(fleet, PREFIX))
                        .as("⚠️ NO RETENTION PASS ON A TERM THAT NO LONGER SERVES").isEmpty();
                assertThatThrownBy(() -> ServingTerm.respondCatchUp(fleet, store,
                        new IndexCostLedger(), catchUp(), frame -> { }))
                        .as("⚠️ AND NO CATCH-UP FROM ITS CHAIN")
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("no serving committed chain");
            } finally {
                fleet.close();
            }
        }
    }
}
