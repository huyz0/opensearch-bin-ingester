// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The term a node's fast route answers under is the one it serves (M13.82).
 */
class FastPeerOwnTermTest {

    @Test
    void aSERVINGTermIsItsEpochAndNoTermIsZero() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig config = new LeaseConfig("bins/c", "pod1", "http://pod1:8080", "uid-pod1",
                Duration.ofSeconds(10), Duration.ofSeconds(3));
        LeaseManager manager = new LeaseManager(store, config, Clock.systemUTC());
        try (LocalSequencer term = LocalSequencer.start(store, "bins/c", manager, 8)
                .orElseThrow()) {
            assertThat(FastPeer.ownTerm(term)).isEqualTo(term.epoch()).isPositive();
            term.close();
            assertThat(FastPeer.ownTerm(term)).as("⚠️ A CLOSED TERM IS NOT SERVED").isZero();
        }
        assertThat(FastPeer.ownTerm(null)).isZero();
    }
}
