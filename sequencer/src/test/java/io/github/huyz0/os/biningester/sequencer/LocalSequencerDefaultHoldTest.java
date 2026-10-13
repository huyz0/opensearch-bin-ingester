// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The default path refuses a run its fast term holds for the takeover
 * (ADR-0081 §5 step 3's tail; M13.33n) before anything is written -- its
 * retry is the wait -- and commits any other at once.
 */
class LocalSequencerDefaultHoldTest {

    @Test
    void aHELDStreamsDefaultCommitIsRefusedAndNothingIsCommitted() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"),
                8, BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            MemoryBinStore rosters = new MemoryBinStore();
            sequencer.attach(FastLeaderTermDefaultHoldTest.walking(rosters, rosters, A));

            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(IOException.class).hasMessageContaining("awaits");
            assertThat(sequencer.committedNext(RA)).as("nothing committed").isZero();

            RunKey other = new RunKey(new UUID(7, 7), 0);
            sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 1, "seg/1", Map.of(other, 2))));
            assertThat(sequencer.committedNext(other)).as("an index no walked term recorded")
                    .isEqualTo(2);
        } finally {
            sequencer.close();
        }
    }
}
