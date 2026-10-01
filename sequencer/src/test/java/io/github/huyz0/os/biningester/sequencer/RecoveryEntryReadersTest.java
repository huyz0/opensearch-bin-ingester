// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Every sequencer-side chain reader takes a recovery entry (M13.25,
 * non-negotiable 8; ADR-0082 §5).
 *
 * <p>⚠️ BEFORE THIS TASK THE READERS DISPATCHED BY {@code instanceof
 * CommitDelta}, so a new kind compiled everywhere and was skipped everywhere:
 * the fold left the next offset below a void, and the next commit would have
 * assigned offsets inside it -- a committed hole given content.
 */
class RecoveryEntryReadersTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);

    private static Recovery recovery(long sequence) {
        return new Recovery(sequence,
                List.of(new SegmentCommit("seg/recovered", List.of(new RunCommit(A, 2, 3),
                        new RunCommit(B, 4, 0)))),
                List.of(new Recovery.VoidRange(A, 5, 100)));
    }

    private static void put(MemoryBinStore store, long sequence, byte[] bytes) throws Exception {
        store.putIfAbsent(new LogKeys("p", 0).keyFor(sequence), Body.ofBytes(bytes));
    }

    @Test
    void theFOLDAdvancesOverARecoverysRunsAndPastItsVoids() {
        Map<RunKey, Long> offsets = new HashMap<>();

        ChainReplay.fold(recovery(1), offsets);

        assertThat(offsets).containsEntry(A, 100L).containsEntry(B, 4L);
    }

    @Test
    void theFOLDNeverRewindsAStreamAlreadyPastAVoid() {
        Map<RunKey, Long> offsets = new HashMap<>(Map.of(A, 500L));

        ChainReplay.fold(recovery(1), offsets);

        assertThat(offsets).as("the maximum, as for a delta (I2)").containsEntry(A, 500L);
    }

    @Test
    void aRECOVEREDLogAssignsTheNextOffsetPastTheVoid() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new CommitLog(store, "p", 0).commit("seg-0", Map.of(A, 3));
        put(store, 1, recovery(1).encode());

        CommitLog log = new CommitLog(store, "p", 0);
        log.recover();
        CommitDelta next = log.commit("seg-2", Map.of(A, 1));

        assertThat(log.nextSequence()).as("the recovery consumed its slot").isEqualTo(3);
        assertThat(next.runs().get(0).firstOffset())
                .as("the void [5, 100) is committed: the next record of A is at 100, never "
                        + "inside the hole")
                .isEqualTo(100);
    }

    @Test
    void aLIVECommitLogAppliesARecoveryItLosesARaceTo() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 0);
        log.commit("seg-0", Map.of(A, 3));
        put(store, 1, recovery(1).encode());

        CommitDelta next = log.commit("seg-2", Map.of(A, 1));

        assertThat(next.sequence()).isEqualTo(2);
        assertThat(next.runs().get(0).firstOffset())
                .as("the slot it lost held a recovery; its voids move A as a delta's runs would")
                .isEqualTo(100);
    }

    @Test
    void theDELTAReaderReturnsARecoverysCommitsAndNothingForOnlyVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 0, recovery(0).encode());
        put(store, 1, new Recovery(1, List.of(), List.of(new Recovery.VoidRange(B, 4, 9)))
                .encode());

        assertThat(DeltaReader.at(store, "p", 0, 0))
                .hasValueSatisfying(d -> assertThat(d.allRuns()).hasSize(2));
        assertThat(DeltaReader.at(store, "p", 0, 1))
                .as("a recovery that only voids commits no segment to read").isEmpty();
    }
}
