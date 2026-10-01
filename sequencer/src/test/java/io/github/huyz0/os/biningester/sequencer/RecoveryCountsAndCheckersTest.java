// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A recovery's runs in the compaction counts on a lost race, and in the test
 * checkers' expected offsets (M13.25 review round 2, T1 and T3).
 */
class RecoveryCountsAndCheckersTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static Recovery recovery(long sequence, long first) {
        return new Recovery(sequence,
                List.of(new SegmentCommit("seg/recovered-" + sequence,
                        List.of(new RunCommit(A, 2, first)))),
                List.of(new Recovery.VoidRange(A, first + 2, first + 50)));
    }

    @Test
    void aLIVELogCountsARecoverysRunsForCompaction() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 0);
        log.commit("seg-0", Map.of(A, 3));
        store.putIfAbsent(new LogKeys("p", 0).keyFor(1), Body.ofBytes(recovery(1, 3).encode()));

        log.commit("seg-2", Map.of(A, 1));

        assertThat(log.compaction().topK())
                .filteredOn(c -> c.stream().equals(A))
                .extracting(CompactionObservable.StreamCount::entries)
                .as("three index entries for A: the two deltas and the recovery it lost to")
                .containsExactly(3L);
    }

    @Test
    void theREADERCheckerExpectsARecoverysRunsAndVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.putIfAbsent(new LogKeys("p", 1).keyFor(0),
                Body.ofBytes(new io.github.huyz0.os.biningester.format.Continue(0, 0, 0).encode()));
        store.putIfAbsent(new LogKeys("p", 1).keyFor(1), Body.ofBytes(recovery(1, 0).encode()));

        assertThat(ReaderInvariants.checkReader(store, "p",
                        new ReaderInvariants.ReaderView(1, Map.of(A, 2L))))
                .extracting(Invariants.Violation::invariant)
                .as("a reader that took the recovery's runs and missed its void stopped "
                        + "short of where the chain leaves A")
                .contains("I4");
        assertThat(ReaderInvariants.checkReader(store, "p",
                        new ReaderInvariants.ReaderView(1, Map.of(A, 50L))))
                .as("and one at the void's end is exact").isEmpty();
    }
}
