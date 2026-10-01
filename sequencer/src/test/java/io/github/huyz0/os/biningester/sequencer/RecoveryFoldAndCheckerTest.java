// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Every void of a recovery is folded, and the chain checker reads a gap and
 * keeps its high-water mark across a recovery (M13.25a, review round 3 T2, T3).
 */
class RecoveryFoldAndCheckerTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 2), 0);

    @Test
    void everyVOIDOfARecoveryIsFolded() {
        Map<RunKey, Long> offsets = new HashMap<>();

        ChainReplay.fold(new Recovery(3, List.of(), List.of(
                new Recovery.VoidRange(A, 0, 4),
                new Recovery.VoidRange(A, 9, 12),
                new Recovery.VoidRange(B, 0, 7))), offsets);

        assertThat(offsets).as("each stream ends at its last void's end")
                .isEqualTo(Map.of(A, 12L, B, 7L));
    }

    private static List<String> i2AndGaps(MemoryBinStore store) throws Exception {
        return Invariants.checkChain(store, "p", 0).stream()
                .map(Invariants.Violation::invariant)
                .filter(i -> i.equals("I2") || i.equals("gap"))
                .toList();
    }

    @Test
    void aRECOVERYPastTheMarkIsAGap() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LogKeys keys = new LogKeys("p", 0);
        store.putIfAbsent(keys.keyFor(0), Body.ofBytes(
                new CommitDelta(0, "seg/0", List.of(new RunCommit(A, 3, 0))).encode()));
        store.putIfAbsent(keys.keyFor(1), Body.ofBytes(new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(A, 5, 9))).encode()));

        assertThat(i2AndGaps(store)).as("A ended at 3; a void from 5 skips two offsets")
                .containsExactly("gap");
    }

    @Test
    void aVOIDBelowTheMarkDoesNotLowerIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LogKeys keys = new LogKeys("p", 0);
        store.putIfAbsent(keys.keyFor(0), Body.ofBytes(
                new CommitDelta(0, "seg/0", List.of(new RunCommit(A, 10, 0))).encode()));
        store.putIfAbsent(keys.keyFor(1), Body.ofBytes(new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(A, 3, 5))).encode()));
        store.putIfAbsent(keys.keyFor(2), Body.ofBytes(
                new CommitDelta(2, "seg/2", List.of(new RunCommit(A, 1, 10))).encode()));

        assertThat(i2AndGaps(store))
                .as("the void voids committed offsets (I2), and the next delta continues "
                        + "from 10, not from the void's end")
                .containsExactly("I2");
    }
}
