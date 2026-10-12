// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A replay keeps EVERY void of a recovery entry, not its first (M13.25k;
 * M13.25j's review, P1).
 *
 * <p>⚠️ **THE SECOND VOID WAS DROPPED SILENTLY**: the replay recorded a
 * recovery's voids one call per void, the same-slot rule read the second call
 * as a repeat, and the chain kept one void of two while saying its voids were
 * complete -- a catch-up would have met the other as an unexplained jump.
 */
class ReplayKeepsEveryVoidTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);
    private static final Recovery.VoidRange VA = new Recovery.VoidRange(A, 5, 9);
    private static final Recovery.VoidRange VB = new Recovery.VoidRange(B, 0, 4);

    private static void put(MemoryBinStore store, long epoch, long sequence, byte[] bytes)
            throws Exception {
        store.putIfAbsent(new LogKeys("p", epoch).keyFor(sequence), Body.ofBytes(bytes));
    }

    private static MemoryBinStore predecessorWithTwoVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 1, 0, new Continue(0, 0, 0).encode());
        put(store, 1, 1, new Recovery(1, List.of(), List.of(VA, VB)).encode());
        put(store, 1, 2, new Seal(2, 2).encode());
        return store;
    }

    @Test
    void aRECOVERKeepsBothVoidsOfOneRecovery() throws Exception {
        MemoryBinStore store = predecessorWithTwoVoids();

        CommitLog log = new CommitLog(store, "p", 1);
        log.recover();

        assertThat(log.chain().snapshot().voids()).containsExactly(
                new ChainMemory.SequencedVoid(1, 1, VA), new ChainMemory.SequencedVoid(1, 1, VB));
    }

    @Test
    void TWORecoveriesKeepTheirOwnSlots() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 1, 0, new Continue(0, 0, 0).encode());
        put(store, 1, 1, new Recovery(1, List.of(), List.of(VA)).encode());
        put(store, 1, 2, new Recovery(2, List.of(), List.of(VB)).encode());

        CommitLog log = new CommitLog(store, "p", 1);
        log.recover();

        assertThat(log.chain().snapshot().voids()).containsExactly(
                new ChainMemory.SequencedVoid(1, 1, VA), new ChainMemory.SequencedVoid(1, 2, VB));
    }

    @Test
    void TWOTermsVoidsAtTheSameSlotKeepTheirOwnEpochs() throws Exception {
        // ⚠️ M13.25k review T1: every takeover writes its recovery at slot 1,
        // so a slot alone is no identity across a crossing.
        MemoryBinStore store = new MemoryBinStore();
        put(store, 1, 0, new Continue(0, 0, 0).encode());
        put(store, 1, 1, new Recovery(1, List.of(), List.of(VA)).encode());
        put(store, 1, 2, new Seal(2, 2).encode());
        put(store, 2, 0, new Continue(0, 1, 2).encode());
        put(store, 2, 1, new Recovery(1, List.of(), List.of(VB)).encode());

        CommitLog log = new CommitLog(store, "p", 2);
        log.recover();

        assertThat(log.chain().snapshot().voids()).containsExactly(
                new ChainMemory.SequencedVoid(1, 1, VA), new ChainMemory.SequencedVoid(2, 1, VB));
    }

    @Test
    void aNEWLeadersInheritedWalkKeepsBothVoids() throws Exception {
        MemoryBinStore store = predecessorWithTwoVoids();

        CommitLog leader = new CommitLog(store, "p", 2);
        leader.recover();
        leader.open(1, 2);

        assertThat(leader.chain().snapshot().voids()).containsExactly(
                new ChainMemory.SequencedVoid(1, 1, VA), new ChainMemory.SequencedVoid(1, 1, VB));
    }
}
