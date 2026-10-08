// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The chain memory keeps a recovery entry's voids beside its deltas (M13.25h,
 * ADR-0082 §5), so catch-up can deliver them.
 *
 * <p>⚠️ **EVERY RECORDING PATH DROPPED THEM**: a live apply recorded only a
 * recovery's delta, and a replay -- at startup and across a takeover --
 * returned only deltas. A void then reached no catch-up, and a consumer
 * behind it met a jump nothing could explain.
 */
class ChainMemoryVoidsTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final Recovery.VoidRange V1 = new Recovery.VoidRange(A, 5, 9);
    private static final Recovery.VoidRange V2 = new Recovery.VoidRange(A, 12, 20);

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "seg/" + sequence, List.of(new RunCommit(A, 1, 0)));
    }

    @Test
    void recordedVOIDSAreSnapshottedInOrderBesideTheDeltas() {
        ChainMemory chain = new ChainMemory(10);
        chain.record(1, delta(0));
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 2, List.of(V2));

        ChainMemory.Snapshot snapshot = chain.snapshot();

        assertThat(snapshot.voids()).containsExactly(new ChainMemory.SequencedVoid(1, 1, V1),
                new ChainMemory.SequencedVoid(1, 2, V2));
        assertThat(snapshot.deltas()).as("⚠️ RETENTION READS ONLY DELTAS, UNCHANGED").hasSize(1);
        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.voidsComplete()).isTrue();
    }

    @Test
    void anEARLIEREpochsLaterSlotIsNoRepeatOfThisEpochsVoid() {
        // ⚠️ M13.25h review T1: a crossing reads the ancestor first, and a
        // sequence alone is no identity.
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 5, List.of(V1));
        chain.recordVoids(2, 3, List.of(V2));

        assertThat(chain.snapshot().voids()).hasSize(2);
    }

    @Test
    void anEVICTEDVoidClearsOnlyTheVoidsFlag() {
        // ⚠️ M13.25h review P1, T2: the deltas' flag stops the orphan sweep
        // and catch-up for the rest of the term.
        ChainMemory chain = new ChainMemory(2);
        chain.record(1, delta(0));
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 2, List.of(V2));
        chain.recordVoids(1, 3, List.of(new Recovery.VoidRange(A, 30, 40)));

        ChainMemory.Snapshot snapshot = chain.snapshot();

        assertThat(snapshot.voids()).as("bounded, the oldest dropped")
                .extracting(ChainMemory.SequencedVoid::sequence).containsExactly(2L, 3L);
        assertThat(snapshot.voidsComplete()).isFalse();
        assertThat(snapshot.complete()).as("⚠️ EVERY DELTA IS STILL HELD").isTrue();
    }

    @Test
    void FORGETTINGIsInclusiveAndCrossesEpochs() {
        // ⚠️ M13.25h review T3.
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 3, List.of(V2));
        chain.forgetThrough(1, 1);
        assertThat(chain.snapshot().voids()).as("the boundary slot is forgotten too")
                .extracting(ChainMemory.SequencedVoid::sequence).containsExactly(3L);

        chain.recordVoids(2, 1, List.of(V1));
        chain.forgetThrough(2, 0);
        assertThat(chain.snapshot().voids()).as("an earlier epoch's void is forgotten")
                .extracting(ChainMemory.SequencedVoid::epoch).containsExactly(2L);
    }

    @Test
    void aRESETLetsTheSameSlotsBeRecordedAgain() {
        // ⚠️ M13.25h review T3: `recover` resets, then replays the same epoch.
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 1, List.of(V1));
        chain.reset();
        chain.recordVoids(1, 1, List.of(V1));

        assertThat(chain.snapshot().voids()).hasSize(1);
    }

    @Test
    void aREPLAYOfVoidsAlreadyHeldAddsNothing() {
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 1, List.of(V1));

        assertThat(chain.snapshot().voids()).hasSize(1);
    }

    @Test
    void FORGETTINGAndResettingDropTheVoidsWithTheDeltas() {
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 1, List.of(V1));
        chain.record(1, delta(2));
        chain.recordVoids(1, 3, List.of(V2));

        chain.forgetThrough(1, 2);
        assertThat(chain.snapshot().voids()).extracting(ChainMemory.SequencedVoid::sequence)
                .containsExactly(3L);

        chain.reset();
        assertThat(chain.snapshot().voids()).isEmpty();
    }

    @Test
    void COLLECTINGADeltaDropsTheVoidsBeforeIt() {
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(1, 1, List.of(V1));
        chain.record(1, delta(2));
        chain.recordVoids(1, 3, List.of(V2));
        chain.record(1, delta(4));

        chain.forgetCollected(Set.of("seg/2"));

        assertThat(chain.snapshot().voids()).extracting(ChainMemory.SequencedVoid::sequence)
                .containsExactly(3L);
    }

    private static void put(MemoryBinStore store, String prefix, long epoch, long sequence,
            byte[] bytes) throws Exception {
        store.putIfAbsent(new LogKeys(prefix, epoch).keyFor(sequence), Body.ofBytes(bytes));
    }

    @Test
    void aLIVEApplyOfARecoveryRecordsItsVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 0);
        log.commit("seg-0", Map.of(A, 3));
        put(store, "p", 0, 1, new Recovery(1, List.of(), List.of(V1)).encode());

        log.commit("seg-2", Map.of(A, 1));

        assertThat(log.chain().snapshot().voids())
                .containsExactly(new ChainMemory.SequencedVoid(0, 1, V1));
    }

    @Test
    void aRECOVERYReplayedAtStartupRecordsItsVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new CommitLog(store, "p", 0).commit("seg-0", Map.of(A, 3));
        put(store, "p", 0, 1, new Recovery(1, List.of(), List.of(V1)).encode());

        CommitLog log = new CommitLog(store, "p", 0);
        log.recover();

        assertThat(log.chain().snapshot().voids())
                .containsExactly(new ChainMemory.SequencedVoid(0, 1, V1));
    }

    @Test
    void aSUCCESSORInheritsItsPredecessorsVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, "p", 1, 0, new Continue(0, 0, 0).encode());
        put(store, "p", 1, 1, new Recovery(1, List.of(), List.of(V1)).encode());
        put(store, "p", 1, 2, new Seal(2, 2).encode());
        put(store, "p", 2, 0, new Continue(0, 1, 2).encode());

        CommitLog successor = new CommitLog(store, "p", 2);
        successor.recover();

        assertThat(successor.chain().snapshot().voids())
                .as("⚠️ A TAKEOVER's VOIDS CROSS WITH ITS OFFSETS")
                .containsExactly(new ChainMemory.SequencedVoid(1, 1, V1));
    }

    @Test
    void aNEWLEADEROpeningItsChainInheritsThePredecessorsVoids() throws Exception {
        // ⚠️ THE INHERITED WALK (`crossFrom`): a new leader recovers its own,
        // still empty chain, then opens it naming the predecessor's slot.
        MemoryBinStore store = new MemoryBinStore();
        put(store, "p", 1, 0, new Continue(0, 0, 0).encode());
        put(store, "p", 1, 1, new Recovery(1, List.of(), List.of(V1)).encode());
        put(store, "p", 1, 2, new Seal(2, 2).encode());

        CommitLog leader = new CommitLog(store, "p", 2);
        leader.recover();
        leader.open(1, 2);

        assertThat(leader.chain().snapshot().voids())
                .containsExactly(new ChainMemory.SequencedVoid(1, 1, V1));
    }
}
