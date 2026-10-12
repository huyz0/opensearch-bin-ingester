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
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The chain read below a takeover's replay carries a recovery entry's voids
 * into the chain memory (M13.25j; M13.25h's review, P2), and the voids flag
 * keeps its edges (M13.25h's review, T4).
 *
 * <p>⚠️ **BACKFILL KEPT A RECOVERY's DELTA AND DROPPED ITS VOIDS**, so a
 * catch-up served from the hours before the term would have met a void as an
 * unexplained jump -- exactly what the chain memory now keeps them for.
 */
class ChainBackfillVoidsTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final Recovery.VoidRange V1 = new Recovery.VoidRange(A, 5, 9);
    private static final Recovery.VoidRange V2 = new Recovery.VoidRange(A, 12, 20);

    private static void put(MemoryBinStore store, long epoch, long sequence, byte[] bytes)
            throws Exception {
        store.putIfAbsent(new LogKeys("p", epoch).keyFor(sequence), Body.ofBytes(bytes));
    }

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "seg/" + sequence, List.of(new RunCommit(A, 1, 0)));
    }

    @Test
    void theBACKFILLWalkReturnsARecoverysVoids() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 1, 0, new Continue(0, 0, 0).encode());
        // ⚠️ M13.25j review T3: a takeover's recovery commits AND voids.
        put(store, 1, 1, new Recovery(1, List.of(new io.github.huyz0.os.biningester.format
                .SegmentCommit("seg/r", List.of(new RunCommit(A, 2, 0)))), List.of(V1)).encode());
        put(store, 1, 2, delta(2).encode());

        ChainBackfill.Below below = ChainBackfill.belowWithVoids(store, "p", 1, 3, () -> true);

        assertThat(below.voids()).containsExactly(new ChainMemory.SequencedVoid(1, 1, V1));
        assertThat(below.deltas()).extracting(ChainGc.DeltaAt::sequence).containsExactly(1L, 2L);
    }

    @Test
    void BACKFILLEDVoidsGoBeforeTheHeldOnesAndNoneTwice() {
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(2, 1, List.of(V2));

        chain.backfill(List.of(), List.of(new ChainMemory.SequencedVoid(1, 1, V1),
                new ChainMemory.SequencedVoid(2, 1, V2)));

        assertThat(chain.snapshot().voids()).containsExactly(
                new ChainMemory.SequencedVoid(1, 1, V1), new ChainMemory.SequencedVoid(2, 1, V2));
    }

    @Test
    void SAMEEpochVoidsAreBackfilledInOrderBelowTheHeldOne() {
        // ⚠️ M13.25j review T1, T2: the ordinary takeover crosses no epoch, and
        // more than one void is backfilled.
        ChainMemory chain = new ChainMemory(10);
        chain.recordVoids(2, 5, List.of(V2));

        chain.backfill(List.of(), List.of(new ChainMemory.SequencedVoid(2, 1, V1),
                new ChainMemory.SequencedVoid(2, 3, V1), new ChainMemory.SequencedVoid(2, 5, V2)));

        assertThat(chain.snapshot().voids()).extracting(ChainMemory.SequencedVoid::sequence)
                .containsExactly(1L, 3L, 5L);
    }

    @Test
    void aBACKFILLPastTheBoundDropsTheOldestAndSaysSo() {
        ChainMemory chain = new ChainMemory(1);
        chain.recordVoids(2, 1, List.of(V2));

        chain.backfill(List.of(), List.of(new ChainMemory.SequencedVoid(1, 1, V1)));

        assertThat(chain.snapshot().voids()).extracting(ChainMemory.SequencedVoid::epoch)
                .containsExactly(2L);
        assertThat(chain.snapshot().voidsComplete()).isFalse();
        assertThat(chain.snapshot().complete()).as("the deltas' flag is untouched").isTrue();
    }

    @Test
    void theBACKGROUNDBackfillCarriesThemIntoTheChain() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 1, 0, new Continue(0, 0, 0).encode());
        put(store, 1, 1, new Recovery(1, List.of(), List.of(V1)).encode());
        ChainMemory chain = new ChainMemory(10);
        chain.record(2, delta(1));

        ChainBackfill.inBackground(store, "p", chain).join();

        assertThat(chain.snapshot().voids())
                .containsExactly(new ChainMemory.SequencedVoid(1, 1, V1));
    }

    @Test
    void aRESETAfterAnEvictionVouchesForTheVoidsAgain() {
        ChainMemory chain = new ChainMemory(1);
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 2, List.of(V2));
        assertThat(chain.snapshot().voidsComplete()).as("the premise").isFalse();

        chain.reset();

        assertThat(chain.snapshot().voidsComplete()).isTrue();
    }

    @Test
    void anEVICTIONStillShowsOnceEveryDeltaIsForgotten() {
        ChainMemory chain = new ChainMemory(1);
        chain.record(1, delta(0));
        chain.recordVoids(1, 1, List.of(V1));
        chain.recordVoids(1, 2, List.of(V2));

        chain.forgetThrough(1, 0);

        assertThat(chain.snapshot().deltas()).as("the premise: the empty branch").isEmpty();
        assertThat(chain.snapshot().voidsComplete()).isFalse();
    }
}
