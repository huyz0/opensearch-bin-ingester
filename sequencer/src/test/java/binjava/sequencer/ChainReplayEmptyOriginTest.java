// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.InvariantFixtures.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import org.junit.jupiter.api.Test;

/**
 * ADR-0037's production change, pinned at the contract rather than through a
 * takeover: {@code inherited} crosses an EMPTY origin, {@code replay} does not.
 *
 * <p>⚠️ THIS EXISTS BECAUSE THE CHANGE WAS UNPINNED. Review measured
 * {@code atOrigin = originIsOwnChain} forced back to a constant {@code true}
 * surviving the WHOLE suite -- the core of the decision, and nothing caught it.
 * It is reachable only through a takeover today, which is why every existing
 * test reached it that way and none of them isolated the semantics.
 *
 * <p>⚠️ THE TWO CALLERS MEAN OPPOSITE THINGS BY AN EMPTY ORIGIN, which is the
 * whole reason the flag survives at all rather than being deleted. For
 * {@code replay} the origin is the node's OWN chain, recovered BEFORE
 * {@code open}, so empty is normal and crossing would read the predecessor
 * twice. For {@code inherited} the origin is the PREDECESSOR, where empty means
 * the walk has not reached the offsets yet.
 */
class ChainReplayEmptyOriginTest {

    private static final RunKey RA = new RunKey(InvariantFixtures.A, 0);

    /** Epoch 1 opened and committing {@code n}, sealed for 2; epoch 2 empty. */
    private static MemoryBinStore emptyEpochAboveACommittingOne(int n) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        first.open(0, 0);
        first.commit("seg/a", InvariantFixtures.counts(n));
        first.seal(2, 8);
        return store;
    }

    @Test
    void inheritedCROSSESAnEmptyOriginToReachTheOffsets() throws Exception {
        MemoryBinStore store = emptyEpochAboveACommittingOne(3);

        assertThat(ChainReplay.inherited(store, PREFIX, 2, Long.MAX_VALUE))
                .as("epoch 2 is empty, so the offsets are epoch 1's -- a walk that stopped "
                        + "at the empty origin would answer {} and the successor would "
                        + "reassign every one of them")
                .containsEntry(RA, 3L);
    }

    @Test
    void replayDoesNOTCrossItsOWNEmptyChain() throws Exception {
        MemoryBinStore store = emptyEpochAboveACommittingOne(3);

        CommitLog own = new CommitLog(store, PREFIX, 2);
        own.recover();

        assertThat(own.offsets())
                .as("a new leader recovers its own chain BEFORE opening it, so empty is "
                        + "normal there and `open` does the inheriting explicitly -- "
                        + "crossing here would read the predecessor twice per takeover")
                .isEmpty();
    }
}
