// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Seal;
import java.io.IOException;

/**
 * The seal a new term puts on the chain it inherits from, and on every burned
 * epoch between (ADR-0029, ADR-0037). Moved out of {@link LocalSequencer#start}
 * unchanged by M13.1d.
 */
final class AncestorSeal {

    /** The inherited chain: its epoch and its sealed end, both 0 at cluster birth. */
    record Inherited(long epoch, long sequence) {
    }

    private AncestorSeal() {
    }

    static Inherited sealFor(BinStore store, String prefix, long epoch, int sealRedriveBudget)
            throws IOException {
        // ⚠️ Epochs advance by exactly one per acquisition, so the
        // predecessor is E-1 — and at E == 1 that is epoch 0, the RESERVED
        // unleased chain, which has no leader to fence and therefore cannot
        // be sealed at all.
        //
        // ⚠️ ADR-0029: SEAL THE ANCESTOR YOU INHERIT FROM, NOT `epoch - 1`
        // BLINDLY. When a run of epochs burned right after the real
        // predecessor — every failed `start` burns one, and a store outage
        // burns many — `epoch - 1` names a chain that was never opened and
        // has nothing to seal, while the genuine ancestor further back
        // stays unsealed and its leader, unaware it has been superseded,
        // keeps committing into offsets this chain is about to reassign.
        // I2. `firstInheritableAncestor` walks back through exactly those
        // burned epochs — the same arithmetic the crossing already does —
        // to the one chain whose offsets actually get inherited, and it is
        // sealed here, BEFORE `open` crosses into it. Order is
        // load-bearing: reading first and sealing second would read a
        // still-growing chain, reopening the defect this closes.
        long prevEpoch = ChainReplay.firstInheritableAncestor(store, prefix, epoch - 1);
        // ⚠️ ADR-0037: SEAL EVERY BURNED EPOCH THE WALK CROSSED, not only
        // the one it lands on. A burned epoch is empty because its leader
        // acquired the lease and has not written YET -- not because it is
        // dead -- so stepping over it without sealing leaves it free to open
        // and write beside this chain, which is the same I2 by another
        // route. MEASURED over 100 ROUGH seeds: the crossed run is mean
        // 1.56 and max 7, two orders of magnitude below the "seal every
        // unsealed ancestor" ADR-0029 rejected on failover latency.
        // ⚠️ THE ONE UNBOUNDED CASE IS CLUSTER BIRTH, when no epoch was ever
        // opened and the walk reaches 0: the run is then every epoch minted
        // so far, which is the count of failed starts before the first
        // success. It is bounded by that and not by history, because once
        // any epoch is opened the walk stops there.
        // ⚠️ THE SEAL'S RETURN VALUE IS THE PROOF, and discarding it was a
        // defect in this loop's first draft that review MEASURED: the probe
        // above and these seals are NOT atomic, and nothing renews the lease
        // during `start`, so a pod stalled past its TTL can open a chain
        // this walk just read as empty. `seal` redrives past whatever
        // landed, so a Seal returned ABOVE slot 0 says exactly one thing --
        // that chain WAS opened after the probe, and it is the real
        // ancestor. Keeping the stale `prevEpoch` there reassigns every
        // offset it committed, which is ADR-0037's own rejected (c)
        // arriving through a race instead of a missing seal.
        long prevSeq = 0;
        for (long crossed = epoch - 1; crossed > Math.max(prevEpoch, 0); crossed--) {
            CommitLog burned = new CommitLog(store, prefix, crossed);
            burned.recoverChainEnd();
            Seal landed = burned.seal(epoch, sealRedriveBudget);
            if (landed.sequence() > 0) {
                // Opened in the window. It is the nearest genuine ancestor,
                // and everything below it is its business rather than ours.
                prevEpoch = crossed;
                prevSeq = landed.sequence();
                break;
            }
        }
        if (prevSeq == 0 && prevEpoch >= 1) {
            CommitLog predecessor = new CommitLog(store, prefix, prevEpoch);
            // ⚠️ THE END, not the contents. This instance only ever SEALS
            // the predecessor; the offsets a full `recover()` would build
            // are dropped with the local, and paying one GET per delta of
            // the previous term makes failover cost grow without bound in
            // that term's length. Measured on a 51-entry chain: 52 GETs,
            // every one discarded.
            predecessor.recoverChainEnd();
            prevSeq = predecessor.seal(epoch, sealRedriveBudget).sequence();
        }
        return new Inherited(prevEpoch, prevSeq);
    }
}
