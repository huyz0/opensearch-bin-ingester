// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.Lease;
import java.io.IOException;
import java.util.Optional;

/**
 * The node holding the lease sequences directly — M4's one implementation.
 *
 * <p>⚠️ BECOMING THE LEADER IS ONE ACT, NOT THREE. Acquiring the lease, SEALING
 * the predecessor's chain and OPENING this one with a CONTINUE happen together
 * or the log has forked: two chains live, both acking, each invisible to the
 * other's readers, and both assigning the same offsets. That is why this is one
 * commit's worth of behaviour and why {@link #start} does all three before it
 * hands back anything a caller can commit through.
 *
 * <p>⚠️ OFFSETS SURVIVE A TAKEOVER, as of M4.6e. A new leader's `recover()`
 * reads only its own epoch's prefix, so until the CONTINUE was followed across
 * the boundary a successor began every stream again at 0 — violating I2,
 * NFR-11 and acceptance criterion 4, the contract {@link Sequencer#commit}
 * states verbatim. It now crosses: measured, leader 1 acknowledges records at
 * 0..99 and leader 2's first commit for the same stream returns 100.
 *
 * <p>⚠️ WHAT THAT COSTS is stated rather than buried: the crossing is
 * TRANSITIVE, so a takeover re-reads every entry of every ancestor chain and
 * per-failover cost grows with the cluster's whole history. M4.8's checkpoints
 * are what make it constant again. `LocalSequencerFailoverTest` pins both
 * slopes so the growth cannot get worse unnoticed.
 *
 * <p>⚠️ COMMIT FORWARDING IS M5's. A node that does not hold the lease gets an
 * empty {@link Optional} here and has no way to reach the node that does, so a
 * multi-node deployment is not correct until M5 — the M4 SPEC states that as a
 * deployment constraint rather than leaving it to be discovered.
 */
public final class LocalSequencer implements Sequencer {

    private final LeaseManager leases;
    private final CommitLog log;
    private boolean closed;

    private LocalSequencer(LeaseManager leases, CommitLog log) {
        this.leases = leases;
        this.log = log;
    }

    /**
     * Acquires the lease, seals the predecessor and opens this term's chain.
     *
     * @param sealRedriveBudget how many times the seal may lose to the OLD
     *     leader's still-in-flight commits before giving up. Sized from the
     *     renew interval, never from in-flight depth — see
     *     {@link CommitLog#seal}.
     * @return empty when another node holds the lease. ⚠️ Empty means "not the
     *     leader", never "failed": nothing has been written and nothing needs
     *     undoing.
     */
    public static Optional<LocalSequencer> start(BinStore store, String prefix,
            LeaseManager leases, int sealRedriveBudget) throws IOException {
        Optional<Lease> won = leases.tryAcquire();
        if (won.isEmpty()) {
            return Optional.empty();
        }
        // ⚠️ NO `held()` FALLBACK HERE, and it was tried. `tryAcquire` also
        // returns empty when THIS INSTANCE already holds the lease, so falling
        // back to `held()` looks like it protects a retry after a failed start.
        // It does not: the catch below RELEASES on failure, so a retry simply
        // re-acquires, and the fallback could then only fire when this instance
        // successfully holds the term — i.e. on a SECOND start of a term already
        // started. Measured: that produced three live sequencers on one epoch,
        // each re-reading the whole chain, with `close()` on any one releasing
        // the shared lease while its siblings kept committing.
        // ⚠️ So `start` is NOT an "am I the leader?" probe. `LeaseManager.held()`
        // is that, and a caller wanting a supervisor tick should use it.
        long epoch = won.get().epoch();
        try {
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
            // sealed here, BEFORE `open` below crosses into it. Order is
            // load-bearing: reading first and sealing second would read a
            // still-growing chain, reopening the defect this closes.
            long prevEpoch = ChainReplay.firstInheritableAncestor(store, prefix, epoch - 1);
            long prevSeq = 0;
            if (prevEpoch >= 1) {
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
            CommitLog log = new CommitLog(store, prefix, epoch);
            // ⚠️ RECOVER BEFORE SEQUENCING, and the note below moved here from
            // DefaultIngest's constructor with the responsibility. `commit`
            // starts at sequence 0 and walks slot by slot on a lost
            // `putIfAbsent`, so against an existing prefix of N entries the
            // first append would issue ~2N requests -- a rate scaling with
            // commit-log HISTORY.
            // ⚠️ `recover()` is NOT "one LIST": it is one LIST per 1000 objects
            // PLUS one GET per entry, and since M4.6e it also crosses into the
            // predecessor to inherit offsets. The request COST is off the hot
            // path and R2 permits it; the STARTUP LATENCY is unbounded in log
            // length, and an operator should not have to learn that from the
            // code. M4.9's bounded recovery is what fixes it.
            log.recover();
            log.open(prevEpoch, prevSeq);
            return Optional.of(new LocalSequencer(leases, log));
        } catch (IOException failed) {
            // ⚠️ WE HOLD THE LEASE AND CANNOT USE IT. Returning empty here would
            // report "not the leader" while holding the term, and the cluster
            // would have no sequencer for a full TTL — the outage the redrive
            // branch exists to prevent, arriving by the other door. Hand the
            // lease back so a successor can take over in milliseconds, and let
            // the failure propagate rather than disguising it as a lost race.
            try {
                leases.release();
            } catch (IOException alsoFailed) {
                failed.addSuppressed(alsoFailed);
            }
            throw failed;
        }
    }

    /** The epoch this instance sequences at. */
    public long epoch() {
        return log.epoch();
    }

    @Override
    public CommitDelta commit(CommitRequest request) throws IOException {
        if (closed) {
            // ⚠️ The Sequencer contract says a commit after close must FAIL. The
            // lease is released, so another node may already have sealed this
            // chain; succeeding here would be writing past a barrier that exists.
            throw new IOException("this sequencer released its lease at epoch "
                    + log.epoch() + " and must not commit again");
        }
        return log.commit(request.segmentKey(), request.recordCounts());
    }

    @Override
    public void close() throws IOException {
        closed = true;
        leases.release();
    }
}
