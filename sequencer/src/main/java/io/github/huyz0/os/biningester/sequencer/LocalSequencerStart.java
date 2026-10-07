// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link LocalSequencer}'s start: the lease taken, the ancestor sealed, the
 * chain recovered and opened, the dedupe window and the checkpoint writer
 * seeded -- and the lease handed back if any of it fails. Split out of
 * {@code LocalSequencer} by M13.73 at that file's size ceiling (M11.24).
 */
final class LocalSequencerStart {

    private LocalSequencerStart() {
    }

    static Optional<LocalSequencer> start(BinStore store, String prefix, LeaseManager leases,
            int sealRedriveBudget, LocalSequencer.RenewTicker ticker, long checkpointEveryDeltas,
            CheckpointWriter.Ticker checkpointTicker) throws IOException {
        Objects.requireNonNull(ticker, "ticker");
        // ⚠️ BEFORE `tryAcquire`, and that ordering is the whole point. The
        // writer is built after the renewer thread is already running, so an
        // IllegalArgumentException from its constructor escapes the
        // `catch (IOException)` below with the lease HELD and RENEWED -- no node
        // can take that term again, for as long as the process lives.
        // ⚠️ THE TICKER IS NULL-CHECKED HERE FOR THE SAME REASON as the policy,
        // and leaving it out was the same defect one argument along: an NPE from
        // the CheckpointWriter constructor escapes the `catch (IOException)`
        // below with the lease HELD AND RENEWED.
        Objects.requireNonNull(checkpointTicker, "checkpointTicker");
        CheckpointWriter.checkPolicy(checkpointEveryDeltas, LocalSequencer.CHECKPOINT_INTERVAL);
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
            // ⚠️ SEALED BEFORE `open` CROSSES INTO IT; see AncestorSeal.
            AncestorSeal.Inherited inherited =
                    AncestorSeal.sealFor(store, prefix, epoch, sealRedriveBudget);
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
            // ⚠️ `open` IS WHAT CROSSES FROM THE PREDECESSOR, so the window can
            // only be seeded after it -- a successor's own chain is empty and
            // `recover` finds nothing to inherit.
            log.open(inherited.epoch(), inherited.sequence());
            LocalSequencer sequencer =
                    new LocalSequencer(leases, log, ticker, store, prefix);
            // ⚠️ SEEDED FROM THE CHAIN, AFTER `open` -- which is what crosses
            // from the predecessor, so it is the only point at which there is
            // anything to seed. The comment below has claimed the window is
            // "inherited,
            // not restarted" since M4.10d while nothing seeded it -- the
            // documented-but-absent shape this codebase keeps producing. It is
            // true as of this line.
            sequencer.window.seed(log.recoveredPods());
            // ⚠️ THE WINDOW IS INHERITED, NOT RESTARTED (M4.10d). Without this
            // a successor knows nothing about what its predecessor applied, so
            // a retry that arrives across a takeover -- exactly when the store
            // was flaky enough to cause one -- commits a second time. The
            // predecessor's newest checkpoint carries the per-pod watermarks
            // and a pointer to the delta that last applied for each.
            sequencer.checkpoints = new CheckpointWriter(store, log, prefix,
                    checkpointEveryDeltas, LocalSequencer.CHECKPOINT_INTERVAL, checkpointTicker);
            // ⚠️ AND THE WRITER INHERITS THE SAME MAP (M5.55). Seeding only the
            // window left this leader's own checkpoint carrying cumulative
            // offsets beside a TRUNCATED pods map, so the next successor's walk
            // stopped at it and a pod that committed earlier lost its
            // protection. The window and the writer must inherit together.
            sequencer.checkpoints.inherit(log.recoveredPods());
            return Optional.of(sequencer);
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
}
