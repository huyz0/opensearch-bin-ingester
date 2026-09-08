// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.Seal;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import binjava.format.SegmentCommit;
import binjava.format.Lease;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
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

    /**
     * How long until the next renew is due.
     *
     * <p>⚠️ A SEAM, for the reason {@code BatchingSequencer.WindowTimer} is one:
     * a test that slept would assert "renewed at roughly the right time", and
     * testing.md forbids the sleep that buys it.
     */
    @FunctionalInterface
    public interface RenewTicker {
        /** Blocks until the holder should renew again. */
        void awaitNextRenew() throws InterruptedException;
    }

    private static final System.Logger LOG =
            System.getLogger(LocalSequencer.class.getName());

    private final LeaseManager leases;
    private final CommitLog log;
    private final RenewTicker ticker;
    private final BinStore store;
    private final String prefix;
    private final Thread renewer;
    private volatile boolean closed;

    /**
     * Set when a renew came back EMPTY, which is how a holder learns it has been
     * fenced — {@link LeaseManager#renew()} says so and says the holder must
     * stop sequencing immediately.
     *
     * <p>⚠️ SEPARATE FROM {@code closed}, because they are different events with
     * different messages: one is this node deciding to stop, the other is this
     * node being told it already has. Conflating them tells an operator a
     * shutdown happened when a takeover did.
     */
    private volatile boolean fenced;
    private volatile CheckpointWriter checkpoints;

    /**
     * What each pod incarnation has already had applied (M4.10d).
     *
     * <p>⚠️ CONFINED TO THE COMMIT PATH. Every read and write happens inside
     * {@link #commitAll}, which the lease makes single-writer per epoch, so a
     * plain map needs no lock -- unlike {@link #checkpoints}, which the renewer
     * thread also touches and which is therefore {@code volatile}.
     */
    private final IdempotencyWindow window;

    /**
     * ⚠️ DEFAULTS, and they are a COST choice rather than a correctness one: one
     * checkpoint per 1000 deltas or per minute, whichever comes first, so a busy
     * cluster pays a fraction of a percent on top of its commit rate and an idle
     * one pays nothing at all (the writer is dirty-flagged). M4.9 measures what
     * they should be; these are what it measures against.
     */
    static final long CHECKPOINT_EVERY_DELTAS = 1000;

    static final java.time.Duration CHECKPOINT_INTERVAL = java.time.Duration.ofSeconds(60);

    private LocalSequencer(LeaseManager leases, CommitLog log, RenewTicker ticker,
            BinStore store, String prefix) {
        this.leases = leases;
        this.log = log;
        this.ticker = ticker;
        this.store = store;
        this.prefix = prefix;
        this.window = new IdempotencyWindow(store, prefix);
        // ⚠️ A VIRTUAL THREAD parked on the tick, like every other blocking
        // worker here.
        this.renewer = Thread.ofVirtual().name("lease-renewer").start(this::renewForever);
    }

    /**
     * Waits for the renew thread to stop.
     *
     * <p>⚠️ A TEST OBSERVATION POINT, package-private and doing nothing for
     * production -- and it is what turns the close/renewer race from an
     * unbounded poll into an exact assertion. {@code fenced} is written BEFORE
     * the thread terminates, so {@code join} is a happens-before edge: once
     * this returns true, whatever the renewer was going to do it has done.
     * Three earlier attempts polled for three seconds instead and could not be
     * made to fail on demand, which is why the test they backed was deleted.
     */
    boolean awaitRenewerStopped(long millis) throws InterruptedException {
        renewer.join(millis);
        return !renewer.isAlive();
    }

    private void renewForever() {
        while (!closed && !fenced) {
            try {
                ticker.awaitNextRenew();
                if (closed) {
                    return;
                }
                if (leases.renew().isEmpty()) {
                    // ⚠️ EMPTY MEANS FENCED, and it is the ONLY thing that does.
                    // Another node holds the term now, and it has sealed this
                    // chain; committing on would reassign offsets a successor
                    // has already issued, which is I2.
                    fenced = true;
                    // ⚠️ THE WRITER STOPS WITH THE LEASE. A fenced node still
                    // holds a live CheckpointWriter, and a checkpoint PUT is a
                    // write into the chain's own prefix -- the thing being
                    // fenced is exactly the right to make it.
                    CheckpointWriter fencedWriter = checkpoints;
                    if (fencedWriter != null) {
                        fencedWriter.close();
                    }
                    // ⚠️ NOT NECESSARILY A TAKEOVER, and an earlier draft of
                    // this line said it was. ADR-0027 accepts a case where the
                    // renew comes back empty because this node's OWN late write
                    // landed between the refresh and the CAS -- nobody took the
                    // term. From in here the two are indistinguishable, which
                    // is why stopping is right and why the message must not
                    // send an operator hunting for a successor that may not
                    // exist.
                    // ⚠️ ERROR, ABOVE the renewer-died case below, because this
                    // one is PERMANENT until restart while a lapsed renewer
                    // merely fails over at the TTL. An earlier draft had the
                    // severities the other way round.
                    LOG.log(System.Logger.Level.ERROR,
                            "the lease renew at epoch " + log.epoch() + " returned empty: either "
                                    + "another node took the term or this one self-fenced after an "
                                    + "ambiguous write (ADR-0027). This node will not sequence "
                                    + "again and must be restarted to rejoin.");
                    return;
                }
            } catch (InterruptedException stopping) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException transientFailure) {
                // ⚠️ NOT FENCED. `renew`'s contract separates an IOException --
                // the store was unreachable, or the lock could not be taken --
                // from an empty result, and only the latter means the term is
                // gone. Standing down here would turn every store hiccup into a
                // cluster-wide failover, which is the outage this task removes
                // rather than one it should add. The next tick retries.
                LOG.log(System.Logger.Level.WARNING,
                        "a lease renew failed; the term is untouched and the next tick retries",
                        transientFailure);
            } catch (Throwable died) {
                // ⚠️ THE RENEWER MUST NOT DIE SILENTLY. If it does, the lease
                // lapses and the cluster fails over at the TTL -- survivable,
                // and exactly the behaviour that existed before this task, but
                // an operator has no other way to learn it happened.
                LOG.log(System.Logger.Level.WARNING,
                        "the lease renewer terminated; this node's term will lapse at its TTL "
                                + "and the cluster will fail over -- survivable, unlike a fence",
                        died);
                return;
            }
        }
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
        return start(store, prefix, leases, sealRedriveBudget,
                sleepFor(leases.renewInterval()));
    }

    /**
     * ⚠️ PACKAGE-PRIVATE FOR ONE TEST, exactly as
     * {@code BatchingSequencer.sleepFor} is and for the same measured reason:
     * the shipping ticker is installed by the 4-arg {@link #start} and exposed
     * nowhere, so every test injected its own and this one ran in NO test.
     * Measured: {@code sleepFor(Duration.ofDays(1))} left the whole build
     * green, which is a leader that renews once a day against a ten-second TTL.
     */
    static RenewTicker sleepFor(java.time.Duration interval) {
        return () -> Thread.sleep(interval);
    }

    /** As {@link #start}, with the renew tick injected. */
    public static Optional<LocalSequencer> start(BinStore store, String prefix,
            LeaseManager leases, int sealRedriveBudget, RenewTicker ticker) throws IOException {
        return start(store, prefix, leases, sealRedriveBudget, ticker,
                CHECKPOINT_EVERY_DELTAS, CheckpointWriter.sleepFor(CHECKPOINT_INTERVAL));
    }

    /**
     * As {@link #start}, with the checkpoint policy injected too.
     *
     * <p>⚠️ PACKAGE-PRIVATE FOR THE SAME MEASURED REASON {@link #sleepFor}
     * records: a seam nothing shipping installs is a seam no test exercises. The
     * K here is crossed by a test in two commits; the shipped default takes a
     * thousand, so without this the WIRING -- that {@code commitAll} calls the
     * writer at all -- would be pinned by nothing.
     */
    static Optional<LocalSequencer> start(BinStore store, String prefix, LeaseManager leases,
            int sealRedriveBudget, RenewTicker ticker, long checkpointEveryDeltas,
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
        CheckpointWriter.checkPolicy(checkpointEveryDeltas, CHECKPOINT_INTERVAL);
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
            LocalSequencer sequencer =
                    new LocalSequencer(leases, log, ticker, store, prefix);
            // ⚠️ THE WINDOW IS INHERITED, NOT RESTARTED (M4.10d). Without this
            // a successor knows nothing about what its predecessor applied, so
            // a retry that arrives across a takeover -- exactly when the store
            // was flaky enough to cause one -- commits a second time. The
            // predecessor's newest checkpoint carries the per-pod watermarks
            // and a pointer to the delta that last applied for each.
            sequencer.checkpoints = new CheckpointWriter(store, log, prefix,
                    checkpointEveryDeltas, CHECKPOINT_INTERVAL, checkpointTicker);
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

    /** The epoch this instance sequences at. */
    public long epoch() {
        return log.epoch();
    }

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        if (fenced) {
            // ⚠️ REFUSED HERE, BEFORE THE STORE. `CommitLog` would also refuse,
            // because a successor sealed this chain -- but only after a round
            // trip, and only once the seal is visible. This node already KNOWS,
            // and spending a request to be told again is both slower and a
            // request that scales with a fenced pod's retry rate.
            throw new IOException("this sequencer lost its lease at epoch " + log.epoch()
                    + " and has been fenced; it must not commit again");
        }
        if (closed) {
            // ⚠️ The Sequencer contract says a commit after close must FAIL. The
            // lease is released, so another node may already have sealed this
            // chain; succeeding here would be writing past a barrier that exists.
            throw new IOException("this sequencer released its lease at epoch "
                    + log.epoch() + " and must not commit again");
        }
        // ⚠️ CLASSIFY FIRST, then commit the fresh submissions BEFORE anything
        // can refuse. `BatchingSequencer` fails a whole window together, so a
        // refusal raised first loses flushes that had nothing wrong with them
        // -- measured at offset 7 instead of 12.
        IdempotencyWindow.Split split = window.split(requests);
        List<CommitRequest> fresh = split.fresh();
        List<CommitRequest> replays = split.replays();
        List<SegmentCommit> answered = new ArrayList<>();
        if (replays.isEmpty()) {
            return applyFresh(requests);
        }
        CommitDelta delta = fresh.isEmpty() ? null : applyFresh(fresh);
        // ⚠️ AFTER the fresh commit, deliberately: this can throw, and by then
        // everything that could be made durable already is.
        answered.addAll(window.answer(replays));
        if (delta == null) {
            // ⚠️ NOTHING WAS WRITTEN: the pure retry. The sequence returned is
            // the one that already holds these records.
            return new CommitDelta(window.sequenceOf(replays), answered);
        }
        List<SegmentCommit> merged = new ArrayList<>(delta.segments());
        merged.addAll(answered);
        // ⚠️ THE RETURNED DELTA IS NOT THE DURABLE ONE when a batch mixes the
        // two. It carries every submitted segment because a caller matches its
        // own flush by segment key; the replayed ones are at their ORIGINAL
        // offsets, under a sequence naming the FRESH write. Never read back.
        return new CommitDelta(delta.sequence(), merged);
    }

    /** Commits {@code requests} for real, and records what that applied. */
    private CommitDelta applyFresh(List<CommitRequest> requests) throws IOException {
        CommitDelta delta = log.commitAll(requests);
        for (CommitRequest request : requests) {
            window.applied(request, log.epoch(), delta.sequence());
        }
        // ⚠️ AFTER the commit, never before: the writer reads `nextSequence` and
        // the offsets, and observing first would checkpoint a chain state that
        // does not exist yet. ⚠️ It cannot throw -- a failed checkpoint must not
        // fail an acknowledged write.
        CheckpointWriter writer = checkpoints;
        if (writer != null) {
            writer.observe(requests, delta.sequence());
        }
        return delta;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        // ⚠️ SIGNALLED, NOT JOINED, and two earlier drafts gave wrong reasons.
        // The first said the renewer holds no chain state, which argues only
        // that the order does not matter. The second said the interrupt stops
        // an in-flight renew "resurrecting" the lease -- and that fork does not
        // exist: `renewLocked` and `releaseLocked` both read `belief` under the
        // same `BoundedLock`, so a renew arriving after `release()` finds a
        // null belief and returns EMPTY rather than writing. Nothing is
        // resurrected, and this commit's own tests measured that.
        // ⚠️ WHAT THE SIGNAL ACTUALLY BUYS is that a renewer already past its
        // `closed` check does not go on to latch `fenced` and log a "must be
        // restarted" ERROR for what was a deliberate shutdown. It is NOT about
        // saving a store request: after `releaseLocked` nulls the belief,
        // `renewLocked` returns at its `mine == null` guard and issues none.
        // The post-tick `closed` check covers the other ordering, and the two
        // together are pinned by `aCallerAfterCLOSEIsToldCLOSEDRatherThanFENCED`
        // -- each masks the other, so only removing BOTH fails it.
        renewer.interrupt();
        CheckpointWriter writer = checkpoints;
        if (writer != null) {
            writer.close();
        }
        leases.release();
    }
}
