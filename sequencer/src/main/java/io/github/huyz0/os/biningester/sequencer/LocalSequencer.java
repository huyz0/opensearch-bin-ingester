// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Seal;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.Lease;
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
 * <p>⚠️ COMMIT FORWARDING IS M5's PROTOCOL AND M8's TRANSPORT. A node that does
 * not hold the lease gets an empty {@link Optional} here and reaches the node
 * that does through {@link RemoteSequencer} — over a TEST
 * {@link SequencerTransport}, the only implementation in the tree. ⚠️ THIS
 * PARAGRAPH SAID "not correct until M5", which now reads as "correct today"
 * because M5 is complete; the production transport is M5.6e, owned by M8, so a
 * multi-node deployment is still not correct. The M4 SPEC states that as a
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

    private final LeaseManager leases;
    private final CommitLog log;
    private final RenewTicker ticker;
    private final BinStore store;
    private final String prefix;
    private final Thread renewer;
    private final Object drainLock = new Object();
    private volatile boolean closed;
    private volatile FastLeaderTerm fastTerm;
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
    volatile CheckpointWriter checkpoints;

    /**
     * What each pod incarnation has already had applied (M4.10d).
     *
     * <p>⚠️ CONFINED TO THE COMMIT PATH, AND GUARDED BY ITS LOCK. Every read and
     * write happens inside {@link #commitAll}, which is {@code synchronized}
     * because a leader has two commit paths at once (M8.49) -- the lease makes
     * ONE NODE the writer, not one thread. Unlike {@link #checkpoints}, which
     * the renewer thread also touches and which is therefore {@code volatile}.
     */
    private final IdempotencyWindow window;

    /**
     * ⚠️ DEFAULTS, and they are a COST choice rather than a correctness one: one
     * checkpoint per 1000 deltas or per minute, whichever comes first, so a busy
     * cluster pays a fraction of a percent on top of its commit rate and an idle
     * one pays nothing at all (the writer is dirty-flagged). M4.9 measures what
     * they should be; these are what it measures against.
     */
    public static final long CHECKPOINT_EVERY_DELTAS = 1000;

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
        this.renewer = Thread.ofVirtual().name("lease-renewer").start(new LeaseRenewer(
                leases, ticker, () -> closed, () -> fenced, this::fence, log::epoch));
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

    /**
     * Marks this term fenced and stops its checkpoint writer (see
     * {@link LeaseRenewer}): ⚠️ THE WRITER STOPS WITH THE LEASE.
     */
    private void fence() {
        fenced = true;
        CheckpointWriter fencedWriter = checkpoints;
        if (fencedWriter != null) {
            fencedWriter.close();
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
                    checkpointEveryDeltas, CHECKPOINT_INTERVAL, checkpointTicker);
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

    /**
     * The term under {@code sequencer}, through a {@link BatchingSequencer}.
     * ⚠️ M8.50: the elected term is BATCHED, so {@code instanceof} on it is
     * false, and retention reading that as "no term" would silently stop GC.
     */
    public static Optional<LocalSequencer> underneath(Sequencer sequencer) {
        Sequencer inner = sequencer instanceof BatchingSequencer batched
                ? batched.delegate() : sequencer;
        return inner instanceof LocalSequencer local ? Optional.of(local) : Optional.empty();
    }
    Object drainLock() { return drainLock; }
    /**
     * The epoch this instance sequences at.
     *
     * <p>⚠️ IT IS ALSO {@link Sequencer#epoch()} SINCE M5.15d, which the push
     * channel reads to carry the sequencer epoch beside a consumer's own
     * session epoch (SPEC criterion 12). This method predates that and needed
     * no change: it is the lease epoch this instance won, and it never moves
     * for the life of the instance, because a lease that moves FENCES this one
     * rather than re-pointing it.
     */
    @Override
    public long epoch() {
        return log.epoch();
    }

    /**
     * The chain this term has written, in memory (M8.3, M7.25).
     *
     * <p>⚠️ **THE ONLY PRODUCER OF THE {@code List<CommitDelta>} A RETENTION
     * PASS TAKES.** Reaching for {@code recover()} instead would cost one LIST
     * per 1,000 deltas plus one GET per delta, on every pass, for ever —
     * ADR-0052 § 3 is why this exists before any GC loop does.
     */
    public ChainMemory chain() {
        return log.chain();
    }

    /** A stream's committed next offset in this term's chain (M13.27j). */
    public synchronized long committedNext(io.github.huyz0.os.biningester.format.RunKey key) {
        return log.nextOffset(key);
    }

    /** This term's fast-mode side, once its start opened it (M13.27j). */
    public Optional<FastLeaderTerm> fastTerm() {
        return Optional.ofNullable(fastTerm);
    }

    public void attach(FastLeaderTerm term) {
        this.fastTerm = Objects.requireNonNull(term, "term");
    }

    /**
     * Whether this term may still act as the chain's writer -- neither fenced
     * nor closed (M8.5).
     *
     * <p>⚠️ **HELD IS NOT THE SAME AS SERVING, AND GC IS WHERE THE DIFFERENCE
     * DELETES DATA.** {@code FleetSequencer.heldTerm()} keeps returning a term
     * after its renewer has fenced it: the term is retired only when a COMMIT
     * through it throws, so a node that receives no writes keeps a dead term
     * indefinitely. Its chain is then frozen at the takeover while the
     * successor keeps committing. A retention loop that read that chain as the
     * orphan sweep's keep list would list the successor's hours, find its
     * committed segments in no delta, and delete them -- with the GC lease
     * genuinely held, so the fence in {@code LeasedGc} lets every batch
     * through. Found by M8.5's review.
     *
     * <p>⚠️ **THREE CONDITIONS, BECAUSE A TERM LAPSES THREE WAYS** -- and
     * review found the first version checking only the flags. {@code fenced}
     * is set ONLY when a renew comes back empty. A renew that throws
     * {@code IOException} leaves the term untouched however many times it
     * fails, and a renewer that dies returns without setting anything; its own
     * log says the term "will lapse at its TTL and the cluster will fail over".
     * In both the node goes on HOLDING a term a successor now has, with the
     * flags saying all is well. So the term must also be unexpired by its own
     * clock ({@link LeaseManager#heldUnexpired()}), which bounds every path at
     * the TTL whatever the renewer did or failed to do.
     *
     * <p>⚠️ **NO REQUEST**: two flags and a clock comparison. The sweep cannot
     * reach a successor's hours for at least an hour plus a grace after a
     * takeover, and a successor cannot take over before this term's TTL, so a
     * TTL-bounded answer is ample.
     */
    public boolean serving() {
        return !fenced && !closed && leases.heldUnexpired();
    }

    /**
     * Records where each stream's retained data now starts, so the next
     * checkpoint carries it (M8.5, M7.10).
     *
     * <p>⚠️ **THE RETENTION LOOP's SINK, AND ITS ONLY WAY IN.** The checkpoint
     * writer is package-private and belongs to this term; a pass that reported
     * its boundary anywhere else would leave the checkpoint saying 0 for ever,
     * which is the literal M7's SPEC called the mutation that survives every
     * other case -- a consumer's refusal would have no boundary to be below.
     *
     * <p>⚠️ **A TERM WITHOUT A WRITER DROPS IT**, and that is not a loss:
     * {@code observeRetained} merges with {@code max}, so the next pass
     * reports a boundary at least as far along, and a boundary reported late
     * refuses a consumer LESS, never for records still in the bucket.
     */
    public void observeRetained(Map<io.github.huyz0.os.biningester.format.RunKey, Long> oldestRetained) {
        Objects.requireNonNull(oldestRetained, "oldestRetained");
        CheckpointWriter writer = checkpoints;
        if (writer != null) {
            writer.observeRetained(oldestRetained);
        }
    }

    /**
     * Commits, one caller at a time.
     *
     * <p>⚠️ **SYNCHRONIZED, BECAUSE A LEADER HAS TWO COMMIT PATHS AT ONCE**
     * (M8.49). Its own flush commits through {@code FleetSequencer}, and every
     * follower's forwarded commit arrives on a {@code CommitService} handler
     * thread that calls this directly. {@link CommitLog} is single-writer by
     * construction: a plain map of offsets and a plain slot counter.
     * Unserialised, two commits can build their deltas from the same offset
     * base, which is I2. MEASURED: M8.12's review found slot 8 reassigning
     * [450,500) after slot 7 had assigned up to 550, inside one epoch. {@code BatchingSequencer} was built to be the
     * single committer and was never wired. Wiring it also batches, and that
     * is a cost change with its own row. This is the correctness fix alone,
     * and it costs nothing a HEAD node had.
     */
    @Override
    public synchronized CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        if (fenced) {
            // ⚠️ REFUSED HERE, BEFORE THE STORE. `CommitLog` would also refuse,
            // because a successor sealed this chain -- but only after a round
            // trip, and only once the seal is visible. This node already KNOWS,
            // and spending a request to be told again is both slower and a
            // request that scales with a fenced pod's retry rate.
            // ⚠️ A TYPE, NOT A MESSAGE. A caller re-sends TO ANOTHER POD on
            // exactly this failure and on no other, because exactly this one
            // proves nothing was appended. Matching on the text does not work:
            // the close refusal below names the lease too.
            // ⚠️ "TO ANOTHER POD" IS THE WHOLE QUALIFICATION, and it was added
            // when M5.23 made a resend to THIS sequencer answerable: an
            // ambiguous append is reconciled against the slot it named, but the
            // mark that does it lives in this instance and dies with its term.
            throw new FencedException("this sequencer lost its lease at epoch " + log.epoch()
                    + " and has been fenced; it must not commit again");
        }
        if (closed) {
            // ⚠️ The Sequencer contract says a commit after close must FAIL. The
            // lease is released, so another node may already have sealed this
            // chain; succeeding here would be writing past a barrier that exists.
            throw new IOException("this sequencer released its lease at epoch "
                    + log.epoch() + " and must not commit again");
        }
        // ⚠️ BEFORE THE CLASSIFICATION, because it is what the classification
        // is missing (M5.23). An append whose response was lost was never
        // recorded as applied, so the window would call an already-committed
        // flush fresh and commit it again.
        reconcileAmbiguousAppend();
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

    /**
     * The append this instance made and never learned the outcome of (M5.23).
     *
     * <p>⚠️ GUARDED BY {@link #commitAll}'s LOCK, like {@link #window}: it is
     * written where an append is made and read where the next one is
     * classified, both inside that method.
     */
    private AmbiguousAppendException ambiguousAppend;

    /**
     * Seeds the window from the slot an ambiguous append named, so a retry of
     * anything that batch carried is answered rather than committed again.
     *
     * <p>⚠️ EVERY ATTRIBUTION IN THE DELTA, not only the ones about to be
     * resubmitted. The delta that landed carries the whole batch, and a caller
     * regrouping its retries -- which {@code BatchingSequencer} does by
     * construction -- may resubmit a subset, a superset, or both at once.
     *
     * <p>⚠️ AN EMPTY SLOT MEANS THE APPEND NEVER LANDED, and clearing the mark
     * is what lets the retry commit it for the first time. Refusing instead
     * would wedge this sequencer for the life of the process: the slot stays
     * empty precisely because nothing may commit until it is read.
     *
     * <p>⚠️ A FAILED READ KEEPS THE MARK and propagates, because the outcome is
     * still unknown -- and a store that cannot answer this read is a store the
     * commit behind it could not have reached either.
     *
     * <p>⚠️ IT SEEDS THE CHECKPOINT AS WELL AS THE WINDOW (M5.25), and seeding
     * only the window was a gap invisible from either side: this pod answered
     * the retry and the next pod did not, because {@code CheckpointWriter}
     * learns only from a commit that RETURNED. Both are seeded from the same
     * delta this reconciliation already read.
     */
    private void reconcileAmbiguousAppend() throws IOException {
        AmbiguousAppendException pending = ambiguousAppend;
        if (pending == null) {
            return;
        }
        Optional<CommitDelta> landed =
                DeltaReader.ifWritten(store, prefix, pending.epoch(), pending.sequence());
        ambiguousAppend = null;
        if (landed.isEmpty()) {
            return;
        }
        for (SegmentCommit segment : landed.get().segments()) {
            SegmentCommit.Attribution attribution = segment.attribution();
            if (attribution != null) {
                window.applied(attribution, pending.epoch(), landed.get().sequence());
            }
        }
        // ⚠️ AND THE CHECKPOINT, WHICH IS WHAT A SUCCESSOR INHERITS (M5.25).
        // Seeding only the in-memory window left this pod answering the retry
        // and the next pod not: `CheckpointWriter` learns from commits that
        // RETURNED, so an ambiguously-landed flush reached it never, and a
        // successor inherited the fact only while that delta was still in the
        // uncheckpointed tail.
        CheckpointWriter writer = checkpoints;
        if (writer != null) {
            writer.observeReconciled(landed.get().segments(), pending.epoch(),
                    landed.get().sequence());
        }
    }

    /** Commits {@code requests} for real, and records what that applied. */
    private CommitDelta applyFresh(List<CommitRequest> requests) throws IOException {
        CommitDelta delta;
        try {
            delta = log.commitAll(requests);
        } catch (AmbiguousAppendException ambiguous) {
            // ⚠️ REMEMBERED, THEN RETHROWN. The commit genuinely failed from
            // this caller's point of view -- nothing here knows whether the
            // records are durable, so acknowledging them would be a lie. What
            // is kept is the slot, so the NEXT commit can find out.
            ambiguousAppend = ambiguous;
            throw ambiguous;
        }
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
        // ⚠️ CHECKPOINT BEFORE THE RELEASE, while the term is still held (M8.16),
        // and release whatever it did: a throw would strand the term for a TTL.
        try {
            if (writer != null) {
                writer.checkpointAndClose();
            }
        } finally {
            leases.release();
        }
    }
}
