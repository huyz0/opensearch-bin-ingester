// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The term this pod holds, if it holds one (M5.6d, FR-11).
 *
 * <p>⚠️ IT IS ITS OWN OBJECT BECAUSE ITS RULES ARE ITS OWN, and three review
 * rounds on {@link FleetSequencer} were all about these rules rather than about
 * forwarding. Taking a term, giving one up, and deciding that a pod does not
 * have one are decisions with sharp edges; committing through whatever comes
 * back is not.
 *
 * <p>⚠️ NO LOCK IS HELD ACROSS I/O. The only mutable state is one
 * {@link AtomicReference}, and the only exclusion is a non-blocking
 * {@link BoundedLock#tryLock()} around the ELECTION — so a caller that finds
 * another thread already electing gets "not leading" rather than queueing
 * behind a chain recovery. A monitor here would serialise every commit in the
 * pod: a leader wrapped in {@code BatchingSequencer} could then never batch,
 * turning one delta PUT per window into one per pod, which is the rate
 * non-negotiable 6 forbids by name. It is also what java-style.md rule 6
 * forbids and what {@link LeaseManager} cites for rejecting a monitor of its
 * own.
 */
public final class Leadership implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(Leadership.class.getName());

    /**
     * How this pod becomes a leader when there is a vacancy.
     *
     * <p>⚠️ It returns a {@link Sequencer}, not a {@link LocalSequencer}, so a
     * leader can be wrapped: {@code BatchingSequencer} folds a window's flushes
     * into one delta PUT, and a signature naming the concrete class would put
     * it out of reach on the one path where the bill scales with the fleet.
     */
    @FunctionalInterface
    public interface Election {
        /** Attempts to take the term; empty when someone else holds it. */
        Optional<? extends Sequencer> tryLead() throws IOException;
    }

    private final Election election;
    private final AtomicReference<Sequencer> held = new AtomicReference<>();
    private final BoundedLock electing = new BoundedLock();
    private volatile boolean closed;

    /**
     * Runs between publishing a won term and re-reading {@link #closed}.
     *
     * <p>⚠️ PACKAGE-PRIVATE FOR ONE TEST, and the reason is that the arm it
     * exists to reach is otherwise reachable only through a thread
     * interleaving. The compare-and-set below fails exactly when {@code close}
     * lands in this window — between the publish and the re-read — and that
     * losing arm is the one M5.6f fixed. A test that raced two real threads for
     * it would assert a schedule; this asserts the rule.
     *
     * <p>⚠️ A NO-OP IN EVERY SHIPPING PATH, installed nowhere but a test, and
     * the same licence {@code LocalSequencer.sleepFor} and
     * {@code checkpointEveryDeltas} are given for the same measured reason: a
     * seam nothing shipping installs is a seam no test exercises, and the
     * alternative here is production logic with no test at all.
     */
    volatile Runnable afterPublish = () -> {
    };

    public Leadership(Election election) {
        this.election = java.util.Objects.requireNonNull(election, "election");
        // ⚠️ ONE ATTEMPT AT BOOT, so a cold cluster elects a leader without
        // waiting for a commit. Failing to WIN is not an error -- it is the
        // normal state of five pods out of six -- and neither is failing to
        // ASK: a store hiccup during a rolling restart must leave a pod booting
        // as a FOLLOWER, not unable to start at all.
        held.set(tryLead());
    }

    /**
     * The term this pod already holds, or {@code null} — WITHOUT electing
     * (M5.15d).
     *
     * <p>⚠️ IT EXISTS BECAUSE {@link #sequencer()} ELECTS AS A SIDE EFFECT,
     * which is right for a commit and wrong for a reader: the push site asks
     * for the chain epoch once per flush, and taking a term to answer would
     * make reading a field acquire a lease, seal an ancestor and replay a
     * chain.
     */
    Sequencer heldWithoutElecting() {
        return held.get();
    }

    /**
     * The sequencer this pod may write the chain with, or {@code null}.
     *
     * <p>⚠️ IT ELECTS INTO A VACANCY, because the holder may have died since the
     * last commit and a vacancy nobody claims is an outage. Promotion therefore
     * happens on a commit rather than on a timer: a pod that lost the election
     * at boot must be able to lead when the holder dies, or a leader's death is
     * a permanent outage for every follower.
     */
    Sequencer sequencer() {
        Sequencer mine = held.get();
        if (mine != null) {
            return mine;
        }
        // ⚠️ A CLOSED POD DOES NOT TAKE A TERM. Without this, a commit arriving
        // after close -- an in-flight flush, a queued batch -- re-acquires the
        // lease this pod has just voluntarily released, and nothing is left to
        // give it back: every other pod then reads an unexpired lease and none
        // can lead. `LocalSequencer` guards exactly this with a closed field.
        if (closed) {
            return null;
        }
        if (!electing.tryLock()) {
            return held.get();
        }
        try {
            Sequencer already = held.get();
            if (already != null) {
                return already;
            }
            Sequencer won = tryLead();
            if (won == null) {
                return null;
            }
            held.set(won);
            afterPublish.run();
            // ⚠️ RE-READ AFTER PUBLISHING, and give the term straight back if
            // close happened while the election was inside the store. Winning
            // takes a `tryAcquire`, a seal and a `recover()` whose latency is
            // unbounded in chain length, so a SIGTERM lands in that window
            // easily -- and `close` would otherwise have read a null reference,
            // closed nothing, and reported a clean shutdown while this thread
            // went on to install a leader with a live renewer that nobody will
            // ever stop.
            if (closed) {
                // ⚠️ NULL ON BOTH OUTCOMES OF THE COMPARE-AND-SET (M5.6f), and
                // an earlier version returned `won` when it FAILED. The CAS
                // fails exactly when `close` got there first -- so it has
                // already closed that sequencer, and handing it to a caller
                // hands back a closed one. Winning the CAS means close did not
                // take it and this thread must give it back itself; losing
                // means close is doing so. Neither is a term this caller holds.
                if (held.compareAndSet(won, null)) {
                    giveBack(won);
                }
                return null;
            }
            return won;
        } finally {
            electing.unlock();
        }
    }

    /** Closes a term this pod turns out not to want, without failing its caller. */
    private void giveBack(Sequencer won) {
        try {
            won.close();
        } catch (IOException closeFailed) {
            LOG.log(System.Logger.Level.WARNING,
                    "a term won during close did not close cleanly", closeFailed);
        }
    }

    /** Whether this pod took the term. */
    public boolean isHeld() {
        return held.get() != null;
    }

    /**
     * The election's answer, with "could not ask" folded into "did not win".
     *
     * <p>⚠️ COULD NOT ASK IS NOT COULD NOT FORWARD, and conflating them inverts
     * the caller. {@code LocalSequencer.start} THROWS for a lock not taken
     * within the renew interval, a 503 on the lease {@code stat}, a 403 from a
     * bad role — and returns empty only for "someone else holds it". A follower
     * asks on every commit, so letting that escape would fail a flush the lease
     * GET and the peer RPC would both have carried, and would report it as
     * AMBIGUOUS per {@link Sequencer#commit} when provably nothing was
     * attempted.
     */
    private Sequencer tryLead() {
        try {
            return election.tryLead().orElse(null);
        } catch (IOException cannotAsk) {
            LOG.log(System.Logger.Level.WARNING,
                    "could not ask whether the term is going; not leading", cannotAsk);
            return null;
        }
    }

    /**
     * Puts down a sequencer whose term has ended.
     *
     * <p>⚠️ THE REFERENCE IS DROPPED FIRST, AND THE CLOSE CANNOT UNDO IT. A
     * first draft closed and then dropped, which wedged the pod for the life of
     * the process on one transient store error: {@link LocalSequencer#close}
     * releases the lease, so its {@code putIfMatch} can throw — and that
     * IOException replaced the fence (making a commit that provably appended
     * NOTHING look ambiguous, and an ambiguous one may not be re-sent to
     * ANOTHER pod — only a fence authorises that) AND left
     * the reference on a closed sequencer, whose later refusal is deliberately
     * not a {@link FencedException}. The fenced branch was then unreachable
     * forever after.
     *
     * <p>⚠️ COMPARE-AND-SET, so two threads racing into one fence close once.
     */
    void retire(Sequencer fenced, FencedException why) {
        if (!held.compareAndSet(fenced, null)) {
            return;
        }
        try {
            // ⚠️ CLOSED, NOT MERELY DROPPED. When the fence came from a seal
            // rather than the renewer, nothing has latched: the renewer wakes
            // and spends a stat, a get and a losing putIfMatch renewing a term
            // someone else holds, once per fencing, forever.
            fenced.close();
        } catch (IOException closeFailed) {
            // ⚠️ REPORTED, NEVER SUBSTITUTED: the fence is the authoritative
            // fact and the commit is forwarded on it. Failing the producer
            // because the goodbye did not land is the defect above, smaller.
            why.addSuppressed(closeFailed);
            LOG.log(System.Logger.Level.WARNING,
                    "a fenced sequencer did not close cleanly; its term is over either way",
                    closeFailed);
        }
    }

    /**
     * Gives the term back, WITHOUT waiting for an election already in the store.
     *
     * <p>⚠️ NOT WAITING IS A DECISION, NOT AN OVERSIGHT (ADR-0038). Taking the
     * election lock here was written, measured and withdrawn: it made the
     * hand-back deterministic and cost more than it bought. {@code takeOrFail}
     * throws {@code InterruptedException} when the caller's interrupt flag is
     * ALREADY set, and {@code DefaultIngest.close} arrives in exactly that
     * state — so the wait turned a successful release into no release at all,
     * on the one path it exists for. And the bound composes: this wait, then
     * {@code BatchingSequencer}'s join, then {@code LeaseManager.release}'s own
     * {@code takeOrFail(ttl)}, is ~30 s at ADR-0007's TTL against the 5 s
     * {@code DefaultIngest} already cut a wait to because thirty "exceeded
     * Kubernetes' default grace period".
     *
     * <p>⚠️ SO THE RACE IS RECONCILED RATHER THAN EXCLUDED, in
     * {@link #sequencer()}, and the losing arm of that reconciliation returns
     * null rather than a sequencer this method has already closed.
     */
    @Override
    public void close() throws IOException {
        // ⚠️ THE FLAG GOES UP FIRST, so an election already inside the store
        // sees it when it comes back, and one that has not started never does.
        closed = true;
        Sequencer mine = held.getAndSet(null);
        if (mine != null) {
            // ⚠️ RELEASED, NOT ABANDONED. `LocalSequencer.close` hands the lease
            // back, which is the difference between a successor taking over in
            // milliseconds and waiting out the TTL (~10 s, ADR-0007).
            mine.close();
        }
    }
}
