// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One delta per {@code commitBatchInterval}, carrying every flush that arrived
 * in the window (M4.7).
 *
 * <p>⚠️ THIS IS THE $52/MONTH. The M4 SPEC prices the commit chain at one writer
 * times {@code 1 / commitBatchInterval} — four PUT/s at the 250 ms default,
 * twenty at 50 ms — and the number that must not regress is <b>commits per
 * second, not commits per stream</b>. Without this, K pods flushing in one
 * window cost K PUTs and the bill scales with the fleet, which non-negotiable 6
 * forbids by name.
 *
 * <p>⚠️ THE INTERVAL IS THE LATENCY DIAL, and the trade is deliberate: a flush
 * waits up to one interval before its offsets are durable, which is the "seconds
 * of latency" the mission trades for 1/40th of the cost. It is not a
 * performance tweak to be tuned away.
 *
 * <p>⚠️ ONE COMMITTER THREAD, not leader election among the callers. A
 * group-commit built out of "whoever arrives first drives the window" has to
 * answer what happens to a caller that arrives after the batch is taken but
 * before the driver clears its flag — and the answer is a caller waiting for a
 * leader that will never serve it. A dedicated committer makes that state
 * unrepresentable: a request is either in the queue or in a batch, and every
 * batch is completed or failed by the thread that took it.
 */
public final class BatchingSequencer implements Sequencer {

    private static final System.Logger LOG =
            System.getLogger(BatchingSequencer.class.getName());

    /**
     * How long a window stays open once its first request arrives.
     *
     * <p>⚠️ A SEAM, because the alternative is a test that sleeps. testing.md
     * forbids {@code Thread.sleep} in tests for the usual reason — it trades
     * wall-clock for flakiness — and a batcher tested by sleeping would assert
     * "roughly one PUT" rather than exactly one. Production sleeps; a test
     * closes the window when it has queued exactly the requests it means to
     * batch, which is what makes the cost assertion an equality.
     */
    @FunctionalInterface
    public interface WindowTimer {
        /** Blocks until the window that just opened should close. */
        void awaitWindowClose() throws InterruptedException;
    }

    private final Sequencer delegate;
    private final WindowTimer timer;
    private final BlockingQueue<PendingCommit> queue = new LinkedBlockingQueue<>();
    private final Thread committer;
    private final Object lifecycle = new Object();
    private volatile boolean running = true;

    /**
     * What ended the committer, or {@code null} if {@code close} asked it to
     * stop.
     *
     * <p>⚠️ WRITTEN BEFORE THE DOOR SHUTS, so a caller that sees {@code running}
     * false also sees why. Without it every later caller was told "this
     * sequencer is closed" — the same string a real {@code close} produces — and
     * an operator debugging a total stall was pointed at a shutdown that never
     * happened. The cause reached exactly one caller and was discarded.
     */
    private volatile Throwable terminated;

    /**
     * ⚠️ THE DELEGATE IS CLOSED EXACTLY ONCE, by whichever path reaches it
     * first: {@code close}, or the committer's own exit after it died.
     * {@code LocalSequencer.close} is {@code leases.release()}, so a second call
     * would take the lock again for a lease this pod has already let go.
     */
    private final AtomicBoolean delegateClosed = new AtomicBoolean();

    /** Batches on a real clock: the window closes {@code interval} after it opens. */
    public BatchingSequencer(Sequencer delegate, Duration interval) {
        this(delegate, sleepFor(interval));
    }

    public BatchingSequencer(Sequencer delegate, WindowTimer timer) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.timer = Objects.requireNonNull(timer, "timer");
        // ⚠️ A VIRTUAL THREAD, like every other blocking worker here: it spends
        // its life parked on a queue or a window, which is what they are for.
        this.committer = Thread.ofVirtual().name("commit-batcher").start(this::drainForever);
    }

    /**
     * ⚠️ PACKAGE-PRIVATE FOR ONE TEST, because from outside this class the
     * shipping timer is unreachable: the public {@code Duration} constructor
     * installs it and nothing exposes it, so every window test went through the
     * {@link WindowTimer} seam instead and the real timer's arithmetic was
     * never exercised. Timing a whole {@code commit} instead would not
     * discriminate — the surrounding PUT and thread join swamp a sub-millisecond
     * window — so the seam is the honest way to assert it.
     */
    static WindowTimer sleepFor(Duration interval) {
        Objects.requireNonNull(interval, "interval");
        if (interval.isNegative() || interval.isZero()) {
            // ⚠️ A zero interval is not "batch nothing", it is "close the window
            // before anyone else can join", which restores the per-pod PUT rate
            // this class exists to remove — silently, and only under load.
            throw new IllegalArgumentException(
                    "commitBatchInterval must be positive; got " + interval);
        }
        // ⚠️ THE DURATION, NOT `toMillis()`, and the gap between them was a
        // silent 14x. `toMillis()` TRUNCATES, so every interval in (0 ms, 1 ms)
        // passed the guard above — which asks the `Duration` — and then
        // installed `Thread.sleep(0)`: no window at all, which is precisely the
        // state `Duration.ZERO` is refused for, reached by a value the guard
        // accepts. Measured, six pods committing 40/s against a 20 ms PUT:
        // `ofMillis(250)` cost 7 PUTs, `ofNanos(500_000)` cost 98, with no
        // exception and no log. `Thread.sleep(Duration)` honours sub-millisecond
        // values, so the window asked for is the window installed.
        return () -> Thread.sleep(interval);
    }

    @Override
    public CommitDelta commit(CommitRequest request) throws IOException {
        return submit(List.of(Objects.requireNonNull(request, "request")));
    }

    /**
     * ⚠️ GOES THROUGH THE WINDOW LIKE EVERYTHING ELSE, and an earlier draft made
     * it a pass-through, which was wrong twice.
     *
     * <p><b>Cost:</b> the interface declares this THE PRIMITIVE precisely so the
     * batched path is the rule; a pass-through made the primitive the one method
     * that does not batch, so a caller reaching it paid one PUT per submission —
     * a rate scaling with pods, which acceptance criterion 9 forbids by name.
     * M5's commit forwarding, where a remote pod's already-grouped flushes
     * arrive at the leaseholder, is exactly a {@code commitAll}.
     *
     * <p><b>Correctness, and this is the worse half:</b> {@link CommitLog} is
     * single-writer by construction — a plain {@code HashMap} of offsets and a
     * plain {@code long} sequence, with no synchronisation — and the dedicated
     * committer is what makes that safe. A pass-through ran the delegate on the
     * CALLER's thread, concurrently with the committer's, so two deltas could be
     * built from the same offset base. That is I2, and no conditional write
     * catches it: each entry is written once and is internally consistent.
     */
    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        Objects.requireNonNull(requests, "requests");
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("a commit with no segments commits nothing");
        }
        return submit(List.copyOf(requests));
    }

    private CommitDelta submit(List<CommitRequest> requests) throws IOException {
        PendingCommit pending = new PendingCommit(requests);
        // ⚠️ ENQUEUE AND THE CLOSED-CHECK ARE ONE ATOMIC STEP. Reading a flag
        // and then adding to the queue leaves a window a caller can be
        // descheduled inside: it observes `running`, `close` drains, and the
        // request lands after the drain and after the committer has exited —
        // in neither the queue nor a batch, waiting on a future nobody holds.
        // Measured on the previous draft: 24 callers racing one close orphaned
        // one within 102 attempts, still parked two seconds later.
        synchronized (lifecycle) {
            if (!running) {
                // ⚠️ WHICH WAY THE DOOR SHUT, because the two are not the same
                // operational problem: a close is somebody's decision, a death
                // is an incident with a cause worth reading.
                Throwable why = terminated;
                if (why != null) {
                    throw new IOException("the commit batcher terminated abnormally and this "
                            + "sequencer can no longer commit", why);
                }
                throw new IOException("this sequencer is closed and must not commit again");
            }
            queue.add(pending);
        }
        return pending.await();
    }

    /**
     * How many requests are queued but not yet taken into a batch.
     *
     * <p>⚠️ A TEST OBSERVATION POINT, package-private and doing nothing for
     * production. It exists because the alternative is a flaky test: a window
     * driven by "count down a latch, then call commit" closes before the last
     * caller has actually queued — {@code commit} blocks, so the countdown
     * necessarily precedes the enqueue — and the batch then splits across two
     * windows. Measured: that fixture produced 2 PUTs where the criterion
     * demands 1, and made a duplicate-key window pass by separating the clash.
     * A timer that waits on the real depth closes the window exactly when the
     * requests under test are in it, which is what makes criterion 9 an
     * equality rather than an approximation.
     */
    int queuedCount() {
        return queue.size();
    }

    private void drainForever() {
        // ⚠️ THE WHOLE LOOP IS GUARDED, and a first fix guarded only the timer.
        // `commitBatch` narrowed to `IOException | RuntimeException`, so an
        // ERROR out of the delegate killed this thread with `running` still
        // true — callers parked on an uninterruptible join, exactly the defect
        // the timer branch was written to close, one statement over. That is
        // not a theoretical Error: batching exists to make batches big, and
        // `CommitLog` encodes the delta TWICE per attempt, so the
        // OutOfMemoryError window is the batched path itself. Measured: two
        // callers still alive five seconds after an OOM, neither interruptible.
        // ⚠️ SO THE LOOP BODY CANNOT EXIT WITH THE DOOR OPEN. Every exit runs
        // the `finally`, which shuts it and fails whatever is queued.
        Throwable died = null;
        try {
            died = drainLoop();
        } catch (Throwable escaped) {
            died = escaped;
        } finally {
            // ⚠️ TWO NETS, AND THEY ARE NOT INTERCHANGEABLE — an earlier
            // version of this comment said they were, on the strength of the
            // suite staying green when either was dropped, which measured the
            // TESTS rather than the code. This one covers the exit `close`
            // never reaches: the committer dying on its own — a timer throw, an
            // Error — on a live pod with no `close` in sight. Drop it and a
            // caller queued behind that window is parked forever. The net in
            // `close` covers the other direction, a close arriving while the
            // committer is parked in `take()`.
            // ⚠️ THE CAUSE IS RECORDED BEFORE THE DOOR SHUTS, so a caller that
            // loses the race in `submit` is told the batcher DIED rather than
            // that somebody closed it.
            terminated = died;
            stop();
            // ⚠️ THE QUEUE IS FAILED BEFORE ANY I/O, which is the order `close`
            // uses and for the same reason. An earlier draft released the lease
            // FIRST, which put an unbounded store call between the door shutting
            // and the callers being freed: `LeaseManager.release` can block in
            // `takeOrFail` and then issue a PUT, while every queued caller sits
            // on `CompletableFuture.join`, which is UNINTERRUPTIBLE. Measured, a
            // queued caller was still parked three seconds in and completed only
            // once the release was let go.
            failAll(drainRemaining(), terminalFailure());
            if (died != null) {
                // ⚠️ SAID OUT LOUD, ONCE, and this is the only place it can be.
                // Failing closed is defensible; failing closed in silence is
                // not, and `close` twenty lines below already logs the strictly
                // milder join-timeout case. Without this the cause reached one
                // caller and vanished.
                LOG.log(System.Logger.Level.ERROR,
                        "the commit batcher terminated abnormally; this sequencer can no "
                                + "longer commit, and its lease is being released so another "
                                + "node can take over", died);
                // ⚠️ AND THE LEASE GOES. A batcher that fails closed while still
                // holding the lease leaves this pod unable to commit and nobody
                // else able to either, until the TTL expires — the TTL-bound
                // failover `Sequencer.close`'s own javadoc says close exists to
                // avoid. `close` may never come: this is the exit it never
                // reaches.
                closeDelegateQuietly();
            }
        }
    }

    /**
     * The failure handed to callers that arrive, or are still queued, once the
     * committer is gone — naming which of the two ways it went.
     */
    private IOException terminalFailure() {
        Throwable why = terminated;
        return why == null
                ? new IOException("this sequencer is closed")
                : new IOException("the commit batcher terminated abnormally and this sequencer "
                        + "can no longer commit", why);
    }

    /** @return true if this call is the one that owes the delegate a close. */
    private boolean claimDelegateClose() {
        return delegateClosed.compareAndSet(false, true);
    }

    /** ⚠️ For the committer's exit path, which is a {@code finally} and must not throw. */
    private void closeDelegateQuietly() {
        if (!claimDelegateClose()) {
            return;
        }
        // ⚠️ THE FLAG IS CLEARED FIRST, and NOT restored: this thread is
        // terminating, so there is nobody left to carry it to. `drainLoop`'s
        // interrupt branch RE-ARMS the flag before returning the cause, so this
        // path is reached interrupted whenever the death WAS an interrupt — and
        // `LocalSequencer.close` is `leases.release()`, which reaches
        // `BoundedLock.takeOrFail`, whose `tryLock(bound, NANOSECONDS)` throws
        // IMMEDIATELY when the flag is set. Measured: `leaseReleased=false`,
        // with the IOException swallowed below and the ERROR log above still
        // announcing that the lease was being released so another node could
        // take over. `close` handles the identical hazard the same way.
        Thread.interrupted();
        try {
            delegate.close();
        } catch (Throwable releaseFailed) {
            LOG.log(System.Logger.Level.ERROR,
                    "releasing the lease after the commit batcher terminated failed; it will "
                            + "now survive to its TTL", releaseFailed);
        }
    }

    /** @return what ended the loop, or {@code null} if {@code close} did. */
    private Throwable drainLoop() {
        while (running) {
            List<PendingCommit> batch = new ArrayList<>();
            try {
                // ⚠️ THE WINDOW OPENS WITH ITS FIRST REQUEST, not on a fixed
                // tick. An idle sequencer must issue NO request at all (NFR-2),
                // and a timer that fired regardless would write an empty delta
                // — or, worse, keep a chain alive with nothing in it.
                batch.add(queue.take());
                timer.awaitWindowClose();
                queue.drainTo(batch);
                commitBatch(batch);
            } catch (InterruptedException stopping) {
                // ⚠️ ALSO SHUTS THE DOOR, via the finally above. An earlier
                // draft broke out leaving `running` true, which is safe only if
                // `close` is the sole source of an interrupt — and nothing
                // enforces that. A delegate using the textbook idiom
                // (`Thread.currentThread().interrupt(); throw new IOException`),
                // which `BoundedLock` already uses and any S3 backend will use
                // for `ClosedByInterruptException`, re-entered `take()` with the
                // flag set and the committer died silently: the first caller
                // failed correctly, so nothing looked wrong, and the next one
                // hung.
                Thread.currentThread().interrupt();
                failAll(batch, new IOException("the commit batcher was stopped"));
                // ⚠️ AN INTERRUPT WITH THE DOOR STILL OPEN IS NOT A CLOSE.
                // `close` calls `stop()` BEFORE it interrupts, so on the
                // shutdown path `running` is already false here. Seeing it true
                // means the interrupt came from somewhere else — a delegate
                // using the textbook idiom, which is the very case the comment
                // above describes — and this pod is now a leaseholder that
                // cannot commit. That is an abnormal death, and reporting it as
                // a close is how it stayed invisible.
                return running ? stopping : null;
            } catch (Throwable timerFailed) {
                // ⚠️ THE COMMITTER MUST NOT DIE SILENTLY, and an earlier draft
                // let it. `WindowTimer` is a PUBLIC seam taken by a public
                // constructor, so an unchecked throw out of it is an
                // intended-shape input — and it terminated this loop while
                // `running` stayed true, so `commit` kept accepting requests
                // that nothing would ever complete. `PendingCommit.await` is
                // `CompletableFuture.join`, which is UNINTERRUPTIBLE: measured,
                // the caller had not returned after five seconds and could not
                // be interrupted. `DefaultIngest` records the identical lesson
                // in its own words — "a PendingCommit nobody completes is a producer
                // thread hung until SIGKILL".
                // ⚠️ SO THE BATCHER FAILS CLOSED: everything held and everything
                // queued is failed with the cause, and the door is shut so
                // later callers get an immediate IOException instead of a hang.
                failAll(batch, timerFailed);
                return timerFailed;
            }
        }
        return null;
    }

    /** ⚠️ Shuts the door under the same lock `submit` enqueues under. */
    private void stop() {
        synchronized (lifecycle) {
            running = false;
        }
    }

    private void commitBatch(List<PendingCommit> batch) {
        List<CommitRequest> requests = new ArrayList<>(batch.size());
        for (PendingCommit p : batch) {
            requests.addAll(p.requests());
        }
        try {
            CommitDelta delta = delegate.commitAll(requests);
            for (PendingCommit p : batch) {
                p.complete(delta);
            }
        } catch (IOException | RuntimeException failed) {
            // ⚠️ THE WHOLE WINDOW FAILS TOGETHER, and that is the honest answer
            // rather than a convenient one. The delegate writes the batch as ONE
            // conditional PUT, so there is no outcome in which some submissions
            // landed and others did not — reporting success to any caller here
            // would be inventing a durability that does not exist.
            // ⚠️ IT ALSO MEANS ONE POISONED REQUEST FAILS ITS WINDOW-MATES. The
            // only in-protocol poison is two submissions sharing a segment key,
            // which the delegate refuses; segment keys carry pod, sequence and
            // millisecond, so a collision is out-of-protocol and would have
            // overwritten the segment object itself first. Splitting a failed
            // window to find the offender is deliberately NOT done here: it
            // would retry a batch whose PUT may already have landed, which is
            // the ambiguity M4.10 exists to make safe.
            failAll(batch, failed);
        }
    }

    private List<PendingCommit> drainRemaining() {
        List<PendingCommit> left = new ArrayList<>();
        queue.drainTo(left);
        return left;
    }

    private static void failAll(List<PendingCommit> batch, Throwable cause) {
        for (PendingCommit p : batch) {
            p.fail(cause);
        }
    }

    /**
     * ⚠️ STOPS THE COMMITTER, THEN THE DELEGATE, and fails whatever was queued.
     * A caller blocked in {@link #commit} when close arrives must not wait for a
     * window that will never close — the {@code Sequencer} contract says a
     * commit after close must FAIL rather than silently succeed, and a hang is
     * neither.
     */
    @Override
    public void close() throws IOException {
        stop();
        committer.interrupt();
        // ⚠️ JOINED, NOT MERELY SIGNALLED, and an earlier draft only signalled.
        // `delegate.close()` releases the lease; running it while the committer
        // is still inside `CommitLog`'s redrive loop lets a retry win a slot
        // AFTER this node gave up its term and a successor sealed the chain —
        // the fork `LocalSequencer`'s own javadoc says must not exist, silently
        // reassigning offsets another leader has issued. That is I2, bought for
        // the sake of not waiting a few milliseconds here.
        // ⚠️ THE INTERRUPT MAY ALREADY BE SET ON THIS THREAD, in which case
        // `join` throws IMMEDIATELY and waits for nothing — measured, with
        // `closeTookMs=0` and the delegate closed under an in-flight commit,
        // which is the I2 fork this join exists to prevent. A shutdown path
        // that caught InterruptedException, restored the flag with the textbook
        // idiom this class's own drain loop uses, and then closed in a finally
        // would hit it every time. So the flag is cleared for the duration of
        // the wait and restored afterwards, and the RESULT is acted on.
        boolean wasInterrupted = Thread.interrupted();
        boolean stopped = false;
        try {
            stopped = committer.join(Duration.ofSeconds(10));
        } catch (InterruptedException interruptedWhileWaiting) {
            wasInterrupted = true;
        }
        if (!stopped) {
            // ⚠️ SAID OUT LOUD rather than proceeding silently. Past this point
            // `delegate.close()` may release a lease while the committer is
            // still inside `CommitLog`'s uninterruptible redrive loop; the
            // epoch-in-path fencing bounds the damage, but an operator has no
            // other way to learn it happened.
            LOG.log(System.Logger.Level.WARNING,
                    "the commit batcher did not stop within 10s; closing the delegate "
                            + "with a commit possibly still in flight");
        }
        // ⚠️ AFTER the join, so anything the committer put back cannot be
        // stranded, and `stop()` under the lock means nothing new can arrive.
        failAll(drainRemaining(), terminalFailure());
        try {
            // ⚠️ SKIPPED IF THE COMMITTER ALREADY CLAIMED THE CLOSE, which is
            // the case this CAS exists for: the committer died, released the
            // lease on its own exit, and there is nothing left here to do.
            // ⚠️ WHEN IT DOES RUN IT PROPAGATES, because `close` is declared
            // `throws IOException` and a caller shutting a pod down needs to
            // hear that the lease did not come back. ⚠️ BUT NOT IN THE HANDOVER
            // CASE, and saying so is the point: if the committer's own release
            // FAILED, that failure was logged at ERROR and swallowed there, and
            // this method then returns cleanly having done nothing. The operator
            // learns it from the log; this caller does not learn it at all.
            // Making it learn would mean holding the committer's outcome and
            // rethrowing it here, which is a wider change than M4.7 asked for.
            if (claimDelegateClose()) {
                delegate.close();
            }
        } finally {
            // ⚠️ THE FLAG IS RESTORED LAST, AFTER the delegate is closed, and
            // restoring it before was measured releasing NOTHING:
            // `LocalSequencer.close` is `leases.release()`, which falls through
            // to `BoundedLock.takeOrFail` whenever `tryLock` loses to an
            // in-flight renew — and `tryLock(bound, NANOSECONDS)` throws
            // IMMEDIATELY when the flag is already set. The lease then survives
            // to its TTL, which is the ~10 s failover this whole method exists
            // to avoid, and `close` throws so the caller's remaining shutdown
            // never runs. Measured both ways: `leaseReleased=false, closeThrew`
            // before, `leaseReleased=true, closeThrew=none` after, with the
            // caller's interrupt still restored either way. `ExecutorService.close`
            // puts the restore last for the same reason.
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
