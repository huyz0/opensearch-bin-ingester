// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What a caller and an operator LEARN when the committer dies on its own (M4.7).
 *
 * <p>⚠️ SPLIT FROM {@link BatchingSequencerLifecycleTest}, which was at the
 * 500-line limit. That file asks whether a caller can be left hanging, and the
 * answer there is already no — every path fails its callers. This file asks the
 * question that survived it: once the batcher has failed closed, is what the
 * caller is TOLD true, and is the lease let go?
 *
 * <p>⚠️ THE DISTINCTION IS NOT COSMETIC. A batcher that died reports the same
 * {@code IOException} a deliberate {@code close} produces, so an operator
 * debugging a total stall is pointed at a shutdown that never happened; and
 * because the delegate is never closed, {@code LocalSequencer} never reaches
 * {@code leases.release()}, so the pod holds a lease it can no longer commit
 * through until the TTL expires. {@code Sequencer.close}'s own javadoc names
 * that TTL-bound failover as the thing close exists to avoid.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BatchingSequencerTerminationTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    /** ⚠️ The window closes at once, so the batch is exactly the request under test. */
    private static final BatchingSequencer.WindowTimer IMMEDIATE = () -> {
    };

    private static CommitRequest from(String pod, long flushSeq) {
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        counts.put(new RunKey(A, 0), 3);
        return new CommitRequest(pod, flushSeq, "bins/" + pod + "/" + flushSeq + ".bseg", counts);
    }

    /**
     * Polls until {@code flag} is set, because the committer runs its exit path
     * on its own thread after the caller it failed has already returned.
     */
    private static void awaitSet(AtomicBoolean flag, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!flag.get() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(flag.get()).as(what).isTrue();
    }

    /** A delegate whose {@code commitAll} always dies, recording whether it was closed. */
    private static final class DyingDelegate implements Sequencer {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) {
            // ⚠️ AN ERROR, NOT AN EXCEPTION, and the class's own comment says why
            // this is the realistic case rather than a contrived one: batching
            // exists to make batches big and `CommitLog` encodes the delta twice
            // per attempt, so OutOfMemoryError is the batched path's own hazard.
            throw new OutOfMemoryError("simulated: encoding a large batch");
        }

        @Override
        public void close() {
            closes.incrementAndGet();
            closed.set(true);
        }
    }

    @Test
    void aCommitterThatDIEDDoesNotTELLTheNextCallerItWasCLOSED() throws Exception {
        DyingDelegate delegate = new DyingDelegate();
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        assertThatThrownBy(() -> batching.commit(from("poda", 1)))
                .as("the caller in the window gets the real cause")
                .isInstanceOf(OutOfMemoryError.class);
        awaitSet(delegate.closed, "the committer's exit path has run");

        // ⚠️ THE POINT OF THE TEST. `close` was never called, so a message
        // saying the sequencer is closed is false, and it is the ONLY thing a
        // later caller ever sees -- the cause reached exactly one caller and was
        // discarded everywhere else.
        assertThatThrownBy(() -> batching.commit(from("poda", 2)))
                .isInstanceOf(IOException.class)
                .as("a caller after an abnormal death is not told it was a close")
                .hasMessageNotContaining("this sequencer is closed and must not commit again")
                .as("it is told the batcher terminated, and why")
                .hasMessageContaining("terminated")
                .hasRootCauseMessage("simulated: encoding a large batch");
    }

    @Test
    void aCommitterThatDIEDReleasesTheLEASERatherThanHoldingItToItsTTL() throws Exception {
        DyingDelegate delegate = new DyingDelegate();
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        assertThatThrownBy(() -> batching.commit(from("poda", 1)))
                .isInstanceOf(OutOfMemoryError.class);

        // ⚠️ `LocalSequencer.close` IS `leases.release()`. A batcher that fails
        // closed without reaching it leaves this pod holding the lease for the
        // whole TTL while being unable to commit through it -- no other pod can
        // take over, so the cluster stops sequencing rather than failing over.
        awaitSet(delegate.closed, "the delegate is closed, so the lease is released");
    }

    @Test
    void aCloseAFTERTheCommitterAlreadyDiedDoesNotReleaseTheLeaseTWICE() throws Exception {
        // ⚠️ THE ONE INTERLEAVING THE CAS EXISTS FOR, and nothing reached it:
        // no test called `close()` on a batcher whose committer had already
        // died, so `claimDelegateClose()` could be
        // `{ delegateClosed.set(true); return true; }` -- always true -- with
        // the whole tree green and `leases.release()` running TWICE. The
        // field's own javadoc makes single-release an explicit correctness
        // claim, so leaving it unpinned is a claim that cannot go false.
        DyingDelegate delegate = new DyingDelegate();
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        assertThatThrownBy(() -> batching.commit(from("poda", 1)))
                .isInstanceOf(OutOfMemoryError.class);
        awaitSet(delegate.closed, "the committer released the lease on its way out");

        // ⚠️ THE ORDINARY SHUTDOWN STILL ARRIVES. A pod whose committer died
        // does not stop being closed by whoever owns it.
        batching.close();

        assertThat(delegate.closes.get())
                .as("the lease is released exactly once, by whichever path got there first")
                .isEqualTo(1);
    }

    @Test
    void aDelegateThatRESTORESTheInterruptFlagDoesNotStrandEveryLaterCaller() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        AtomicInteger calls = new AtomicInteger();
        Sequencer delegate = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                // ⚠️ THE TEXTBOOK IDIOM, verbatim: `BoundedLock` already uses it
                // and any S3 backend will for `ClosedByInterruptException`. It
                // leaves the flag set on the COMMITTER, which then re-enters
                // `take()` and dies -- with nobody having called `close`.
                calls.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new IOException("the store call was interrupted");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        assertThatThrownBy(() -> batching.commit(from("poda", 1)))
                .isInstanceOf(IOException.class)
                .hasMessage("the store call was interrupted");
        awaitSet(closed, "an interrupt with no close behind it still releases the lease");

        // ⚠️ THE COMMITTER IS GONE, so this must fail immediately and say so
        // truthfully. The delegate is deliberately NOT consulted again.
        assertThatThrownBy(() -> batching.commit(from("poda", 2)))
                .isInstanceOf(IOException.class)
                .as("an interrupt-killed committer is not reported as a close")
                .hasMessageNotContaining("this sequencer is closed and must not commit again")
                .hasMessageContaining("terminated");
        assertThat(calls.get()).as("the dead committer took no further batch").isEqualTo(1);
    }

    @Test
    void closeRestoresTheInterruptFlagAFTERTheDelegateHasReleasedTheLease() throws Exception {
        // ⚠️ THE ORDERING WAS UNPINNED, and only the ordering: `close` already
        // CLEARS the flag for the join, and a test kills that. What nothing
        // reached was the restore happening AFTER `delegate.close()` rather
        // than before, because every delegate in every fixture had an
        // interrupt-INSENSITIVE close -- `{}` or `closed.set(true)` -- so both
        // orderings passed identically.
        // ⚠️ REAL CLOSES ARE NOT INSENSITIVE. `LocalSequencer.close` is
        // `leases.release()`, which reaches `BoundedLock.takeOrFail`, and
        // `tryLock(bound, NANOSECONDS)` throws IMMEDIATELY when the calling
        // thread already carries an interrupt. Restoring first therefore
        // releases NOTHING: the lease survives to its TTL -- the ~10 s failover
        // this method exists to avoid -- and `close` throws, so the caller's
        // remaining shutdown never runs. This delegate models exactly that.
        AtomicBoolean released = new AtomicBoolean();
        Sequencer delegate = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                throw new UnsupportedOperationException("no commit in this test");
            }

            @Override
            public void close() throws IOException {
                if (Thread.currentThread().isInterrupted()) {
                    throw new IOException("interrupted before the lease could be released");
                }
                released.set(true);
            }
        };
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        // ⚠️ THE CLOSING THREAD ALREADY CARRIES AN INTERRUPT, which is the
        // realistic shutdown: a pod draining on a signal, or any caller using
        // the textbook restore idiom before closing in a finally.
        Thread.currentThread().interrupt();
        try {
            batching.close();
        } finally {
            // ⚠️ CONSUMED HERE so the flag cannot leak into the next test.
            boolean stillSet = Thread.interrupted();
            assertThat(stillSet)
                    .as("the caller's interrupt is handed back, not swallowed")
                    .isTrue();
        }

        assertThat(released.get())
                .as("the lease is released despite the caller arriving interrupted")
                .isTrue();
    }

    @Test
    void aCommitterKilledByAnINTERRUPTStillReleasesTheLease() throws Exception {
        // ⚠️ THE COMMITTER REACHES ITS OWN EXIT PATH INTERRUPTED. `drainLoop`'s
        // InterruptedException branch RE-ARMS the flag before returning the
        // cause, so the lease release below runs on a thread already carrying
        // it -- and `LocalSequencer.close` is `leases.release()`, which reaches
        // `BoundedLock.takeOrFail`, whose `tryLock(bound, NANOSECONDS)` throws
        // IMMEDIATELY when the flag is set. The release then fails, the
        // IOException is swallowed by the quiet catch, and the ERROR log still
        // announces that the lease is being released so another node can take
        // over. It is not: the pod holds a lease it cannot commit through until
        // the TTL, which is the exact outcome that block exists to prevent.
        // ⚠️ `close()` ALREADY HANDLES THIS HAZARD one screen below, by clearing
        // the flag for the duration. The exit path did not, and no fixture saw
        // it because every delegate's close was interrupt-INSENSITIVE -- the
        // same fixture weakness that hid the ordering defect one round ago.
        AtomicBoolean released = new AtomicBoolean();
        Sequencer delegate = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                Thread.currentThread().interrupt();
                throw new IOException("the store call was interrupted");
            }

            @Override
            public void close() throws IOException {
                // ⚠️ MODELS `BoundedLock.takeOrFail` under contention: it does
                // not merely observe the flag, it FAILS on it, which is what
                // `tryLock` does.
                if (Thread.currentThread().isInterrupted()) {
                    throw new IOException("interrupted before the lease could be released");
                }
                released.set(true);
            }
        };
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);

        assertThatThrownBy(() -> batching.commit(from("poda", 1)))
                .isInstanceOf(IOException.class);

        awaitSet(released, "the lease is released even though the death WAS an interrupt");
    }

    @Test
    void aQUEUEDCallerIsFailedWithoutWaitingForTheLeaseReleaseToFinish() throws Exception {
        // ⚠️ THE ORDER OF THE EXIT PATH IS THE SUBJECT. Failing the queue must
        // come BEFORE the lease release, which is the order `close` itself uses
        // and documents. Releasing first puts unbounded delegate I/O between the
        // door shutting and the callers being failed: `LeaseManager.release` can
        // block in `takeOrFail` and then issue a store PUT, while every queued
        // caller sits on `CompletableFuture.join`, which is UNINTERRUPTIBLE.
        // ⚠️ MEASURED with the release ordered first: a queued caller was still
        // parked three seconds in and completed only once the close was let go.
        var releaseMayFinish = new java.util.concurrent.CountDownLatch(1);
        AtomicBoolean released = new AtomicBoolean();
        var ref = new java.util.concurrent.atomic.AtomicReference<BatchingSequencer>();
        var entered = new java.util.concurrent.CountDownLatch(1);
        Sequencer delegate = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                // ⚠️ SIGNALLED BEFORE THE WAIT, so the test can hold B back
                // until the batch is closed AND drained. Starting both callers
                // together is a race: B can land in A's own batch, and then the
                // drain net -- the thing under test -- is never reached.
                entered.countDown();
                // ⚠️ DIES ONLY ONCE B IS REALLY QUEUED, and that is what makes
                // the arrangement deterministic rather than a race. The window
                // has already closed and drained, so a caller arriving now
                // CANNOT be in this batch -- it is behind it, which is the only
                // position the drain net covers. A timer that merely waited for
                // a queued caller would pull B INTO the batch on the very next
                // statement, and then both callers are failed by the catch and
                // the net is never exercised at all. Measured: that fixture
                // handed B the OutOfMemoryError, not the net's IOException.
                long giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (ref.get() == null || ref.get().queuedCount() < 1) {
                    if (System.nanoTime() > giveUpAt) {
                        throw new IllegalStateException("the second caller never queued");
                    }
                    Thread.onSpinWait();
                }
                throw new OutOfMemoryError("simulated: encoding a large batch");
            }

            @Override
            public void close() throws IOException {
                try {
                    // ⚠️ A SLOW RELEASE, not a hung one: the test releases it at
                    // the end, so a correct implementation and a broken one
                    // differ in WHEN the queued caller is failed, not whether.
                    releaseMayFinish.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
                released.set(true);
            }
        };

        // ⚠️ THE WINDOW HOLDS CALLER A AND LEAVES CALLER B QUEUED BEHIND IT,
        // which is the only arrangement that exercises the drain net at all --
        // the caller in flight is failed by the catch, not by the finally.
        // ⚠️ THE WINDOW CLOSES AT ONCE, so the batch is exactly caller A and the
        // drain has already happened before the delegate is entered.
        BatchingSequencer batching = new BatchingSequencer(delegate, IMMEDIATE);
        ref.set(batching);

        var queuedFailure = new java.util.concurrent.CompletableFuture<Throwable>();
        Thread a = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("poda", 1));
            } catch (Throwable ignored) {
                // the in-flight caller is failed by the catch, not by the net
            }
        });
        assertThat(entered.await(20, TimeUnit.SECONDS))
                .as("caller A is in flight and its window is drained").isTrue();
        Thread b = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("podb", 2));
                queuedFailure.complete(null);
            } catch (Throwable failed) {
                queuedFailure.complete(failed);
            }
        });

        Throwable failure = queuedFailure.get(10, TimeUnit.SECONDS);
        assertThat(released.get())
                .as("the queued caller was failed while the lease release was STILL running")
                .isFalse();
        assertThat(failure)
                .as("and it was failed, not left parked")
                .isInstanceOf(IOException.class);

        releaseMayFinish.countDown();
        a.join();
        b.join();
        awaitSet(released, "and the lease is released once it can be");
    }
}
