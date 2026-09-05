// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A caller must never be left hanging (M4.7).
 *
 * <p>⚠️ SPLIT FROM {@link BatchingSequencerTest} because the two halves answer
 * different questions and the combined file passed the 500-line limit. That one
 * asks whether the commit rate stops scaling with pods; this one asks whether a
 * request can end up in neither the queue nor a batch, waiting on a future
 * nobody holds.
 *
 * <p>⚠️ EVERY TEST HERE EXISTS BECAUSE REVIEW MEASURED THE HANG IT PREVENTS.
 * {@code PendingCommit.await} is {@code CompletableFuture.join}, which is
 * UNINTERRUPTIBLE — a caller nobody completes is a producer thread hung until
 * SIGKILL, which is the lesson {@code DefaultIngest} already records in its own
 * words. Three separate paths reached that state during review: an unchecked
 * throw from the window timer, an {@code Error} from the delegate, and a request
 * enqueued around {@code close}.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BatchingSequencerLifecycleTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static CommitRequest from(String pod, long flushSeq) {
        return new CommitRequest(pod, flushSeq, "bins/" + pod + "/" + flushSeq + ".bseg",
                counts(new RunKey(A, 0), 3));
    }

    /** ⚠️ A `Sequencer` over a `CommitLog` with no lease, so the batcher is the subject. */
    private record DirectSequencer(CommitLog log) implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            return log.commitAll(requests);
        }

        @Override
        public void close() {
        }
    }

    /** ⚠️ Records that {@code close} reached the delegate — where a lease would be released. */
    private record ClosingSequencer(CommitLog log,
            java.util.concurrent.atomic.AtomicBoolean closed) implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            return log.commitAll(requests);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    /** A window that closes once {@code expected} requests are really in it. */
    private static final class QueuedTimer implements BatchingSequencer.WindowTimer {
        private final int expected;
        private volatile BatchingSequencer watching;

        QueuedTimer(int expected) {
            this.expected = expected;
        }

        void watch(BatchingSequencer sequencer) {
            this.watching = sequencer;
        }

        @Override
        public void awaitWindowClose() throws InterruptedException {
            long giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (watching == null || watching.queuedCount() < expected - 1) {
                if (System.nanoTime() > giveUpAt) {
                    throw new InterruptedException("the window never filled");
                }
                Thread.sleep(1);
            }
        }
    }

    @Test
    void aCommitAfterCloseFAILSRatherThanHanging() throws Exception {
        // ⚠️ The Sequencer contract says a commit after close must FAIL. A
        // batcher gets this wrong in a particular way: the caller is not
        // rejected, it is parked on a window that will never close.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        BatchingSequencer batching = new BatchingSequencer(new DirectSequencer(log),
                () -> { });
        batching.close();

        assertThatThrownBy(() -> batching.commit(from("pod0", 0)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void aCommitterKilledByItsTimerFAILSCallersRatherThanParkingThem() throws Exception {
        // ⚠️ MEASURED, AND THE WORST FAILURE MODE THIS CLASS HAS. `WindowTimer`
        // is a public seam, so an unchecked throw out of it is an
        // intended-shape input; it used to terminate the committer while the
        // batcher kept accepting, and `PendingCommit.await` is
        // `CompletableFuture.join` — UNINTERRUPTIBLE. The caller did not return
        // after five seconds and could not be interrupted. A hang is not a
        // failure a caller can handle.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        try (var batching = new BatchingSequencer(new DirectSequencer(log), () -> {
            throw new IllegalStateException("the timer is broken");
        })) {
            // ⚠️ THE CAUSE, UNWRAPPED. Asserting only RuntimeException is
            // satisfied by the raw CompletionException a bare `join()` throws —
            // deleting the whole unwrap left the suite green, and callers would
            // then get an unchecked exception where `Sequencer` declares
            // `throws IOException` and `DefaultIngest` catches it.
            assertThatThrownBy(() -> batching.commit(from("pod0", 0)))
                    .as("the caller held when the committer died must be failed, not parked")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("the timer is broken");

            assertThatThrownBy(() -> batching.commit(from("pod0", 1)))
                    .as("and the door is shut, so a later caller fails fast instead of hanging")
                    .isInstanceOf(IOException.class)
                    // ⚠️ "terminated", NOT "closed", AND THAT IS A DELIBERATE
                    // STRENGTHENING rather than a relaxation. Nobody called
                    // `close` here — the timer threw — so the old message was
                    // simply false, and review measured an operator being
                    // pointed at a shutdown that never happened. The cause is
                    // now pinned too, which the previous assertion did not do at
                    // all: `hasMessageContaining("closed")` held even though the
                    // reason the batcher died was discarded.
                    .hasMessageContaining("terminated")
                    .hasRootCauseMessage("the timer is broken");
        }
    }

    @Test
    void aCallerQUEUEDBehindADyingCommitterIsFailedEvenWithNoCloseAtAll() throws Exception {
        // ⚠️ THE EXIT `close` NEVER REACHES: a live pod whose committer dies on
        // its own — a timer throw, an Error — with nobody calling close. The
        // caller HOLDING the window is failed by the catch; the ones QUEUED
        // BEHIND it are failed only by the net in the drain loop's finally.
        // ⚠️ Measured: dropping that net alone leaves the whole suite green,
        // because every other lifecycle test has a single caller. The queued
        // thread was still parked after an eight-second join.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        var outcomes = new CopyOnWriteArrayList<Throwable>();
        var holder = new java.util.concurrent.atomic.AtomicReference<BatchingSequencer>();
        var taken = new java.util.concurrent.CountDownLatch(1);

        BatchingSequencer batching = new BatchingSequencer(new DirectSequencer(log), () -> {
            // The first request is already taken; wait for a second to QUEUE
            // behind it, then die the way a broken timer dies.
            taken.countDown();
            long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (holder.get() == null || holder.get().queuedCount() < 1) {
                if (System.nanoTime() > giveUp) {
                    break;
                }
                Thread.sleep(1);
            }
            throw new IllegalStateException("the timer is broken");
        });
        holder.set(batching);
        Thread inWindow = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod0", 0));
            } catch (Throwable t) {
                outcomes.add(t);
            }
        });
        assertThat(taken.await(10, TimeUnit.SECONDS)).isTrue();
        Thread queued = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod1", 0));
            } catch (Throwable t) {
                outcomes.add(t);
            }
        });

        inWindow.join(java.time.Duration.ofSeconds(10));
        queued.join(java.time.Duration.ofSeconds(10));

        assertThat(queued.isAlive())
                .as("no close was ever called, so only the drain loop's own net can free this one")
                .isFalse();
        assertThat(outcomes).as("both are told").hasSize(2);
    }

    @Test
    void anERRORFromTheDelegateFailsCallersRatherThanKillingTheCommitter() throws Exception {
        // ⚠️ THE BLOCKING DEFECT, and it is not a theoretical Error. Batching
        // exists to make batches BIG, and `CommitLog` encodes the delta TWICE
        // per attempt — once for the length, once for the body — so the
        // OutOfMemoryError window is the batched path itself. A `LinkageError`
        // from a lazily-loaded store SDK class reaches the same line.
        // ⚠️ Measured on the draft that guarded only the timer: two callers
        // still alive five seconds after an OOM, neither interruptible, because
        // `commitBatch` narrowed to `IOException | RuntimeException` and the
        // Error killed the committer with the door still open.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        Sequencer exploding = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                throw new OutOfMemoryError("delta encode");
            }

            @Override
            public void close() {
            }
        };

        try (var batching = new BatchingSequencer(exploding, () -> { })) {
            assertThatThrownBy(() -> batching.commit(from("pod0", 0)))
                    .as("the caller in the failing window is told, not parked")
                    .isInstanceOf(OutOfMemoryError.class);

            assertThatThrownBy(() -> batching.commit(from("pod0", 1)))
                    .as("and the door is shut, so the next caller fails fast")
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aFailedWindowFailsEVERYCallerInIt() throws Exception {
        // ⚠️ The batch is ONE conditional PUT, so there is no outcome where some
        // submissions landed and others did not. Reporting success to any caller
        // would be inventing a durability that does not exist.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        QueuedTimer timer = new QueuedTimer(2);
        var failures = new CopyOnWriteArrayList<Throwable>();
        // Two submissions naming the SAME segment: the delegate refuses the batch.
        CommitRequest one = from("pod0", 0);
        CommitRequest clash = new CommitRequest("pod1", 0, one.segmentKey(),
                counts(new RunKey(A, 1), 2));
        var threads = new java.util.ArrayList<Thread>();

        try (var batching = new BatchingSequencer(new DirectSequencer(log), timer)) {
            timer.watch(batching);
            for (CommitRequest r : List.of(one, clash)) {
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                            batching.commit(r);
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
        }

        assertThat(failures).as("both callers, not just the offender").hasSize(2);
    }

    @Test
    void closingWithACallerParkedFailsItAndClosesTheDelegate() throws Exception {
        // ⚠️ THE THREE THINGS `close` PROMISES, none of which had a test: the
        // parked caller is failed rather than left waiting, the committer is
        // stopped, and the DELEGATE is closed — a `LocalSequencer` delegate that
        // is not closed holds its lease to the TTL, which is the ~10 s failover
        // the contract says close exists to avoid.
        // ⚠️ WAITS FOR THE WINDOW TO BE ENTERED, not for the queue to be
        // non-empty: the committer TAKES the request the moment it arrives, so
        // queue depth is back to zero while the caller is parked. A fixture
        // watching the depth here hangs to its own timeout.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        var closed = new java.util.concurrent.atomic.AtomicBoolean();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var neverCloses = new java.util.concurrent.CountDownLatch(1);
        var failure = new CopyOnWriteArrayList<Throwable>();

        BatchingSequencer batching = new BatchingSequencer(
                new ClosingSequencer(log, closed), () -> {
                    entered.countDown();
                    neverCloses.await();
                });
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod0", 0));
            } catch (Throwable t) {
                failure.add(t);
            }
        });
        assertThat(entered.await(10, TimeUnit.SECONDS))
                .as("the committer must have taken the request and opened a window").isTrue();
        batching.close();
        caller.join(java.time.Duration.ofSeconds(10));

        assertThat(caller.isAlive()).as("the parked caller must not still be waiting").isFalse();
        assertThat(failure).as("it is failed, with a reason").hasSize(1);
        assertThat(closed.get()).as("and the delegate is closed, so a lease is released").isTrue();
    }

    // ⚠️ THE ENQUEUE/CLOSE ATOMICITY IS NOT PINNED BY A TEST, and that is
    // recorded rather than hidden. `submit` takes the lifecycle monitor around
    // the closed-check AND the enqueue, so a caller cannot observe the batcher
    // open and then land after `close` has drained. Pinning that from outside
    // is the problem: a seam parked BEFORE the check reproduces nothing (a
    // draft did exactly that, and reverting the lock left the whole suite
    // green), while a seam BETWEEN the check and the enqueue makes `close`
    // block on the same monitor — so the fixed code deadlocks the fixture and
    // the broken code races it, and no single ordering distinguishes them.
    // ⚠️ WHAT WAS DONE INSTEAD: the seam was DELETED from production rather
    // than left as a member existing for a test that does not work, and review
    // probed the property directly — 300 rounds of 16 callers racing one
    // `close`, zero orphans, every caller terminated with an outcome. A stress
    // test of the same shape was also deleted: it caught the unfixed defect in
    // 2 runs of 8, and a detector that misses more often than it fires cannot
    // be red-recorded and constrains nothing a reader can rely on.


    @Test
    void closingFailsTheQUEUEDCallersTooNotOnlyTheOneInFlight() throws Exception {
        // ⚠️ EVERY LIFECYCLE TEST HAD AT MOST ONE CALLER, so the QUEUED ones
        // were never exercised — and on a real close mid-window the committer
        // holds only the FIRST request while the rest sit in the queue.
        // Measured: deleting both `failAll(drainRemaining(), ...)` nets left the
        // suite green, and a queued caller was still alive after a ten-second
        // join. That is the hang this class exists to prevent, reached by the
        // one door no test opened.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var neverCloses = new java.util.concurrent.CountDownLatch(1);
        var outcomes = new CopyOnWriteArrayList<Throwable>();

        BatchingSequencer batching = new BatchingSequencer(new DirectSequencer(log), () -> {
            entered.countDown();
            neverCloses.await();
        });
        Thread inFlight = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod0", 0));
            } catch (Throwable t) {
                outcomes.add(t);
            }
        });
        assertThat(entered.await(10, TimeUnit.SECONDS))
                .as("the committer holds the first request and is inside the window").isTrue();
        Thread queued = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod1", 0));
            } catch (Throwable t) {
                outcomes.add(t);
            }
        });
        while (batching.queuedCount() < 1) {
            Thread.sleep(1);
        }

        batching.close();
        inFlight.join(java.time.Duration.ofSeconds(10));
        queued.join(java.time.Duration.ofSeconds(10));

        assertThat(queued.isAlive())
                .as("the caller waiting BEHIND the window must be failed, not left parked")
                .isFalse();
        assertThat(inFlight.isAlive()).isFalse();
        assertThat(outcomes).as("both are told, with a reason").hasSize(2);
    }

    @Test
    void aFailedWindowDoesNotKillTheCommitterForEveryWindowAfterIt() throws Exception {
        // ⚠️ NOTHING ASSERTED THAT THE BATCHER SURVIVES A FAILURE. Measured:
        // narrowing `commitBatch` to `catch (IOException failed)` lets a
        // RuntimeException escape to the drain loop's `catch (Throwable)`, which
        // fails the batch and RETURNS — so one duplicate-segment-key batch, the
        // only in-protocol poison there is, kills the pod's committer
        // permanently and every later commit hangs. Both suites stayed green.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        var failNext = new java.util.concurrent.atomic.AtomicBoolean(true);
        Sequencer flaky = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                if (failNext.getAndSet(false)) {
                    throw new IllegalArgumentException("one poisoned window");
                }
                return log.commitAll(requests);
            }

            @Override
            public void close() {
            }
        };

        try (var batching = new BatchingSequencer(flaky, () -> { })) {
            assertThatThrownBy(() -> batching.commit(from("pod0", 0)))
                    .isInstanceOf(IllegalArgumentException.class);

            CommitDelta after = batching.commit(from("pod0", 1));

            assertThat(after.segments()).as("the NEXT window is served normally").hasSize(1);
        }
    }

    @Test
    void closeJOINSTheCommitterBeforeClosingTheDelegate() throws Exception {
        // ⚠️ SIGNALLING IS NOT JOINING, and deleting the join left the suite
        // green. `delegate.close()` releases the lease; running it while the
        // committer is still inside `CommitLog`'s redrive loop lets a retry win
        // a slot AFTER this node gave up its term and a successor sealed the
        // chain — the fork `LocalSequencer`'s javadoc says must not exist,
        // reassigning offsets another leader has issued. I2.
        // ⚠️ THE DELEGATE'S WAIT IGNORES INTERRUPTS deliberately: `close`
        // interrupts the committer, and `CommitLog.commitSubmissions`'s
        // `while (true)` redrive has no interrupt check, so a real in-flight
        // PUT does not stop on request either. A delegate that gave up on
        // interrupt would make the join look unnecessary.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var inCommit = new java.util.concurrent.CountDownLatch(1);
        var committing = new java.util.concurrent.atomic.AtomicBoolean();
        var closedMidCommit = new java.util.concurrent.atomic.AtomicBoolean();

        Sequencer blocking = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                committing.set(true);
                inCommit.countDown();
                // ⚠️ BLOCKS UNTIL INTERRUPTED, then keeps working for a while.
                // `close` is what interrupts, so the wake-up is deterministic
                // rather than a race with a latch the test releases — an
                // earlier fixture released from the test thread and the commit
                // sometimes finished BEFORE close ran, which made the missing
                // join look present. The delay after the interrupt models what
                // `CommitLog.commitSubmissions` really does: its `while (true)`
                // redrive has no interrupt check, so an in-flight PUT does not
                // stop on request. Without the join, `delegate.close()` lands
                // inside this window every time.
                try {
                    release.await();
                } catch (InterruptedException wokenByClose) {
                    Thread.currentThread().interrupt();
                }
                long busyUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
                while (System.nanoTime() < busyUntil) {
                    Thread.onSpinWait();
                }
                committing.set(false);
                return log.commitAll(requests);
            }

            @Override
            public void close() {
                closedMidCommit.set(committing.get());
            }
        };

        BatchingSequencer batching = new BatchingSequencer(blocking, () -> { });
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                batching.commit(from("pod0", 0));
            } catch (Exception ignored) {
                // the outcome is not the subject here
            }
        });
        assertThat(inCommit.await(10, TimeUnit.SECONDS)).isTrue();
        // ⚠️ THE CLOSING THREAD ALREADY CARRIES AN INTERRUPT, which is the
        // shape that skipped the join entirely: `join` throws at once when the
        // flag is set, so `close` waited for nothing and released the lease
        // under an in-flight commit. Measured `closeTookMs=16` and
        // `closedMidCommit=true` against 306ms and false. One line, and it is
        // the difference between the fix being present and being decorative.
        Thread.currentThread().interrupt();
        batching.close();
        assertThat(Thread.interrupted())
                .as("and the caller's own interrupt is handed back, not swallowed")
                .isTrue();
        caller.join(java.time.Duration.ofSeconds(15));

        assertThat(closedMidCommit.get())
                .as("the delegate -- and its lease -- must not be closed under an in-flight commit")
                .isFalse();
    }
}
