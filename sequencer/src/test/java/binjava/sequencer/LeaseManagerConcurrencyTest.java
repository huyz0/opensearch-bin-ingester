// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Lease;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ⚠️ SEPARATE FROM {@code LeaseManagerTest} because that file reached the
 * 500-line limit, and because these tests need a second thread and a gated
 * store rather than the advance-the-clock shape every test there uses. Same
 * class under test.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LeaseManagerConcurrencyTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);

    /** ⚠️ Fixed: nothing here expires, so nothing needs to advance. */
    private static final Clock FIXED =
            Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC);

    @Test
    void aLosingConcurrentAcquireCannotWipeTheWinnersBeliefOnTheSameInstance() throws Exception {
        // ⚠️ M4.3c: this is what `synchronized` on `tryAcquire` buys, and
        // round-3 test review measured that removing it left the whole suite
        // green -- the property was asserted by a javadoc line and by nothing
        // executable.
        //
        // ⚠️ The interleave is DETERMINISTIC, not raced. Thread A is parked
        // inside its `putIfAbsent` by the gate; B then runs. WITH the lock B
        // cannot enter the cycle at all, so it parks; A wins; B's later
        // pass sees the lease present-and-unexpired and returns early WITHOUT
        // calling `adopt`, so A's belief survives. WITHOUT the monitor B
        // interleaves: it stats (still absent -- A has not written yet), wins
        // its own `putIfAbsent`, sets `held`, and then A's write LOSES and
        // `adopt(empty)` clears `held` on the SAME instance, wiping the
        // winner's belief. One instance is the point: the lock is
        // per-instance and the belief is instance state.
        //
        // ⚠️ The sync point is B PARKED-or-finished, never "B completed"
        // alone: under the correct build B parks on the lock and cannot
        // complete while A is gated, so awaiting completion would deadlock
        // exactly the implementation this test defends.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager shared = new LeaseManager(gate, new LeaseConfig("bins/cluster-a", "podA", "",
                TTL, RENEW), FIXED);

        // ⚠️ CAPTURED, not thrown into the void. `tryAcquire` wraps in
        // `UncheckedIOException`, which would otherwise reach the default
        // uncaught-exception handler and be asserted on by NOTHING -- and
        // `awaitParkedOrDone` cannot tell "B finished contending" from "B died
        // before contending", so a later change that makes the second
        // contender throw early would silently turn this into a
        // single-threaded test that still claims to pin the monitor.
        AtomicReference<Optional<Lease>> aGot = new AtomicReference<>();
        AtomicReference<Optional<Lease>> bGot = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        Thread a = thread("acquire-A", shared, aGot, failed);
        Thread b = thread("acquire-B", shared, bGot, failed);

        a.start();
        try {
            gate.awaitEntered();
            b.start();
            awaitParkedOrDone(b);
        } finally {
            // ⚠️ `finally`: if the spin above times out, A is still parked on
            // the gate, and leaving it there outlives the assertion failure.
            gate.release();
            a.join();
            b.join();
        }

        assertThat(failed.get()).as("neither contender threw").isNull();
        assertThat(bGot.get())
                .as("B contended and LOST -- it did not skip contending, and it "
                        + "did not report holding the term A took")
                .isEmpty();

        assertThat(shared.held())
                .as("one term was taken, and the instance still believes it holds it")
                .isPresent();
        assertThat(Lease.decode(backing.get(shared.key()).readAllBytes()).epoch())
                .as("and the store agrees -- one first term, not two")
                .isEqualTo(1);
    }

    @Test
    void aReleaseDuringAnInFlightAcquireWAITSForItRatherThanNoOpingInstantly() throws Exception {
        // ⚠️ M4.3k. `belief == null` means "I held nothing at the INSTANT of
        // this read", not "there is nothing to release" -- and an acquiring
        // thread holds the lock across stat+get+PUT, writing `belief` only at
        // the END. So the window in which the OLD fast path was wrong was the
        // ENTIRE duration of an in-flight acquire: rolling restart, election
        // thread polling `tryAcquire`; SIGTERM lands while the acquire that
        // WINS is in flight; `release` reads null, returns instantly, the pod
        // exits, and the lease it won a moment later is orphaned with nobody
        // renewing it -- the successor waits the full TTL.
        //
        // ⚠️ THE INTERLEAVE, deterministic exactly as the test above builds
        // one: A parks inside its `putIfAbsent`, HOLDING THE LOCK (M4.3f).
        // `release` on the SAME instance from a second thread must therefore
        // park too -- observably, on the lock -- rather than reading `belief`
        // (still null) and returning at once.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager shared = new LeaseManager(gate, new LeaseConfig("bins/cluster-a", "podA", "",
                TTL, RENEW), FIXED);

        AtomicReference<Optional<Lease>> acquired = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        Thread acquirer = thread("acquire-A", shared, acquired, failed);
        AtomicReference<Throwable> releaseFailed = new AtomicReference<>();
        Thread releaser = releaseThread(shared, releaseFailed);

        acquirer.start();
        try {
            gate.awaitEntered();
            releaser.start();
            // ⚠️ THE ASSERTION THAT MATTERS. Under the OLD fast path this
            // spin times out with `releaser` already TERMINATED -- it read
            // `belief == null` and returned before ever reaching the lock.
            awaitParkedOrDone(releaser);
            assertThat(releaser.getState())
                    .as("release() must be WAITING for the lock the in-flight "
                            + "acquire holds, not finished already -- a "
                            + "terminated releaser here means it no-opped "
                            + "instantly on a belief that had not been "
                            + "written yet")
                    .isNotEqualTo(Thread.State.TERMINATED);
        } finally {
            gate.release();
            acquirer.join();
            releaser.join();
        }

        assertThat(failed.get()).as("the acquirer did not throw").isNull();
        assertThat(releaseFailed.get()).as("the releaser did not throw").isNull();
        assertThat(acquired.get()).as("A won the lease").isPresent();
        // ⚠️ AND RELEASED, not merely unparked. `release` waiting for the
        // lock and then finding nothing to do would pass the assertion above
        // for the wrong reason -- the store must show the lease actually
        // given up.
        assertThat(Lease.decode(backing.get(shared.key()).readAllBytes())
                .isExpiredAt(FIXED.millis()))
                .as("the lease A won a moment ago is released, not orphaned")
                .isTrue();
        assertThat(shared.held())
                .as("and the instance no longer believes it holds anything")
                .isEmpty();
    }

    @Test
    void anUncontendedReleaseDoesNotWedgeTheLockForALaterThread() throws Exception {
        // ⚠️ THE HALF THE INTERLEAVE TEST ABOVE CANNOT SEE, found by review
        // rather than by the task's own text. `release()`'s new
        // `if (!lock.tryLock()) { lock.takeOrFail(...); }` pairs exactly one
        // lock attempt with the existing `finally { lock.unlock(); }` -- but
        // a mutation that ALSO calls `takeOrFail` after an already-successful
        // `tryLock()` (a negated condition, for instance) would silently
        // double-lock on the UNCONTENDED path. `ReentrantLock` is reentrant,
        // so the SAME thread's second acquisition just succeeds -- no
        // exception, nothing to catch on a single thread -- and the single
        // `unlock()` in `finally` leaves the hold count at ONE rather than
        // ZERO. Every test above stays on one or two threads that each touch
        // the lock once; only a THIRD, later thread on the SAME instance can
        // tell a hold count of one from zero.
        MemoryBinStore backing = new MemoryBinStore();
        LeaseManager m = new LeaseManager(backing, new LeaseConfig("bins/cluster-a", "podA", "",
                TTL, RENEW), FIXED);

        AtomicReference<Throwable> releaseFailed = new AtomicReference<>();
        Thread releaser = releaseThread(m, releaseFailed);
        releaser.start();
        releaser.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(releaseFailed.get()).as("the uncontended release did not throw").isNull();

        // ⚠️ THE PROBE. A DIFFERENT thread now needs the SAME instance's
        // lock. Wedged, this thread parks until the class's own
        // SEPARATE_THREAD @Timeout kills the whole test; free, it returns at
        // once with the lease won.
        AtomicReference<Optional<Lease>> acquired = new AtomicReference<>();
        AtomicReference<Throwable> acquireFailed = new AtomicReference<>();
        Thread prober = thread("prober", m, acquired, acquireFailed);
        prober.start();
        prober.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(acquireFailed.get()).as("the probing thread did not throw").isNull();
        assertThat(acquired.get())
                .as("a fresh thread could take the lock and win the lease -- the "
                        + "uncontended release did not leave it wedged")
                .isPresent();
    }

    private static Thread releaseThread(LeaseManager m, AtomicReference<Throwable> failed) {
        Thread t = new Thread(() -> {
            try {
                m.release();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, "release");
        t.setUncaughtExceptionHandler((thread, e) -> failed.compareAndSet(null, e));
        return t;
    }

    private static Thread thread(String name, LeaseManager m,
            AtomicReference<Optional<Lease>> got, AtomicReference<Throwable> failed) {
        Thread t = new Thread(() -> {
            try {
                got.set(m.tryAcquire());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, name);
        t.setUncaughtExceptionHandler((thread, e) -> failed.compareAndSet(null, e));
        return t;
    }

    /** ⚠️ Bounded spin, never {@code Thread.sleep} -- testing.md leaves no escape. */
    private static void awaitParkedOrDone(Thread t) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        // ⚠️ TIMED_WAITING is here because M4.3f made the lease lock a
        // `ReentrantLock` acquired with a timeout: a contender parks in
        // `tryLock` rather than blocking on a monitor. Leaving it out did not
        // make the test stricter, it made it WRONG -- the spin never saw B
        // park, so the gate was released only after B had already timed out
        // and thrown, and the test failed on a contender that behaved exactly
        // as designed.
        while (t.getState() != Thread.State.BLOCKED
                && t.getState() != Thread.State.WAITING
                && t.getState() != Thread.State.TIMED_WAITING
                && t.getState() != Thread.State.TERMINATED) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("B never reached the lock or finished: " + t.getState());
            }
            Thread.onSpinWait();
        }
    }
}
