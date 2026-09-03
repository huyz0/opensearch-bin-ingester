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
        // inside its `putIfAbsent` by the gate; B then runs. WITH the monitor B
        // cannot enter `tryAcquire` at all, so it blocks; A wins; B's later
        // pass sees the lease present-and-unexpired and returns early WITHOUT
        // calling `adopt`, so A's belief survives. WITHOUT the monitor B
        // interleaves: it stats (still absent -- A has not written yet), wins
        // its own `putIfAbsent`, sets `held`, and then A's write LOSES and
        // `adopt(empty)` clears `held` on the SAME instance, wiping the
        // winner's belief. One instance is the point: `synchronized` is
        // per-instance and `held` is instance state.
        //
        // ⚠️ The sync point is B BLOCKED-or-finished, never "B completed"
        // alone: under the correct build B blocks on the monitor and cannot
        // complete while A is gated, so awaiting completion would deadlock
        // exactly the implementation this test defends.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager shared = new LeaseManager(gate, "bins/cluster-a", "podA", "", TTL, RENEW, FIXED);

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
        while (t.getState() != Thread.State.BLOCKED
                && t.getState() != Thread.State.WAITING
                && t.getState() != Thread.State.TERMINATED) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("B never reached the monitor or finished: " + t.getState());
            }
            Thread.onSpinWait();
        }
    }
}
