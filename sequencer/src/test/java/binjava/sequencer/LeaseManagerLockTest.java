// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ⚠️ THE LEASE LOCK IS HELD ACROSS OBJECT-STORE I/O, which java-style.md rule 6
 * says makes it a {@code ReentrantLock} acquired with a timeout rather than a
 * monitor (M4.3f). A monitor cannot be given up: on a real backend one hung PUT
 * parks every other caller — {@code tryAcquire}, {@code renew}, {@code release}
 * and {@code held()} — with no timeout and no interruptibility. There is one
 * test here per victim, because a bound that is merely present is not a bound:
 * {@code Duration.ofDays(1)} is a timeout too.
 *
 * <p>⚠️ Each bound is asserted through the duration NAMED IN THE MESSAGE, not
 * through elapsed time. That makes it a string comparison rather than a race
 * between two timeouts, so raising a bound fails loudly instead of slowly.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LeaseManagerLockTest {

    private static final Clock FIXED =
            Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC);
    private static final Duration TTL = Duration.ofMillis(200);
    private static final Duration RENEW = Duration.ofMillis(50);

    private final AtomicReference<Throwable> helperFailed = new AtomicReference<>();

    private static LeaseManager manager(binjava.binstore.BinStore store) {
        return new LeaseManager(store, new LeaseConfig("bins/cluster-a", "podA", "",
                TTL, RENEW), FIXED);
    }

    /**
     * ⚠️ Every helper's uncaught exception is CAPTURED, never left to the
     * default handler, which prints and is asserted on by nothing. None of
     * these tests can currently go green on a dead helper, but that is a
     * property of today's assertions rather than of the fixture — the same
     * latent hole M4.3c closed in its own concurrency test.
     */
    private Thread helper(String name, ThrowingRunnable body) {
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, name);
        t.setUncaughtExceptionHandler((thread, e) -> helperFailed.compareAndSet(null, e));
        return t;
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private void assertNoHelperFailed() {
        assertThat(helperFailed.get()).as("no helper thread died unnoticed").isNull();
    }

    @Test
    void readingTheBeliefDoesNotQueueBehindAWriteInFlight() throws Exception {
        // ⚠️ Thread A is parked INSIDE its conditional write, holding the lock
        // that guards the read-decide-CAS cycle. If `held()` took that lock, a
        // hung PUT would make every caller that merely asks "am I the leader?"
        // hang with it -- and `held()` is what a caller gates on.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager m = manager(gate);

        Thread writer = helper("writer", m::tryAcquire);
        writer.start();
        try {
            gate.awaitEntered();
            CountDownLatch read = new CountDownLatch(1);
            Thread reader = helper("reader", () -> {
                m.held();
                read.countDown();
            });
            reader.start();
            assertThat(read.await(5, TimeUnit.SECONDS))
                    .as("held() must answer while a write is still in flight")
                    .isTrue();
            reader.join();
        } finally {
            gate.release();
        }
        writer.join();
        assertNoHelperFailed();
    }

    @Test
    void acquireGivesUpOnTheLockAfterOneRenewIntervalRatherThanWaitingOut() throws Exception {
        // ⚠️ THE METHOD THE TASK NAMES FIRST, and the one that held the monitor
        // before this change. A bound that merely EXISTS is not a bound:
        // `Duration.ofDays(1)` is the monitor with extra words, so the
        // assertion is on the duration itself.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager m = manager(gate);

        Thread writer = helper("writer", m::tryAcquire);
        writer.start();
        try {
            gate.awaitEntered();
            assertThatThrownBy(m::tryAcquire)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("acquire")
                    .hasMessageContaining("PT0.05S")
                    .as("one renew interval, not the ttl and not forever")
                    .hasMessageNotContaining("PT0.2S");
        } finally {
            gate.release();
        }
        writer.join();
        assertNoHelperFailed();
    }

    @Test
    void renewThatCannotTakeTheLockReportsIoFailureAndKeepsTheBelief() throws Exception {
        // ⚠️ THE DANGEROUS SHORTHAND. An empty `renew` means FENCED and the
        // caller must stop sequencing at once, so a lock timeout must never
        // borrow that signal: failing to take a lock is not evidence that
        // someone else took the lease.
        // ⚠️ The instance must HOLD something first, or "belief untouched" and
        // "belief cleared" are the same observation and only the return
        // channel is constrained. Gating `putIfMatch` leaves the acquire clean,
        // so there is a real term to preserve.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate =
                new GateFirstPutStore(backing, GateFirstPutStore.Target.PUT_IF_MATCH);
        LeaseManager m = manager(gate);
        assertThat(m.tryAcquire()).isPresent();

        Thread renewer = helper("renewer", m::renew);
        renewer.start();
        try {
            gate.awaitEntered();
            assertThatThrownBy(m::renew)
                    .as("not an empty Optional -- that would self-fence a healthy holder")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("renew")
                    .hasMessageContaining("PT0.05S");
            assertThat(m.held())
                    .as("and the term survives, because NOTHING WAS WRITTEN -- unlike "
                            + "an IOException out of the conditional write, which leaves "
                            + "the belief ambiguous rather than intact")
                    .isPresent();
        } finally {
            gate.release();
        }
        renewer.join();
        assertNoHelperFailed();
    }

    @Test
    void releasingWhenNothingIsHeldDoesNotWaitForTheLockAtAll() throws Exception {
        // ⚠️ `release`'s javadoc calls this a no-op, and a SIGTERM on a
        // non-holder is entirely normal. Behind the lock it stops being one: a
        // pod that never held the lease, whose election thread is inside a slow
        // `tryAcquire`, blocks its whole shutdown path and then throws about a
        // lock it had no reason to want.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate = new GateFirstPutStore(backing);
        LeaseManager m = manager(gate);

        Thread elector = helper("elector", m::tryAcquire);
        elector.start();
        try {
            gate.awaitEntered();
            m.release();
        } finally {
            gate.release();
        }
        elector.join();
        assertNoHelperFailed();
    }

    @Test
    void releaseWaitsForTheLockUpToTheTtlRatherThanTheRenewInterval() throws Exception {
        // ⚠️ The renew-interval argument INVERTS for `release`, where giving up
        // early costs exactly the thing release exists to buy: the successor
        // then waits out the full TTL. Waiting up to the TTL for the lock is
        // never worse than not waiting, because the TTL is what not-waiting
        // costs.
        MemoryBinStore backing = new MemoryBinStore();
        GateFirstPutStore gate =
                new GateFirstPutStore(backing, GateFirstPutStore.Target.PUT_IF_MATCH);
        LeaseManager m = manager(gate);
        assertThat(m.tryAcquire()).as("gated on putIfMatch, so the acquire runs clean").isPresent();

        Thread renewer = helper("renewer", m::renew);
        renewer.start();
        try {
            gate.awaitEntered();
            assertThatThrownBy(m::release)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("PT0.2S")
                    .as("the TTL, not the 50ms renew interval")
                    .hasMessageNotContaining("PT0.05S");
        } finally {
            gate.release();
        }
        renewer.join();
        assertNoHelperFailed();
    }
}
