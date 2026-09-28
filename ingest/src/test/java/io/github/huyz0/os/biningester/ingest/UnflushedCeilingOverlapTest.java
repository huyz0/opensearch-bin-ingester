// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The unflushed-bytes ceiling (ADR-0079) under overlapping flushes, and both
 * sides of its guards (M12.3; M11 review F4, M11.7 P4 and T5, M11.24a T1).
 *
 * <p>T0: the ceiling is a class of its own since M11.24a, so the race is driven
 * by calling it in the order the flush completion can run, not by timing a
 * real flush.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class UnflushedCeilingOverlapTest {

    private static final long MAX = 100;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition room = lock.newCondition();
    private final UnflushedCeiling ceiling = new UnflushedCeiling(MAX, room);
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * ⚠️ THE RACE (M11 review F4): flush A's completion runs after flush B was
     * detached. Zeroing the in-flight bytes there dropped B's, and the pod
     * admitted about twice its ceiling.
     */
    @Test
    void aFinishingFlushReleasesOnlyItsOwnBytesNeverTheNextOnes() throws Exception {
        long a = locked(() -> ceiling.detached(60));
        long b = locked(() -> ceiling.detached(70));
        locked(() -> {
            ceiling.flushEnded(a);
            return null;
        });

        CompletableFuture<Void> append = appendWith(40, true);

        assertThat(waiting(append)).as("40 buffered + B's 70 in flight is past 100: it waits")
                .isTrue();
        locked(() -> {
            ceiling.flushEnded(b);
            return null;
        });
        append.get(10, TimeUnit.SECONDS);
    }

    @Test
    void anAppendAtTheCeilingExactlyWaitsAndOneBelowItProceeds() throws Exception {
        long flush = locked(() -> ceiling.detached(60));

        CompletableFuture<Void> atCeiling = appendWith(40, true);
        assertThat(waiting(atCeiling)).as("60 + 40 = the ceiling: it waits").isTrue();
        locked(() -> {
            ceiling.flushEnded(flush);
            return null;
        });
        atCeiling.get(10, TimeUnit.SECONDS);

        locked(() -> ceiling.detached(60));
        appendWith(39, true).get(10, TimeUnit.SECONDS);
    }

    /**
     * ⚠️ IT WAITS ONLY FOR A FLUSH THAT WILL COME (M11.7 P4): records a throwing
     * source left buffered with no waiter have nobody to flush them, so an
     * append over the ceiling with no flush coming proceeds rather than waiting
     * for ever.
     */
    @Test
    void anAppendOverTheCeilingProceedsWhenNoFlushWillCome() throws Exception {
        locked(() -> ceiling.detached(90));

        appendWith(90, false).get(10, TimeUnit.SECONDS);
    }

    @Test
    void closingWakesAnAppendWaitingForRoomAndRefusesIt() throws Exception {
        locked(() -> ceiling.detached(90));
        CompletableFuture<Void> append = appendWith(90, true);
        assertThat(waiting(append)).isTrue();

        closed.set(true);
        locked(() -> {
            ceiling.wakeAll();
            return null;
        });

        assertThat(append).failsWithin(10, TimeUnit.SECONDS)
                .withThrowableThat().havingRootCause().isInstanceOf(IOException.class)
                .withMessageContaining("closed");
    }

    /** Runs an append of {@code buffered} bytes on its own thread, under the lock. */
    private CompletableFuture<Void> appendWith(long buffered, boolean flushWillCome) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            lock.lock();
            try {
                ceiling.awaitRoom(() -> buffered, () -> flushWillCome, closed::get);
                done.complete(null);
            } catch (Throwable failed) {
                done.completeExceptionally(failed);
            } finally {
                lock.unlock();
            }
        });
        return done;
    }

    /** Whether {@code append} is parked on the room condition, not finished, within 5 s. */
    private boolean waiting(CompletableFuture<Void> append) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (append.isDone()) {
                return false;
            }
            lock.lock();
            try {
                if (lock.hasWaiters(room)) {
                    return true;
                }
            } finally {
                lock.unlock();
            }
            Thread.onSpinWait();
        }
        return false;
    }

    private <T> T locked(java.util.concurrent.Callable<T> action) throws Exception {
        lock.lock();
        try {
            return action.call();
        } finally {
            lock.unlock();
        }
    }
}
