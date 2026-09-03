// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock that can only be taken with a stated bound and a named purpose.
 *
 * <p>⚠️ AN IOException, never a quiet {@code false}. {@code tryAcquire} and
 * {@code renew} both have a "nothing happened" return value that means
 * something ELSE — empty from {@code renew} means FENCED, and a caller must
 * stop sequencing on it — so a lock timeout must not borrow that signal.
 * Failing to take a lock held across object-store I/O is evidence of slow
 * I/O, not of having lost the lease.
 *
 * <p>⚠️ THE BOUND IS THE CALLER'S, because the right one is not the same for
 * all three. For {@code tryAcquire} and {@code renew} it is
 * {@code renewInterval}: waiting longer than one renew interval guarantees
 * missing the renew anyway, and giving up early costs a retry. For
 * {@code release} that argument INVERTS — giving up early costs exactly the
 * thing release exists to buy — so its bound is the {@code ttl}, which is
 * what a successor pays when the release does not happen. Waiting up to
 * that is never worse than not waiting.
 *
 * <p>⚠️ Nanoseconds, so a sub-millisecond bound is not truncated to a
 * single non-blocking attempt while the message quotes the duration asked
 * for.
 */
final class BoundedLock {

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * @param what the operation waiting, named in the failure message
     * @param bound how long this particular caller may wait
     * @throws IOException the lock could not be taken within {@code bound}
     */
    void takeOrFail(String what, Duration bound) throws IOException {
        boolean taken;
        try {
            taken = lock.tryLock(bound.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for the lease lock to " + what, e);
        }
        if (!taken) {
            throw new IOException("could not take the lease lock to " + what
                    + " within " + bound + "; a store call is still in flight");
        }
    }

    void unlock() {
        lock.unlock();
    }
}
