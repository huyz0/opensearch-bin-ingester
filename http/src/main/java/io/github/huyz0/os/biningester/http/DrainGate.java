// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * What the front door admits while this node is going away (M8.7, NFR-9,
 * research 08 §7).
 *
 * <p>⚠️ **THREE SWITCHES, THROWN IN THE ORDER §7 GIVES, AND NEVER UN-THROWN.**
 * Readiness fails first, so the load balancer stops sending new work. Then
 * subscribers are told to reconnect: a poll that is waiting is woken and
 * answered 503, and a new one is refused with a 503. Then bulk admission
 * closes, and the node waits for the requests already inside. A node that
 * is draining never becomes ready again. The next process is the one that
 * will be.
 *
 * <p>⚠️ **A WAITING POLL IS WOKEN, NOT LEFT TO TIME OUT.** §7 step 2 says
 * why: a pod that simply closes makes every subscriber wait out its timeout
 * and then reconnect SIMULTANEOUSLY. The 503 reaches the consumer's
 * transport as a failed poll, and it retries with the jittered backoff it
 * already has, through the cluster address, to a pod that is still ready.
 * A 503 is a status this route already answers (the session cap), so no
 * consumer needs upgrading to understand it.
 *
 * <p>⚠️ **IT HOLDS NO CLOCK.** A bound is given as a {@link Duration}, and
 * {@link Condition#awaitNanos} counts it down, so no time is read here.
 */
public final class DrainGate {

    private final java.util.function.Consumer<String> journal;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Set<Thread> pollers = new HashSet<>();
    private volatile boolean ready = true;
    private boolean pollsRefused;
    private boolean bulkRefused;
    private int bulkInFlight;

    /** A gate that tells nobody when it is thrown. */
    public DrainGate() {
        this(event -> { });
    }

    /**
     * A gate that tells {@code journal} each time a switch is thrown.
     *
     * <p>⚠️ **THE JOURNAL IS WRITTEN AT THE SWITCH, NOT BY WHOEVER THROWS
     * IT**, so the order it records is the order the switches really moved.
     * A shutdown that forgot a step, or ran it late, shows up there even
     * though the sequence's own report would still list every step in
     * order.
     */
    public DrainGate(java.util.function.Consumer<String> journal) {
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
    }

    /** Whether the readiness probe should pass. */
    public boolean ready() {
        return ready;
    }

    /** §7 step 1: the load balancer stops sending new requests here. */
    public void failReadiness() {
        ready = false;
        journal.accept(READINESS_FAILED);
    }

    public static final String READINESS_FAILED = "readiness failed";
    public static final String POLLS_RELEASED = "polls released";
    public static final String BULK_REFUSED = "bulk refused";

    /**
     * Admits a poll, or refuses it because subscribers have been told to go.
     *
     * <p>⚠️ **THE CALLER'S THREAD IS REGISTERED**, so {@link #releasePollers}
     * can wake it. Every {@code true} must be paired with {@link #exitPoll}.
     */
    public boolean enterPoll() {
        lock.lock();
        try {
            if (pollsRefused) {
                return false;
            }
            pollers.add(Thread.currentThread());
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ends a poll that {@link #enterPoll} admitted.
     *
     * <p>⚠️ **AN INTERRUPT THIS GATE SENT IS CLEARED HERE.** It was meant for
     * the wait, and a handler thread that went back to the server still
     * interrupted would fail its next blocking call for a reason nobody gave.
     * Only this gate interrupts a registered thread, and only under the lock,
     * so once the thread is deregistered no interrupt of ours can still be in
     * flight.
     */
    public void exitPoll() {
        lock.lock();
        try {
            pollers.remove(Thread.currentThread());
            if (pollsRefused) {
                Thread.interrupted();
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Whether subscribers have been told to reconnect elsewhere. */
    public boolean pollsRefused() {
        lock.lock();
        try {
            return pollsRefused;
        } finally {
            lock.unlock();
        }
    }

    /** §7 step 2: every waiting poll is woken, and no new one is admitted. */
    public void releasePollers() {
        lock.lock();
        try {
            pollsRefused = true;
            for (Thread poller : pollers) {
                poller.interrupt();
            }
            journal.accept(POLLS_RELEASED);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until every admitted poll has answered, or the bound passes.
     *
     * @return how many polls were still open when this returned
     */
    public int awaitNoPollers(Duration bound) throws InterruptedException {
        lock.lock();
        try {
            long left = bound.toNanos();
            while (!pollers.isEmpty() && left > 0) {
                left = changed.awaitNanos(left);
            }
            return pollers.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Admits a bulk request, or refuses it because the node is draining.
     * Every {@code true} must be paired with {@link #exitBulk}.
     */
    public boolean enterBulk() {
        lock.lock();
        try {
            if (bulkRefused) {
                return false;
            }
            bulkInFlight++;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Ends a bulk request that {@link #enterBulk} admitted. */
    public void exitBulk() {
        lock.lock();
        try {
            bulkInFlight--;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** §7 step 3, first half: no new bulk request is admitted. */
    public void refuseBulk() {
        lock.lock();
        try {
            bulkRefused = true;
            journal.accept(BULK_REFUSED);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until every admitted bulk request has answered, or the bound
     * passes.
     *
     * @return how many were still in flight when this returned
     */
    public int awaitNoBulk(Duration bound) throws InterruptedException {
        lock.lock();
        try {
            long left = bound.toNanos();
            while (bulkInFlight > 0 && left > 0) {
                left = changed.awaitNanos(left);
            }
            return bulkInFlight;
        } finally {
            lock.unlock();
        }
    }

    /** How many bulk requests are inside the door. */
    public int bulkInFlight() {
        lock.lock();
        try {
            return bulkInFlight;
        } finally {
            lock.unlock();
        }
    }
}
