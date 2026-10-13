// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.time.Duration;

/**
 * The RELEASEs this pod took from a joined term's leader, counted (ADR-0081
 * §9; M13.31g): a departing pod re-sends HELD after each one rather than on a
 * fixed pause, so a copy the leader's upload released lets it leave at once.
 *
 * <p>⚠️ A WAIT IS BOUNDED, never a promise: a RELEASE lost on the way, or a
 * leader that cannot be reached, still lets the departure ask again after at
 * most the wait's bound.
 */
final class FastReleases {

    private long count;

    /** A RELEASE was taken. */
    synchronized void released() {
        count++;
        notifyAll();
    }

    /** How many RELEASEs were taken so far. */
    synchronized long count() {
        return count;
    }

    /**
     * Waits at most {@code max} for a RELEASE after the {@code seen}-th:
     * whether one came. ⚠️ At once when one already did.
     */
    synchronized boolean awaitAfter(long seen, Duration max) throws InterruptedException {
        if (count > seen) {
            return true;
        }
        long millis = Math.max(1, max.toMillis());
        wait(millis);
        return count > seen;
    }
}
