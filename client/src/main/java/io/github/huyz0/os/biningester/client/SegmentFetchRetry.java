// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.time.Duration;
import java.util.Objects;

/**
 * How a consumer retries a failed segment fetch (M10.23, research 02 §6).
 *
 * <p>⚠️ **RETRIED, BECAUSE A THROW OUT OF {@code readNext} PAUSES THE SHARD**
 * until an operator resumes it. A {@code 502} from the ingester's segment
 * route, an ingester that is restarting, a {@code 503} from the store under a
 * grant: each is over in seconds, and before this each one cost a shard an
 * operator had to restart by hand.
 *
 * <p>⚠️ **AND BOUNDED, BECAUSE SOME FAILURES ARE NOT TRANSIENT.** An expired
 * grant is re-sent unchanged on every retry, and a segment that is gone stays
 * gone; after {@link #maxAttempts} the failure surfaces exactly as it did
 * before, so a shard that cannot make progress is paused where an operator
 * sees it rather than retrying out of sight for ever.
 *
 * @param floor the wait after the first failure, jittered as the subscription
 *     reconnect's is ({@code HttpSubscriptionTransport.jitteredMillis})
 * @param ceiling how far the doubling wait may grow
 * @param maxAttempts fetches of one segment before the failure surfaces
 * @param sleeper how a backoff is waited out; {@link #DEFAULT} sleeps the
 *     calling thread, and a test injects one that records instead
 * @param clockMillis the host's relative clock in millis, or {@code null}. With
 *     one a backoff is a DUE TIME, which passes whether or not anyone waits, so
 *     a catch-up lane backing off gives its turn to live and is retried when
 *     due (M12.26); without one it is a debt only waiting pays, as before. This
 *     module reads no clock of its own (non-negotiable 7): the plugin passes
 *     OpenSearch's.
 */
public record SegmentFetchRetry(Duration floor, Duration ceiling, int maxAttempts,
        Sleeper sleeper, java.util.function.LongSupplier clockMillis) {

    /** Waits out one backoff. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration wait) throws InterruptedException;
    }

    /**
     * Eight attempts, from one second doubling to thirty: about ninety
     * seconds of trying before the shard is paused.
     *
     * <p>⚠️ **THE SUBSCRIPTION RECONNECT's OWN FLOOR AND CEILING** (M8.16), for
     * the same reason: a rolling deploy fails every consumer's fetch at the
     * same instant, and the jittered one-second floor is what spreads their
     * retries. Ninety seconds covers an ingester restart and a store's 5xx
     * burst; a failure outlasting it is not one retrying will fix.
     *
     * <p>⚠️ **THE ONE PLACE A REAL SLEEP IS BOUND**, as the reconnect's
     * {@code sleepAndGrow} is: the policy never names it, so every case drives
     * the backoff through an injected sleeper and none of them waits.
     */
    public static final SegmentFetchRetry DEFAULT = new SegmentFetchRetry(
            HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR,
            HttpSubscriptionTransport.DEFAULT_RETRY_CEILING, 8, Thread::sleep);

    /** A policy with no clock: a backoff is a debt only waiting pays. */
    public SegmentFetchRetry(Duration floor, Duration ceiling, int maxAttempts, Sleeper sleeper) {
        this(floor, ceiling, maxAttempts, sleeper, null);
    }

    /** This policy, its backoffs due on {@code clockMillis} (M12.26). */
    public SegmentFetchRetry withClock(java.util.function.LongSupplier clockMillis) {
        return new SegmentFetchRetry(floor, ceiling, maxAttempts, sleeper, clockMillis);
    }

    public SegmentFetchRetry {
        Objects.requireNonNull(floor, "floor");
        Objects.requireNonNull(ceiling, "ceiling");
        Objects.requireNonNull(sleeper, "sleeper");
        if (floor.isNegative() || floor.isZero() || ceiling.compareTo(floor) < 0) {
            throw new IllegalArgumentException("a retry floor is positive and at most the ceiling");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("a segment is fetched at least once");
        }
    }
}
