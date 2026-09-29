// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.FetchMode;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * One consumer's segment bytes, and the retry of a fetch that failed (M10.23).
 *
 * <p>⚠️ **THE POLICY: THE FAILED DELIVERY STAYS AT THE HEAD AND IS RETRIED BY
 * A LATER {@code readNext}, ONCE ITS BACKOFF HAS BEEN WAITED OUT OF CALLERS'
 * TIMEOUTS.** Retrying inside one call's deadline was the alternative, and it
 * needs what this module may not have: the time remaining is a clock reading
 * (non-negotiable 7), and a call that has already blocked on the queue and on
 * a 30 s fetch has no honest remainder to retry in. Here time is counted in
 * waits this class itself asked for, which is a LOWER bound on time passed --
 * so the fetch rate is bounded without a clock, and:
 * <ul>
 *   <li>a call waits at most its own timeout, and never after a fetch it made;
 *   <li>{@code readNext(ZERO)} neither waits nor fetches while a backoff is
 *       owed, so {@code drain}'s zero-timeout reads cannot become a request loop;
 *   <li>offsets stay in order: the queue keeps the failed delivery at its head
 *       and returns its permits, and nothing behind it is read first.
 * </ul>
 * ⚠️ The cost of the choice, said rather than hidden: a caller that only ever
 * polls with ZERO never serves a backoff and never retries. The shard
 * consumer's first read of every batch carries the full poll timeout.
 *
 * <p>⚠️ **WITH A CLOCK ON THE POLICY, AS THE PLUGIN RUNS (M12.26), THE BACKOFF
 * IS A DUE TIME INSTEAD**, on the host's clock rather than one this module
 * reads: a wait is what is left until due, never more; a ZERO-only caller
 * does retry once due, still without a request loop before it; and a lane
 * backing off can give its turn to the other ({@link #backingOff()}). The
 * paragraph above describes the policy with no clock.
 *
 * <p>⚠️ **ONLY THE FETCH IS RETRIED.** A segment that arrived and will not
 * decode is corrupt however often it is fetched, so that failure surfaces on
 * the first attempt; and one delivery is one fetch per attempt, so a retry
 * adds attempts per failed SEGMENT -- at most {@code maxAttempts} -- and never
 * a request per record.
 */
final class SegmentFetcher {

    private static final System.Logger LOG = System.getLogger(SegmentFetcher.class.getName());

    /**
     * A fetch that failed or is not yet due: the delivery stays queued and the
     * poll answers empty. Never seen outside this package.
     */
    static final class Deferred extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final boolean attempted;
        private final SegmentFetcher from;

        Deferred(String message, Throwable cause, boolean attempted, SegmentFetcher from) {
            super(message, cause, false, false);
            this.attempted = attempted;
            this.from = from;
        }

        /** The fetcher whose backoff this is -- one per lane since M12.12. */
        SegmentFetcher from() {
            return from;
        }

        /** Whether this poll spent its time on a fetch, so owes the caller no wait. */
        boolean attempted() {
            return attempted;
        }
    }

    private final SegmentSource source;
    private final SegmentFetchRetry retry;
    private int failures;
    private Duration backoff;
    private Duration owed = Duration.ZERO;
    /**
     * With a clock on the policy (M12.26): when the backoff ends, on it. The
     * backoff is then a DUE TIME, which passes whether or not anyone waits --
     * so a catch-up that gives its turn to live is still retried once due --
     * where without one it is a debt that only {@link #awaitBackoff} pays.
     */
    private long dueAtMillis = Long.MIN_VALUE;
    /**
     * The node's fetch count BEFORE this run's first held answer of the round,
     * or -1 (M10.28 review F4): a run pauses after the node has failed
     * {@code maxAttempts} times SINCE it started waiting, not at a count the
     * node reached before -- so a resumed or newly assigned run gets its own
     * round, as a run fetching for itself does.
     */
    private int heldBaseline = -1;

    SegmentFetcher(SegmentSource source, SegmentFetchRetry retry) {
        this.source = source;
        this.retry = Objects.requireNonNull(retry, "retry");
        this.backoff = retry.floor();
    }

    /**
     * The segment's bytes: carried on the delivery, fetched by key from the
     * ingester for an empty {@code proxy} delivery, or fetched under a grant.
     *
     * <p>⚠️ A FAILED FETCH IS NEVER AN EMPTY ARRAY. Returning one would have
     * {@code readNext} report an ordinary empty poll while a whole window went
     * missing. It is a {@link Deferred} while the policy has attempts left and
     * the failure itself once it has none.
     *
     * <p>⚠️ AND NO SOURCE IS AN {@code IllegalStateException}, never retried:
     * it is the mirror of {@code SubscriptionHub}'s null-issuer guard, and a
     * pod that elects {@code direct} for a consumer built without a source is
     * a misconfiguration an operator must see.
     *
     * @throws Deferred if the fetch failed and will be retried, or is not due
     * @throws IOException if the fetch failed on its last attempt
     */
    byte[] bytesOf(Delivery delivery) throws IOException {
        if (delivery.via() == FetchMode.PROXY && delivery.segment().length == 0) {
            // ⚠️ M10.2, ADR-0073: A PROXY EVENT OVER HTTP CARRIES COORDINATES
            // ONLY. Decoding its empty array was the pre-M10 behaviour, and a
            // zero-length segment fails its footer check -- so every batch
            // above the inline cap was lost to a consumer reached over HTTP.
            // An in-process subscriber is handed the assembled bytes and is
            // not fetched again.
            requireSource(delivery, "proxy", "a consumer reached over HTTP fetches it from "
                    + "its ingester (M10.2)");
            return retried(delivery, () -> source.fetchSegment(delivery.segmentKey()));
        }
        if (delivery.via() != FetchMode.DIRECT) {
            return delivery.segment();
        }
        requireSource(delivery, "direct", "a deployment enabling `direct` builds one (M5.45g)");
        return retried(delivery, () -> source.fetch(delivery.grant()));
    }

    private void requireSource(Delivery delivery, String mode, String remedy) {
        if (source == null) {
            throw new IllegalStateException("segment " + delivery.segmentKey() + " was served `"
                    + mode + "` to a consumer with no segment source; " + remedy);
        }
    }

    @FunctionalInterface
    private interface Fetch {
        byte[] get() throws IOException;
    }

    private byte[] retried(Delivery delivery, Fetch fetch) throws IOException {
        synchronized (this) {
            if (owesWait()) {
                throw new Deferred("segment " + delivery.segmentKey() + " is backing off", null,
                        false, this);
            }
        }
        byte[] bytes;
        try {
            bytes = fetch.get();
        } catch (SegmentFetchHeldException held) {
            throw heldAnswer(delivery, held);
        } catch (IOException failed) {
            throw failedAttempt(delivery, failed);
        }
        synchronized (this) {
            failures = 0;
            backoff = retry.floor();
            heldBaseline = -1;
        }
        return bytes;
    }

    /**
     * A node's held failure (M10.28): not this run's attempt, so neither
     * counted nor grown -- the run waits out what is left of the hold and
     * asks again. ⚠️ SURFACED ONCE THE NODE HAS FAILED THE BUDGET's WORTH OF
     * FETCHES SINCE THIS RUN STARTED WAITING, which is the run's own rule
     * applied to the one schedule the node's runs share: a run that is only
     * ever answered from the hold still pauses, and a resumed one gets a
     * fresh round rather than pausing again on its first poll (review F4).
     */
    private synchronized RuntimeException heldAnswer(Delivery delivery,
            SegmentFetchHeldException held) throws IOException {
        // ⚠️ THE FAILURE THAT STARTED THE HOLD IS THIS RUN's ROUND's FIRST, so
        // the baseline is one below the count first seen; and a count that
        // fell below it -- the node's own reset after an old outage -- starts
        // a new round rather than one that can never end.
        if (heldBaseline < 0 || held.nodeAttempts() <= heldBaseline) {
            heldBaseline = held.nodeAttempts() - 1;
        }
        int sinceWaiting = held.nodeAttempts() - heldBaseline;
        if (sinceWaiting >= retry.maxAttempts()) {
            failures = 0;
            backoff = retry.floor();
            heldBaseline = -1;
            throw new IOException("segment " + delivery.segmentKey() + " could not be fetched "
                    + "(this node's attempt " + sinceWaiting + " of " + retry.maxAttempts()
                    + " since this run began waiting): " + held.getMessage(), held);
        }
        owe(held.remaining().isZero() ? Duration.ofMillis(1) : held.remaining());
        return new Deferred("segment " + delivery.segmentKey() + " is held failed on this node",
                held, false, this);
    }

    private synchronized RuntimeException failedAttempt(Delivery delivery, IOException failed)
            throws IOException {
        failures++;
        if (failures >= retry.maxAttempts()) {
            // ⚠️ RESET AS IT SURFACES, so an operator's resume starts a fresh
            // round of attempts at the same head rather than surfacing again
            // on its first poll.
            int attempts = failures;
            failures = 0;
            backoff = retry.floor();
            heldBaseline = -1;
            throw new IOException("segment " + delivery.segmentKey() + " could not be fetched "
                    + "(attempt " + attempts + " of " + attempts + "): " + failed.getMessage(),
                    failed);
        }
        owe(Duration.ofMillis(HttpSubscriptionTransport.jitteredMillis(backoff)));
        backoff = HttpSubscriptionTransport.grow(backoff, retry.ceiling());
        LOG.log(System.Logger.Level.WARNING, "segment {0} fetch attempt {1} of {2} failed, "
                + "retrying in {3}: {4}", delivery.segmentKey(), failures, retry.maxAttempts(),
                owed, failed.getMessage());
        return new Deferred("segment " + delivery.segmentKey() + " fetch failed", failed, true,
                this);
    }

    /**
     * Waits out as much of the owed backoff as {@code timeout} allows.
     *
     * <p>⚠️ NEVER LONGER THAN THE TIMEOUT, and not at all for a zero one.
     */
    void awaitBackoff(Duration timeout) throws InterruptedException {
        Duration wait;
        synchronized (this) {
            Duration left = retry.clockMillis() == null ? owed
                    : Duration.ofMillis(Math.max(0, dueAtMillis - retry.clockMillis().getAsLong()));
            wait = left.compareTo(timeout) < 0 ? left : timeout;
        }
        if (wait.compareTo(Duration.ZERO) <= 0) {
            return;
        }
        retry.sleeper().sleep(wait);
        synchronized (this) {
            owed = owed.minus(wait).isNegative() ? Duration.ZERO : owed.minus(wait);
        }
    }

    /** Starts a backoff of {@code wait}: a debt, and with a clock a due time too. */
    private void owe(Duration wait) {
        owed = wait;
        if (retry.clockMillis() != null) {
            dueAtMillis = retry.clockMillis().getAsLong() + wait.toMillis();
        }
    }

    /** Whether the next fetch must wait: until the due time, or until the debt is paid. */
    private synchronized boolean owesWait() {
        return retry.clockMillis() == null ? owed.compareTo(Duration.ZERO) > 0
                : retry.clockMillis().getAsLong() < dueAtMillis;
    }

    /**
     * Whether this fetcher is backing off and its time has not come, which is
     * only ever true with a clock (M12.26).
     *
     * <p>⚠️ FALSE WITHOUT ONE, deliberately: a debt is paid only by waiting it
     * out, so a lane that yielded its turn on it would never be retried while
     * the other lane kept the reader busy.
     */
    synchronized boolean backingOff() {
        return retry.clockMillis() != null && retry.clockMillis().getAsLong() < dueAtMillis;
    }
}
