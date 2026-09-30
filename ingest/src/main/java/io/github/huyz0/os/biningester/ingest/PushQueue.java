// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The ingester's push to its subscribers: one ordered queue, bounded in bytes,
 * drained by one thread off the ingest lock (extracted from
 * {@link DefaultIngest} by M10.29, unchanged in behaviour).
 */
final class PushQueue {
    private static final System.Logger LOG =
            System.getLogger(PushQueue.class.getName());

    /**
     * ⚠️ Pushes run OFF the ingest lock, on one thread so they stay ordered.
     * {@link SubscriptionHub} calls {@code sink.accept} synchronously and its own
     * contract says "a slow or dead subscriber must not block a commit" — it
     * isolates a subscriber that throws, not one that is slow. Publishing under
     * the lock would let one consumer's slow sink stall every producer on the
     * pod; publishing from many threads would let the push for offset 10
     * overtake the push for offset 5.
     *
     * <p>⚠️ ONE DEDICATED THREAD, not a single-thread executor: java-style
     * rule 4 forbids pooling virtual threads.
     *
     * <p>⚠️ The bound is BYTES ({@code maxQueuedPushBytes}, rule 7), enforced
     * when a push is offered — not a count of entries. An earlier version
     * bounded the COUNT and derived its byte figure from {@code maxSegmentBytes},
     * which is wrong: that is a flush TRIGGER checked only after a caller's whole
     * record list is buffered, so one 32 MiB `_bulk` body makes one ~32 MiB
     * segment and eight of them would have retained ~256 MiB — the very
     * OutOfMemoryError the bound exists to prevent.
     *
     * <p>⚠️ Past the budget a push is DROPPED and COUNTED. In M1 that is record
     * LOSS for the subscriber, not lag: nothing yet detects an offset gap or
     * falls back to the commit log. That is M1.6d; {@link #dropped()} is
     * the only signal until it lands.
     */
    private final LinkedBlockingQueue<PendingPush> pushes = new LinkedBlockingQueue<>();
    private final AtomicLong queuedPushBytes = new AtomicLong();
    private final Thread pusher;
    private final SubscriptionHub hub;
    private final SegmentServing serving;
    private final long maxQueuedPushBytes;
    private final AtomicLong droppedPushes = new AtomicLong();
    private final AtomicLong undeliverablePushes = new AtomicLong();

    /**
     * One queued delivery; the shutdown sentinel is the instance below.
     *
     * <p>⚠️ THE SEGMENT IS ALWAYS ATTACHED, whatever mode it is served under.
     * The mode governs how the bytes reach each SUBSCRIBER; this pod wrote
     * them and holding one copy costs one copy, bounded by
     * {@code maxQueuedPushBytes}.
     */
    private record PendingPush(CommitDelta delta, String segmentKey, byte[] segment, int bytes,
            long sequencerEpoch) {
    }

    /** Ends {@link #pushLoop} without interrupting a delivery in flight. */
    private static final PendingPush POISON =
            new PendingPush(null, null, new byte[0], 0, SubscriptionHub.EPOCH_UNKNOWN);

    /**
     * How long {@link #drain} waits for the queued pushes before it gives up
     * on them. ⚠️ FIVE seconds, not thirty: the old wait exceeded Kubernetes'
     * default grace period, so a slow subscriber turned a graceful drain into
     * a SIGKILL -- for data that is already durable by this point.
     */
    static final Duration DRAIN_BOUND = Duration.ofSeconds(5);
    private final Duration drainBound;
    private final AtomicLong abandonedPushes = new AtomicLong();

    PushQueue(SubscriptionHub hub, SegmentServing serving, long maxQueuedPushBytes) {
        this(hub, serving, maxQueuedPushBytes, DRAIN_BOUND);
    }

    PushQueue(SubscriptionHub hub, SegmentServing serving, long maxQueuedPushBytes,
            Duration drainBound) {
        this.drainBound = drainBound;
        this.hub = hub;
        this.serving = serving;
        this.maxQueuedPushBytes = maxQueuedPushBytes;
        this.pusher = Thread.ofVirtual().name("binstore-push").start(this::pushLoop);
    }

    /** Queues one durable segment for its subscribers, or drops and counts it. */
    void offer(CommitDelta delta, String segmentKey, byte[] segment, long sequencerEpoch) {
        // ⚠️ THE ARRAY IS QUEUED WHATEVER MODE IS CHOSEN, and an earlier
        // draft of this line dropped it for `proxy` on the theory that
        // holding it was the memory cost the mode exists to avoid. That is
        // WRONG IN BOTH DIRECTIONS. It buys nothing against SPEC criterion
        // 6, whose bound is flat in the CONSUMER count K and not in the
        // queue's depth -- one array serves every subscriber either way,
        // and `maxQueuedPushBytes` already bounds the depth. And it costs a
        // GET PER SEGMENT for bytes this pod wrote and still holds, which
        // is a request bought for nothing.
        //
        // ⚠️ SO THE MODE IS ABOUT THE HAND-OFF, NOT ABOUT POSSESSION:
        // `inline` gives each subscriber the whole segment at once,
        // `proxy` gives it a chunk at a time. Reading the store is for
        // bytes this pod does NOT hold -- another pod's segment, or a late
        // subscriber after the array is gone, which is M5.16's prefetch.
        int bytes = segment.length;
        if (queuedPushBytes.get() + bytes > maxQueuedPushBytes) {
            // ⚠️ Dropped, not blocked: blocking here would put a slow
            // subscriber back on the commit path by another route. ⚠️ And in
            // M1 a drop is record LOSS for that subscriber, not lag --
            // nothing yet detects an offset gap or falls back to the log.
            // That is M1.6d; this counter is the only signal until it lands.
            droppedPushes.incrementAndGet();
        } else {
            queuedPushBytes.addAndGet(bytes);
            pushes.add(new PendingPush(delta, segmentKey, segment, bytes, sequencerEpoch));
        }
    }

    /** How many pushes were dropped because a subscriber could not keep up. */
    long dropped() {
        return droppedPushes.get();
    }

    /**
     * How many pushes {@link #pushLoop} could not deliver.
     *
     * <p>⚠️ A DIFFERENT CAUSE FROM {@link #dropped()}, and two counters
     * on purpose. That one is BACK-PRESSURE -- the queue budget was full, the
     * subscriber is behind, and the answer is to slow down or raise the budget.
     * This one is a delivery that THREW -- any {@link RuntimeException} out of
     * {@link SubscriptionHub#publish}, which today is a segment that could not
     * be read and is not limited to that -- so the count alone does not say
     * where to look and the log line beside the increment quotes the cause.
     * Collapsing the two into one number would make the metric unactionable,
     * which is the objection to the silence this counter ends.
     *
     * <p>⚠️ NO SEGMENT KEY HERE. observability.md rule 1 names an object key
     * among the things that are never a label, so WHICH segment failed goes in
     * the log line beside the increment and only the count is exported.
     */
    long undeliverable() {
        return undeliverablePushes.get();
    }

    /**
     * How many pushes {@link #drain} gave up on: still queued when its bound
     * ran out, so never delivered.
     *
     * <p>⚠️ A THIRD CAUSE, beside {@link #dropped()} (the budget was full at
     * offer time) and {@link #undeliverable()} (a delivery threw). Before
     * M12.15 these were lost uncounted. The commits are durable; a consumer
     * recovers them from the commit log, as it does a dropped push.
     */
    long abandoned() {
        return abandonedPushes.get();
    }

    /** The bytes queued and in flight now, which the budget counts (M13.16). */
    long queuedBytes() {
        return queuedPushBytes.get();
    }

    /**
     * Whether the pusher thread still runs; for a test (M13.16). ⚠️ A DRAIN
     * DOES NOT GUARANTEE IT ENDS: it interrupts the pusher and re-queues the
     * sentinel, and a subscriber that blocks uninterruptibly keeps it alive.
     */
    boolean pusherAlive() {
        return pusher.isAlive();
    }

    /** How long {@link #drain} waits before it gives up; for a test (M13.16). */
    Duration drainBound() {
        return drainBound;
    }

    /** Delivers pushes in order, off the ingest lock. */
    private void pushLoop() {
        while (true) {
            PendingPush next;
            try {
                next = pushes.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (next == POISON) {
                return;
            }
            try {
                hub.publish(next.delta(), next.segmentKey(), next.segment(), serving,
                        next.sequencerEpoch());
            } catch (RuntimeException e) {
                // ⚠️ A subscriber's failure is its own. SubscriptionHub already
                // isolates a sink that throws; this is the backstop that keeps
                // one bad sink from ending delivery for every other consumer.
                //
                // ⚠️ BUT IT IS NO LONGER SILENT, which is M5.48. Until this
                // counter and this line existed, a store outage and a quiet
                // window were indistinguishable from outside the process:
                // `droppedPushes` counts the OTHER drop, the queue budget, so a
                // reader of it was told about back-pressure and told nothing
                // about a failed read.
                //
                // ⚠️ THE KEY GOES IN THE LOG AND NOT IN A LABEL. An operator
                // needs to know WHICH segment; observability.md rule 1 names an
                // object key among the things that are never a metric label, so
                // the counter carries the count and the line carries the name.
                undeliverablePushes.incrementAndGet();
                // ⚠️ THE CAUSE NAMES THE FAILING SEGMENT, not this frame. The
                // only key here is `next.segmentKey()`, the object this pod just
                // PUT and the store therefore holds; a read only ever happens
                // for a segment the pod does NOT hold, so naming this one would
                // send an operator to a healthy object and a wrong conclusion.
                LOG.log(System.Logger.Level.WARNING,
                        "push undelivered: " + e.getMessage()
                                + " -- this pod's own segment for that commit was "
                                + next.segmentKey()
                                + "; the commit is durable and consumers recover from the "
                                + "commit log", e);
                continue;
            } finally {
                queuedPushBytes.addAndGet(-next.bytes());
            }
        }
    }

    /**
     * ⚠️ A SHORT wait, and an IOException rather than an UncheckedIOException.
     * The old 30 s exceeded Kubernetes' default grace period, so a slow
     * subscriber turned a graceful drain into a SIGKILL for data that was
     * already durable — and an unchecked throw from a {@code finally} escaped
     * every {@code catch (IOException)} while masking a failed final flush.
     */
    void drain() {
        // ⚠️ POISON is queued BEHIND the pushes already waiting, so shutting
        // down DELIVERS them rather than abandoning them.
        //
        // ⚠️ An earlier version wrote `while (!pushes.offer(POISON)) pushes.poll();`
        // against a COUNT-bounded queue. poll() removes from the HEAD, so on a
        // full queue that silently discarded a queued push -- uncounted -- which
        // is the exact opposite of what the comment claimed. The queue is
        // unbounded in count now (the budget is BYTES, enforced at offer time),
        // so the sentinel always lands last and nothing is displaced.
        pushes.add(POISON);
        try {
            if (pusher.join(drainBound)) {
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pusher.interrupt();
        abandonQueued();
    }

    /**
     * Counts, releases and logs every push still queued once the pusher is
     * interrupted -- the delivery in flight finishes or not, but nothing
     * behind it is delivered.
     *
     * <p>⚠️ THE SENTINEL GOES BACK. A pusher whose subscriber swallowed the
     * interrupt takes its next entry rather than exiting; with the queue
     * emptied that would be a {@code take()} that never returns, a thread
     * leaked for the life of the process.
     */
    private void abandonQueued() {
        List<PendingPush> left = new ArrayList<>();
        pushes.drainTo(left);
        pushes.add(POISON);
        long abandoned = 0;
        for (PendingPush push : left) {
            if (push != POISON) {
                abandoned++;
                queuedPushBytes.addAndGet(-push.bytes());
            }
        }
        if (abandoned > 0) {
            abandonedPushes.addAndGet(abandoned);
            LOG.log(System.Logger.Level.WARNING,
                    "push drain gave up after " + drainBound + " with " + abandoned
                            + " push(es) undelivered; those commits are durable and "
                            + "consumers recover them from the commit log");
        }
    }
}
