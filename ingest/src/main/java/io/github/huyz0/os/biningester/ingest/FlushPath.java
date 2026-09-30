// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The durable flush path behind {@link DefaultIngest}: the active buffer, the
 * callers waiting on it, the unflushed ceiling, and the loop that decides when a
 * flush is due and hands the detached batch to {@link FlushCoordinator}.
 * {@code DefaultIngest} keeps admission, composition and shutdown's order.
 *
 * <p>⚠️ SPLIT OUT BY M13.1a so fast mode adds a sibling write path rather than
 * growing {@code DefaultIngest} past its ceiling (M13 criterion 1).
 */
final class FlushPath {

    private final IngestConfig config;
    /** ⚠️ Volatile for {@link #flushSpacingMillis()}, read off-lock; writers hold it. */
    private volatile Accumulator accumulator;

    /**
     * ⚠️ Serializes active-buffer mutation and flush detachment. The detached
     * accumulator's PUT and commit run on {@link FlushCoordinator}, whose
     * single queue preserves segment/offset order without holding this lock
     * across object-store I/O.
     *
     * <p>⚠️ It does NOT cover the push — see {@link PushQueue}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /** ⚠️ Replaces a 1 ms poll that woke 1000x/s and took this lock each time. */
    private final Condition work = lock.newCondition();

    /** The unflushed-bytes ceiling, under {@link #lock} (ADR-0079). */
    private final UnflushedCeiling ceiling;

    /**
     * The spacing that governed the batch being flushed, {@link Long#MAX_VALUE}
     * when none is (M10.7); see {@link #flushSpacingMillis()}. Written under
     * {@link #lock}, read off it.
     */
    private volatile long inFlightSpacingMillis = Long.MAX_VALUE;

    private final List<DefaultIngest.Pending> pending = new ArrayList<>();
    private final Map<RunKey, Integer> bufferedPerStream = new HashMap<>();
    private final Thread flusher;
    private final FlushCoordinator flushes;
    private volatile boolean closed;

    FlushPath(IngestConfig config, Clock clock, FlushCoordinator.Handler handler) {
        this.config = config;
        this.accumulator = new Accumulator(config, clock);
        this.ceiling = new UnflushedCeiling(Math.max(DefaultIngest.MIN_UNFLUSHED_BYTES,
                Math.multiplyExact(DefaultIngest.UNFLUSHED_SEGMENTS, config.maxSegmentBytes())),
                lock.newCondition());
        this.flushes = new FlushCoordinator(lock, work, handler,
                batch -> {
                    accumulator.adoptAdaptiveStateFrom(batch.accumulator());
                    inFlightSpacingMillis = Long.MAX_VALUE;
                });
        this.flusher = Thread.ofVirtual().name("binstore-flush").start(this::flushLoop);
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Buffers {@code records} for {@code stream} and registers the caller's
     * {@link DefaultIngest.Pending}, triggering a flush when one is due.
     */
    DefaultIngest.Pending buffer(RunKey stream, byte lane, Ingest.RecordSource records)
            throws IOException {
        lock.lock();
        try {
            // ⚠️ CHECKED AGAIN, under the lock. The caller's check is outside it,
            // so a thread that passed it can still be waiting here while close()
            // runs its final flush: it would then register a Pending that
            // nothing will ever complete, and CompletableFuture.join is
            // UNINTERRUPTIBLE, so the producer hangs until SIGKILL with its
            // records stranded in a dead accumulator.
            if (closed) {
                throw new IOException("this ingester is closed");
            }
            awaitRoomLocked();
            int before = bufferedPerStream.getOrDefault(stream, 0);
            // ⚠️ A one-element array, not a local: a lambda captures effectively
            // final variables only, and this is mutated once per record as
            // `records` is consumed -- which is the whole point (java-style.md
            // rule 8): nothing here requires `records` to have a known size, or
            // any backing collection at all, before this loop starts.
            int[] count = {0};
            // ⚠️ If `records` throws partway through (a producer's body turns
            // out malformed after some valid records -- see Ingest.append's
            // javadoc), whatever this lambda already handed to `accumulator`
            // must stay reflected in `bufferedPerStream` -- updated PER RECORD
            // below, not once after `forEachRecord` returns -- or the NEXT
            // append to this same stream computes `before` from a stale count
            // and hands out a Pending whose offset slice does not match what
            // the eventual RunCommit assigns. No Pending is registered for
            // THIS append when that happens: it has already failed, so nothing
            // should be waiting on a future for it.
            records.forEachRecord(record -> {
                accumulator.add(stream, record, lane);
                count[0]++;
                bufferedPerStream.put(stream, before + count[0]);
            });
            if (count[0] == 0) {
                // ⚠️ Refused rather than flushed: an empty append that still
                // wrote a segment would spend two requests on nothing. Detected
                // HERE rather than up front -- a RecordSource does not know its
                // own size before it has been run -- but nothing has been
                // buffered for THIS append (count is 0), so there is nothing to
                // leak into the next flush.
                throw new IllegalArgumentException(
                        "an append of no records has nothing to make durable");
            }
            DefaultIngest.Pending mine = new DefaultIngest.Pending(stream, before, count[0],
                    new CompletableFuture<>());
            pending.add(mine);
            // ⚠️ M10.32: NEVER BEHIND A QUEUED FLUSH. The coordinator admits one
            // batch at a time and REFUSES a second (M11.10) -- it once answered it
            // with the first's future, so detaching here DROPPED this batch: records
            // never written, producers blocked for ever. The flusher enqueues
            // it instead, woken when the queued flush completes.
            if (accumulator.isFlushDue() && !flushes.isQueued()) {
                enqueueFlushLocked();
            } else {
                // ⚠️ Wakes the flusher so it re-computes its deadline rather
                // than discovering this append up to a poll interval later.
                work.signal();
            }
            return mine;
        } finally {
            lock.unlock();
        }
    }

    /** Flushes whatever is buffered, whether or not the trigger says it is due. */
    void flushNow() throws IOException {
        while (true) {
            CompletableFuture<Void> done;
            lock.lock();
            try {
                if (!pending.isEmpty() && !flushes.isQueued()) {
                    done = enqueueFlushLocked();
                } else if (flushes.isQueued()) {
                    done = flushes.currentDone();
                } else {
                    return;
                }
            } finally {
                lock.unlock();
            }
            FlushCoordinator.await(done);
        }
    }

    /** See {@link DefaultIngest#whenReleased}. */
    void whenReleased(int index, Runnable onRelease) {
        lock.lock();
        try {
            pending.get(index).done().whenComplete((result, failure) -> onRelease.run());
        } finally {
            lock.unlock();
        }
    }

    /** Whether a detached batch is still queued or in flight. */
    boolean flushQueued() {
        lock.lock();
        try {
            return flushes.isQueued();
        } finally {
            lock.unlock();
        }
    }

    /** Whether an append is parked waiting for room under the unflushed ceiling (M12.3). */
    boolean waitingForRoom() {
        lock.lock();
        try {
            return ceiling.hasWaiters(lock);
        } finally {
            lock.unlock();
        }
    }

    /** How many callers are waiting for the next flush. */
    int pendingAppends() {
        lock.lock();
        try {
            return pending.size();
        } finally {
            lock.unlock();
        }
    }

    /** See {@link DefaultIngest#flushSpacingMillis()}. */
    long flushSpacingMillis() {
        return Math.min(accumulator.flushSpacing().toMillis(), inFlightSpacingMillis);
    }

    /** Refuses further appends and wakes every waiter; the final flush follows. */
    void markClosed() {
        lock.lock();
        try {
            closed = true;
            work.signalAll();
            ceiling.wakeAll();
        } finally {
            lock.unlock();
        }
        flusher.interrupt();
    }

    /**
     * ⚠️ Anything that slipped in after the final flush is FAILED, never left
     * waiting: join() is uninterruptible, so a Pending nobody completes is a
     * producer thread hung until SIGKILL.
     */
    void failRemaining() {
        lock.lock();
        try {
            for (DefaultIngest.Pending p : pending) {
                p.done().completeExceptionally(
                        new IOException("the ingester closed before this append was flushed"));
            }
            pending.clear();
            bufferedPerStream.clear();
        } finally {
            lock.unlock();
        }
        flushes.close();
    }

    private void flushLoop() {
        lock.lock();
        try {
            while (!closed) {
                if (!pending.isEmpty() && !flushes.isQueued() && accumulator.isFlushDue()) {
                    enqueueFlushLocked();
                    continue;
                }
                // ⚠️ A quarter of the floor, so the trigger is noticed promptly
                // without polling; an append signals this condition as well, so
                // the wait is an upper bound rather than a poll. ⚠️ M10.7: and
                // no later than the EARLIEST buffered lane's deadline, read off
                // the INJECTED clock (ADR-0074 decision 3), so a +2 record wakes
                // the loop at its own deadline. ⚠️ ONLY WHEN THE CHECK ABOVE
                // COULD FLUSH, i.e. with a waiter and no flush queued; in any
                // other state a past deadline answers 0 and the loop would spin
                // at the 1 ms minimum doing nothing: behind a queued flush until
                // it signals, and -- with NO waiter -- for ever, since records a
                // throwing RecordSource left buffered have nobody to flush them.
                long waitMillis = config.intervalFloor().toMillis() / 4;
                if (!pending.isEmpty() && !flushes.isQueued()) {
                    waitMillis = Math.min(waitMillis, accumulator.millisUntilDue());
                }
                try {
                    work.awaitNanos(Math.max(1_000_000L,
                            TimeUnit.MILLISECONDS.toNanos(waitMillis)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Waits for room under the unflushed ceiling; caller holds {@link #lock}. */
    private void awaitRoomLocked() throws IOException {
        ceiling.awaitRoom(() -> accumulator.bufferedBytes(),
                () -> flushes.isQueued() || !pending.isEmpty(), () -> closed);
    }

    /** Detaches the active buffer and queues its store work; caller holds {@link #lock}. */
    private CompletableFuture<Void> enqueueFlushLocked() {
        if (pending.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (flushes.isQueued()) { // ⚠️ BEFORE the detach, which would lose them (H5)
            throw new IllegalStateException("a flush is already queued");
        }
        List<DefaultIngest.Pending> batch = List.copyOf(pending);
        pending.clear();
        bufferedPerStream.clear();
        Accumulator detached = accumulator;
        accumulator = accumulator.emptyCopy();
        // ⚠️ TAKEN BEFORE THE DRAIN, which resets the detached buffer's lanes:
        // the governor samples the spacing at this batch's data PUT, and by
        // then both buffers would answer as if no +2 record had been written.
        inFlightSpacingMillis = detached.flushSpacing().toMillis();
        // ⚠️ TAKEN BEFORE THE DRAIN, which zeroes the detached buffer's count.
        UnflushedCeiling.Flush flush = ceiling.detached(detached.bufferedBytes());
        CompletableFuture<Void> done = flushes.enqueue(batch, detached);
        // ⚠️ HOWEVER THE FLUSH ENDS -- see UnflushedCeiling.flushEnded.
        done.whenComplete((ignored, failure) -> {
            lock.lock();
            try {
                ceiling.flushEnded(flush);
            } finally {
                lock.unlock();
            }
        });
        return done;
    }
}
