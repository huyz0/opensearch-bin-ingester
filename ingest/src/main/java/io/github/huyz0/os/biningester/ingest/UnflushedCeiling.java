// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The unflushed-bytes ceiling (M11.7, ADR-0079): at most
 * {@link DefaultIngest#UNFLUSHED_SEGMENTS} segments' worth buffered and not yet
 * durable on a pod, an append past it waiting for the flush in flight. Every
 * method is called with the owning {@link DefaultIngest}'s lock held; the
 * condition is that lock's.
 *
 * <p>Moved out of {@code DefaultIngest} unchanged by M11.24a.
 */
final class UnflushedCeiling {

    /** {@link DefaultIngest#UNFLUSHED_SEGMENTS} segments, at least the floor. */
    private final long maxUnflushedBytes;

    /** Signalled when a flush completes, for an append waiting for room. */
    private final Condition roomToBuffer;

    /** The batch being flushed's buffered bytes, 0 when none; under the lock. */
    private long inFlightBytes;

    /** The batch in flight, whose bytes {@link #inFlightBytes} are, under the lock (M12.3); null before the first. */
    private Flush current;

    /**
     * One detached batch's claim on the in-flight bytes (M13.14, M12.3 review
     * T2). ⚠️ OPAQUE, SO A COMPLETION CAN HAND BACK ONLY ITS OWN: a counter
     * could be handed back off by one -- `generation - 1` passed every test --
     * and a token cannot be computed, only kept.
     */
    static final class Flush {
        private Flush() {
        }
    }

    UnflushedCeiling(long maxUnflushedBytes, Condition roomToBuffer) {
        this.maxUnflushedBytes = maxUnflushedBytes;
        this.roomToBuffer = roomToBuffer;
    }

    /**
     * Waits while the pod holds {@link #maxUnflushedBytes} or more buffered and
     * not yet durable; caller holds the lock (M11.7, ADR-0079).
     *
     * <p>⚠️ THIS IS WHAT BOUNDS MEMORY ONCE THE FRONT DOOR GIVES ITS ADMISSION
     * PERMIT BACK AT BUFFERED: without it, a slow store lets every release
     * admit another chunk behind the stalled flush, and the accumulator grows
     * with the number of producers (NFR-6). It WAITS rather than refusing: the
     * caller may be part-way through a body whose prefix is durable-bound.
     *
     * <p>⚠️ MEASURED FROM THE ACCUMULATORS THEMSELVES -- the active buffer and
     * the detached one in flight -- never from a per-append tally, which a
     * {@code RecordSource} throwing part-way would leave uncounted or never
     * released.
     *
     * <p>⚠️ IT WAITS ONLY FOR A FLUSH THAT WILL COME: a batch in flight, or a
     * waiter the flusher will flush for. Records a throwing source left
     * buffered with no waiter have nobody to flush them, and waiting on those
     * would wait for ever; the append proceeds, and its own records bring the
     * flush.
     *
     * @param bufferedBytes the active buffer's bytes, re-read on each wake
     * @param flushWillCome a batch is queued, or a waiter will be flushed for
     * @param closed whether the owning ingester is closed
     */
    void awaitRoom(LongSupplier bufferedBytes, BooleanSupplier flushWillCome,
            BooleanSupplier closed) throws IOException {
        while (bufferedBytes.getAsLong() + inFlightBytes >= maxUnflushedBytes
                && !closed.getAsBoolean() && flushWillCome.getAsBoolean()) {
            try {
                roomToBuffer.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for room to buffer", e);
            }
        }
        if (closed.getAsBoolean()) {
            throw new IOException("this ingester is closed");
        }
    }

    /**
     * A batch of {@code bytes} was detached for flushing; caller holds the lock.
     *
     * @return the batch's token, which its completion hands to
     *     {@link #flushEnded}
     */
    Flush detached(long bytes) {
        inFlightBytes = bytes;
        current = new Flush();
        return current;
    }

    /**
     * The flush in flight ended, however it ended; caller holds the lock.
     *
     * <p>⚠️ HOWEVER THE FLUSH ENDS, its bytes are no longer buffered here: a
     * failed batch answered its waiters exceptionally and is not retried.
     *
     * <p>⚠️ ONLY ITS OWN BYTES (M12.3, M11 review F4). A batch's completion can
     * run after the NEXT batch was detached -- the coordinator admits the next
     * one once this one is done, and this callback runs after that. Zeroing
     * unconditionally then dropped the next batch's bytes, and the pod admitted
     * about twice its ceiling. A stale token releases nothing; it still
     * signals, which costs a waiter one re-check.
     */
    void flushEnded(Flush ended) {
        // ⚠️ NO TOKEN IS NO FLUSH (M13.14 review P1): refused rather than read
        // as stale, so a completion handing back nothing fails loudly.
        java.util.Objects.requireNonNull(ended, "ended");
        if (ended == current) {
            inFlightBytes = 0;
        }
        roomToBuffer.signalAll();
    }

    /** Whether an append waits for room; caller holds {@code lock}, whose condition this is. */
    boolean hasWaiters(ReentrantLock lock) {
        return lock.hasWaiters(roomToBuffer);
    }

    /** Wakes every waiting append, so it can see the ingester closed. */
    void wakeAll() {
        roomToBuffer.signalAll();
    }
}
