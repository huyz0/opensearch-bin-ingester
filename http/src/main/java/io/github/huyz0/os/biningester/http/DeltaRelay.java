// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

/**
 * An AZ's relay: turns each 24-byte hint from the leaseholder into the delta
 * it names, read once from the store, and hands it on within this AZ
 * (M10.20, ADR-0075 decision 4).
 *
 * <p>⚠️ **ONE HINT AT A TIME, IN ARRIVAL ORDER.** A failed read is retried,
 * backing off to {@link #MAX_BACKOFF}, up to {@link #READ_ATTEMPTS} times --
 * about 30 s -- before the next hint is looked at, so a transient store error
 * delays this AZ rather than reordering it. A read that still fails is a lost
 * delta for this AZ, counted: a gap only catch-up repairs, on the leaseholder until M11.
 *
 * <p>⚠️ **A HINT FOR A DELTA THAT DOES NOT EXIST ADVANCES NOTHING.** It is
 * stale or forged; it is counted and dropped, and nothing is published.
 *
 * <p>⚠️ **ONE READ PER (DELTA, REMOTE AZ)** is the cost NFR-5 buys the cross-AZ
 * saving with: this is the only pod of its AZ that reads the delta, and it
 * reads it once unless the store fails. A read is whatever the {@link Reader}
 * does -- {@code DeltaReads.read} is one GET, and a stat only when it fails -- and
 * {@link #reads} counts reader calls, not store requests. A hint not after the
 * last delta relayed (a sender's retry after a lost answer) is skipped
 * without a read.
 */
public final class DeltaRelay implements AutoCloseable {

    /** Hints that may wait for their read. */
    public static final int QUEUE_DEPTH = 4096;

    /** Reads of one delta before it is counted lost. */
    public static final int READ_ATTEMPTS = 12;

    /** The longest wait between two reads of one delta. */
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(5);

    private static final System.Logger LOG = System.getLogger(DeltaRelay.class.getName());

    /** Reads the delta at {@code (epoch, sequence)}; empty when it was never written. */
    @FunctionalInterface
    public interface Reader {
        Optional<CommitDelta> read(long epoch, long sequence) throws IOException;
    }

    /** Waits between two reads; {@code Thread::sleep} in production. */
    @FunctionalInterface
    interface Pause {
        void pause(long millis) throws InterruptedException;
    }

    private final Reader reader;
    private final ObjLongConsumer<CommitDelta> withinAz;
    private final Duration backoff;
    private final Pause pause;
    private final BlockingQueue<DeltaHintFrame> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
    private final Thread worker;
    private final AtomicLong relayed = new AtomicLong();
    private final AtomicLong missing = new AtomicLong();
    private final AtomicLong lost = new AtomicLong();
    private final AtomicLong overflowed = new AtomicLong();
    private final AtomicLong reads = new AtomicLong();
    private volatile boolean closed;
    private long lastEpoch = -1;
    private long lastSequence = -1;
    private final AtomicLong skipped = new AtomicLong();

    /** The first retry's wait in production: 12 reads then span about 31 s. */
    public static final Duration FIRST_BACKOFF = Duration.ofMillis(100);

    /** With {@link #FIRST_BACKOFF}, the ~30 s budget ADR-0075 decision 4 names. */
    public DeltaRelay(Reader reader, ObjLongConsumer<CommitDelta> withinAz) {
        this(reader, withinAz, FIRST_BACKOFF);
    }

    /**
     * @param withinAz {@code DeltaFanOut#relayed}: local publish and same-AZ pushes
     * @param backoff the first retry's wait, doubled for each after it
     */
    public DeltaRelay(Reader reader, ObjLongConsumer<CommitDelta> withinAz, Duration backoff) {
        this(reader, withinAz, backoff, Thread::sleep);
    }

    DeltaRelay(Reader reader, ObjLongConsumer<CommitDelta> withinAz, Duration backoff,
            Pause pause) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.withinAz = Objects.requireNonNull(withinAz, "withinAz");
        this.backoff = Objects.requireNonNull(backoff, "backoff");
        if (backoff.isNegative() || backoff.isZero()) {
            throw new IllegalArgumentException("a backoff must be positive: " + backoff);
        }
        this.pause = Objects.requireNonNull(pause, "pause");
        this.worker = Thread.ofVirtual().name("delta-relay").start(this::run);
    }

    /**
     * Queues a hint for its read.
     *
     * @return false when the queue was full and the hint was dropped
     */
    public boolean offer(DeltaHintFrame hint) {
        Objects.requireNonNull(hint, "hint");
        if (closed || !queue.offer(hint)) {
            overflowed.incrementAndGet();
            return false;
        }
        return true;
    }

    private void run() {
        try {
            while (true) {
                relay(queue.take());
            }
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }

    private void relay(DeltaHintFrame hint) throws InterruptedException {
        if (hint.epoch() < lastEpoch
                || (hint.epoch() == lastEpoch && hint.sequence() <= lastSequence)) {
            skipped.incrementAndGet();
            return;
        }
        long wait = backoff.toMillis();
        for (int attempt = 1; attempt <= READ_ATTEMPTS; attempt++) {
            Optional<CommitDelta> delta;
            try {
                reads.incrementAndGet();
                delta = reader.read(hint.epoch(), hint.sequence());
            } catch (IOException | RuntimeException failed) {
                if (attempt < READ_ATTEMPTS) {
                    pause.pause(wait);
                    wait = Math.min(wait * 2, MAX_BACKOFF.toMillis());
                }
                continue;
            }
            if (delta.isEmpty()) {
                missing.incrementAndGet();
                return;
            }
            lastEpoch = hint.epoch();
            lastSequence = hint.sequence();
            try {
                withinAz.accept(delta.get(), hint.epoch());
                relayed.incrementAndGet();
            } catch (RuntimeException broken) {
                LOG.log(System.Logger.Level.WARNING, "relaying epoch " + hint.epoch()
                        + " sequence " + hint.sequence() + " failed", broken);
                lost.incrementAndGet();
            }
            return;
        }
        LOG.log(System.Logger.Level.WARNING, "delta at epoch " + hint.epoch() + " sequence "
                + hint.sequence() + " could not be read; this AZ misses it until catch-up");
        lost.incrementAndGet();
    }

    /** Deltas read and handed on within this AZ. */
    public long relayed() {
        return relayed.get();
    }

    /** Hints whose delta does not exist: dropped, advancing nothing. */
    public long missing() {
        return missing.get();
    }

    /** Deltas this AZ missed: every read failed, or handing on threw. */
    public long lost() {
        return lost.get();
    }

    /** Hints dropped because the queue was full, or offered after close. */
    public long overflowed() {
        return overflowed.get();
    }

    /** Hints not after the last delta relayed: skipped without a read. */
    public long skipped() {
        return skipped.get();
    }

    /** Reader calls made, retries included. */
    public long reads() {
        return reads.get();
    }

    /** Stops reading; hints still queued are counted as lost. */
    @Override
    public void close() {
        closed = true;
        worker.interrupt();
        try {
            worker.join(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        lost.addAndGet(queue.size());
        queue.clear();
    }
}
