// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes durable deltas to this pod's subscribers in chain order, from
 * whichever source hands them over (M10.18, ADR-0075).
 *
 * <p>⚠️ **ONE PUBLISHER, ONE ORDER.** In an assembled node the leaseholder's
 * commit hook and the {@code /ctl/push} route both feed this, and nothing else
 * publishes. A delta whose {@code (epoch, sequence)} is not strictly after the
 * last one published is dropped: a duplicate is then harmless, and a stale
 * leaseholder's push behind a newer epoch cannot reach a consumer. Two
 * publishers racing -- the flushing pod publishing its own commit, the push
 * publishing it again -- is the shape ADR-0075 rejects.
 *
 * <p>⚠️ **HELD BYTES ARE REGISTERED BEFORE THE COMMIT**, by the pod that wrote
 * the segment, so the push that comes back for it finds them and the writer
 * still serves its own segment inline or from memory at zero GETs. They are
 * used once and forgotten. The map is bounded in BYTES and drops its oldest
 * entry first; a segment whose bytes were dropped publishes cold -- one GET
 * on this pod, the pre-existing bound.
 *
 * <p>⚠️ **THE QUEUE IS BOUNDED IN BYTES AS WELL AS DELTAS.** A delta is
 * metadata, but ~150 KiB of it at scale (ADR-0075), so a count alone would
 * let a stalled worker hold hundreds of MiB before the first drop. Offering
 * past either bound drops the delta and counts it -- a gap for this pod's
 * consumers, which only catch-up off the leaseholder repairs until M11
 * (ADR-0075: a follower cannot serve it). Blocking instead
 * would put a slow subscriber on the commit path, which is the trade
 * {@code DefaultIngest}'s push queue already refuses for the same reason.
 *
 * <p>⚠️ **CLOSING DELIVERS WHAT IS QUEUED**, for five seconds -- the bound
 * {@code DefaultIngest.drainPushes} keeps for the same Kubernetes grace
 * period -- then interrupts the worker, waits at most five more for it to
 * stop, and counts whatever it abandons.
 */
public final class ChainPublisher implements AutoCloseable {

    /** Deltas that may wait for publication. */
    public static final int QUEUE_DEPTH = 4096;

    /** Estimated bytes of deltas that may wait for publication. */
    public static final long QUEUE_BYTES = 64L << 20;

    private static final java.time.Duration CLOSE_WAIT = java.time.Duration.ofSeconds(5);

    private static final System.Logger LOG = System.getLogger(ChainPublisher.class.getName());

    private record Offered(long epoch, CommitDelta delta, long bytes) {
    }

    private final SubscriptionHub hub;
    private final SegmentServing serving;
    private final long maxHeldBytes;
    private final BlockingQueue<Offered> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
    private final Thread worker;

    private final Object heldLock = new Object();
    private final Map<String, byte[]> held = new LinkedHashMap<>();
    private long heldBytes;

    private long lastEpoch = -1;
    private long lastSequence = -1;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong stale = new AtomicLong();
    private final AtomicLong overflowed = new AtomicLong();
    private final AtomicLong heldEvicted = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong abandoned = new AtomicLong();
    private final AtomicLong queuedBytes = new AtomicLong();
    private volatile boolean closing;

    public ChainPublisher(SubscriptionHub hub, SegmentServing serving, long maxHeldBytes) {
        this.hub = Objects.requireNonNull(hub, "hub");
        this.serving = Objects.requireNonNull(serving, "serving");
        if (maxHeldBytes < 0) {
            throw new IllegalArgumentException("a negative hold is not a bound: " + maxHeldBytes);
        }
        this.maxHeldBytes = maxHeldBytes;
        this.worker = Thread.ofVirtual().name("chain-publisher").start(this::run);
    }

    /** Registers the bytes of a segment this pod has PUT and is about to commit. */
    public void hold(String segmentKey, byte[] bytes) {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(bytes, "bytes");
        synchronized (heldLock) {
            if (bytes.length > maxHeldBytes) {
                heldEvicted.incrementAndGet();
                return;
            }
            byte[] replaced = held.remove(segmentKey);
            if (replaced != null) {
                heldBytes -= replaced.length;
            }
            var oldestFirst = held.entrySet().iterator();
            while (heldBytes + bytes.length > maxHeldBytes && oldestFirst.hasNext()) {
                heldBytes -= oldestFirst.next().getValue().length;
                oldestFirst.remove();
                heldEvicted.incrementAndGet();
            }
            held.put(segmentKey, bytes);
            heldBytes += bytes.length;
        }
    }

    /** Forgets held bytes whose commit failed, so they do not wait for a push that never comes. */
    public void forget(String segmentKey) {
        synchronized (heldLock) {
            byte[] removed = held.remove(segmentKey);
            if (removed != null) {
                heldBytes -= removed.length;
            }
        }
    }

    /**
     * Queues a durable delta for publication.
     *
     * @return false when the queue was full and the delta was dropped
     */
    public boolean offer(long epoch, CommitDelta delta) {
        Objects.requireNonNull(delta, "delta");
        long bytes = estimatedBytes(delta);
        if (queuedBytes.addAndGet(bytes) > QUEUE_BYTES) {
            queuedBytes.addAndGet(-bytes);
            overflowed.incrementAndGet();
            return false;
        }
        if (!queue.offer(new Offered(epoch, delta, bytes))) {
            queuedBytes.addAndGet(-bytes);
            overflowed.incrementAndGet();
            return false;
        }
        return true;
    }

    /** Roughly what the delta holds on the heap: its keys and its runs. */
    static long estimatedBytes(CommitDelta delta) {
        long bytes = 64;
        for (SegmentCommit segment : delta.segments()) {
            bytes += 64 + 2L * segment.segmentKey().length() + 48L * segment.runs().size();
        }
        return bytes;
    }

    private void run() {
        while (true) {
            Offered next;
            try {
                next = queue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (next == null) {
                if (closing) {
                    return;
                }
                continue;
            }
            queuedBytes.addAndGet(-next.bytes());
            if (!after(next.epoch(), next.delta().sequence())) {
                stale.incrementAndGet();
                continue;
            }
            publish(next.epoch(), next.delta());
        }
    }

    private boolean after(long epoch, long sequence) {
        if (epoch < lastEpoch || (epoch == lastEpoch && sequence <= lastSequence)) {
            return false;
        }
        lastEpoch = epoch;
        lastSequence = sequence;
        return true;
    }

    private void publish(long epoch, CommitDelta delta) {
        for (SegmentCommit segment : delta.segments()) {
            byte[] bytes;
            synchronized (heldLock) {
                bytes = held.remove(segment.segmentKey());
                if (bytes != null) {
                    heldBytes -= bytes.length;
                }
            }
            try {
                // One segment at a time, so each carries its own held bytes;
                // the sequence is the delta's, which is the chain position.
                hub.publish(new CommitDelta(delta.sequence(), List.of(segment)),
                        segment.segmentKey(), bytes, serving, epoch);
            } catch (RuntimeException publishFailed) {
                failed.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, "publish of " + segment.segmentKey()
                        + " failed; a gap for this pod's consumers until catch-up repairs it", publishFailed);
            }
        }
        published.incrementAndGet();
    }

    /** Deltas published. */
    public long published() {
        return published.get();
    }

    /** Deltas dropped because they were not after the last one published. */
    public long stale() {
        return stale.get();
    }

    /** Segments whose publication threw; each is a gap its consumers repair. */
    public long failed() {
        return failed.get();
    }

    /** Deltas still queued when {@link #close} gave up waiting. */
    public long abandoned() {
        return abandoned.get();
    }

    /** Deltas dropped because the queue was full, in deltas or in bytes. */
    public long overflowed() {
        return overflowed.get();
    }

    /** Held segments dropped to keep the byte bound. */
    public long heldEvicted() {
        return heldEvicted.get();
    }

    /** Bytes currently held for segments awaiting their push. */
    public long heldBytes() {
        synchronized (heldLock) {
            return heldBytes;
        }
    }

    /** How many offered deltas are still waiting. */
    public int queued() {
        return queue.size();
    }

    /** Estimated bytes of the deltas still waiting. */
    public long queuedBytes() {
        return queuedBytes.get();
    }

    @Override
    public void close() {
        closing = true;
        try {
            if (!worker.join(CLOSE_WAIT)) {
                worker.interrupt();
                worker.join(CLOSE_WAIT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            worker.interrupt();
        }
        abandoned.addAndGet(queue.size());
        queue.clear();
    }
}
