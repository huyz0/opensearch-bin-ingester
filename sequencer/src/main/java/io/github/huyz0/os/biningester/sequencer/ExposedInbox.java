// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A writer's EXPOSEDs, by batch identity -- the term, {@code assignedAfter}
 * and every run's stream and first offset (ADR-0081 §2.4; M13.29a) -- and its
 * DISCARDEDs, by the term and the batch's idempotency key (M13.34t; §2.3).
 *
 * <p>⚠️ ONE MAY ARRIVE BEFORE ITS ASSIGNED: a batch the leader's own copy
 * completes is exposed at its flush, while the ASSIGNED is still on its way
 * (M13.27w's carry); a switch may discard a batch before its writer has its
 * ASSIGNED. Each is kept for the writer that asks.
 *
 * <p>⚠️ THE KEPT ONES ARE BOUNDED, the oldest dropped first: one nobody asks
 * for -- its writer timed out and retried -- costs memory, never a leak. A
 * kept DISCARDED is taken by the first wait for its batch, so the batch's
 * retry is never refused by its predecessor's discard.
 */
public final class ExposedInbox {

    /** How many delivered EXPOSEDs, and how many DISCARDEDs, are kept for writers yet to ask. */
    public static final int CAPACITY = 4096;

    private record Key(long epoch, long assignedAfter, Set<FastWriteFrame.RunAt> runs) {
    }

    private record Discard(long epoch, FastJournalRecord.IdempotencyKey key) {
    }

    /** True once exposed, false once discarded. */
    private final Map<Key, CompletableFuture<Boolean>> batches = new LinkedHashMap<>();
    private final Map<Discard, Key> waitingByKey = new HashMap<>();
    private final Set<Discard> discards = new LinkedHashSet<>();

    private static Key key(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs) {
        return new Key(epoch, assignedAfter, Set.copyOf(runs));
    }

    /** An EXPOSED of term {@code epoch} arrived. */
    public synchronized void deliver(long epoch, FastWriteFrame.Exposed exposed) {
        batches.computeIfAbsent(key(epoch, exposed.assignedAfter(), exposed.runs()),
                k -> new CompletableFuture<>()).complete(true);
        Iterator<Map.Entry<Key, CompletableFuture<Boolean>>> oldest =
                batches.entrySet().iterator();
        while (batches.size() > CAPACITY && oldest.hasNext()) {
            if (oldest.next().getValue().isDone()) {
                oldest.remove();
            }
        }
    }

    /**
     * Term {@code epoch}'s leader discarded the batch under {@code key}
     * (M13.34t): its wait ends unexposed, now or when its writer asks.
     */
    public synchronized void discarded(long epoch, FastJournalRecord.IdempotencyKey key) {
        Discard d = new Discard(epoch, key);
        Key waiting = waitingByKey.get(d);
        if (waiting != null) {
            batches.get(waiting).complete(false);
            return;
        }
        discards.add(d);
        Iterator<Discard> oldest = discards.iterator();
        while (discards.size() > CAPACITY) {
            oldest.next();
            oldest.remove();
        }
    }

    /** Whether a writer is waiting for this batch: for tests, which must not sleep. */
    synchronized boolean waiting(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs) {
        CompletableFuture<Boolean> f = batches.get(key(epoch, assignedAfter, runs));
        return f != null && !f.isDone();
    }

    /**
     * Whether the batch is exposed within {@code wait}; its EXPOSED is taken
     * either way.
     */
    public boolean await(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs,
            Duration wait) throws InterruptedException {
        return await(epoch, null, assignedAfter, runs, wait);
    }

    /**
     * The same, the wait ended unexposed by a DISCARDED of the batch's
     * {@code key} (M13.34t).
     */
    public boolean await(long epoch, FastJournalRecord.IdempotencyKey key, long assignedAfter,
            List<FastWriteFrame.RunAt> runs, Duration wait) throws InterruptedException {
        Key k = key(epoch, assignedAfter, runs);
        Discard d = key == null ? null : new Discard(epoch, key);
        CompletableFuture<Boolean> f;
        synchronized (this) {
            if (d != null && discards.remove(d)) {
                return false;
            }
            f = batches.computeIfAbsent(k, ignored -> new CompletableFuture<>());
            if (d != null) {
                waitingByKey.put(d, k);
            }
        }
        try {
            if (wait.isZero()) {
                return f.isDone() && f.getNow(false);
            }
            return f.get(wait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | ExecutionException notExposed) {
            return false;
        } finally {
            synchronized (this) {
                batches.remove(k, f);
                if (d != null) {
                    waitingByKey.remove(d, k);
                }
            }
        }
    }
}
