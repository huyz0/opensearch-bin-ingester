// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A writer's EXPOSEDs, by batch identity -- the term, {@code assignedAfter}
 * and every run's stream and first offset (ADR-0081 §2.4; M13.29a).
 *
 * <p>⚠️ ONE MAY ARRIVE BEFORE ITS ASSIGNED: a batch the leader's own copy
 * completes is exposed at its flush, while the ASSIGNED is still on its way
 * (M13.27w's carry). It is kept for the writer that asks.
 *
 * <p>⚠️ THE KEPT ONES ARE BOUNDED, the oldest dropped first: one nobody asks
 * for -- its writer timed out and retried -- costs memory, never a leak.
 */
public final class ExposedInbox {

    /** How many delivered EXPOSEDs are kept for writers yet to ask. */
    public static final int CAPACITY = 4096;

    private record Key(long epoch, long assignedAfter, Set<FastWriteFrame.RunAt> runs) {
    }

    private final Map<Key, CompletableFuture<Void>> batches = new LinkedHashMap<>();

    private static Key key(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs) {
        return new Key(epoch, assignedAfter, Set.copyOf(runs));
    }

    /** An EXPOSED of term {@code epoch} arrived. */
    public synchronized void deliver(long epoch, FastWriteFrame.Exposed exposed) {
        batches.computeIfAbsent(key(epoch, exposed.assignedAfter(), exposed.runs()),
                k -> new CompletableFuture<>()).complete(null);
        Iterator<Map.Entry<Key, CompletableFuture<Void>>> oldest = batches.entrySet().iterator();
        while (batches.size() > CAPACITY && oldest.hasNext()) {
            if (oldest.next().getValue().isDone()) {
                oldest.remove();
            }
        }
    }

    /** Whether a writer is waiting for this batch: for tests, which must not sleep. */
    synchronized boolean waiting(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs) {
        CompletableFuture<Void> f = batches.get(key(epoch, assignedAfter, runs));
        return f != null && !f.isDone();
    }

    /**
     * Whether the batch is exposed within {@code wait}; its EXPOSED is taken
     * either way.
     */
    public boolean await(long epoch, long assignedAfter, List<FastWriteFrame.RunAt> runs,
            Duration wait) throws InterruptedException {
        Key k = key(epoch, assignedAfter, runs);
        CompletableFuture<Void> f;
        synchronized (this) {
            f = batches.computeIfAbsent(k, ignored -> new CompletableFuture<>());
        }
        try {
            if (wait.isZero()) {
                return f.isDone();
            }
            f.get(wait.toNanos(), TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException | ExecutionException notExposed) {
            return false;
        } finally {
            synchronized (this) {
                batches.remove(k, f);
            }
        }
    }
}
