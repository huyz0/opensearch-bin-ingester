// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Bounded replay work for one consumer node (M8.24, ADR-0065).
 *
 * <p>Replay is represented by one cursor per stream, not by one queued item
 * per historical run. Each turn asks the source for at most one run and puts
 * the cursor back at the tail, which bounds both memory and the amount of
 * catch-up work that can sit ahead of live work. All admission and execution
 * is serialized here so a live enqueue cannot invalidate a catch-up capacity
 * check between its check and its queue operations.
 */
final class CatchUpCoordinator {

    private record Work(Runnable action, boolean catchUp) {
    }

    private final CommittedDeltaSource source;
    private final CatchUpScheduler<Work> scheduler;
    private final Consumer<CommittedDeltaSource.CommittedRun> catchUpSink;
    private int reservedCatchUp;
    private int inFlightCatchUp;

    CatchUpCoordinator(CommittedDeltaSource source, int liveQuantum, int capacity,
            Consumer<CommittedDeltaSource.CommittedRun> catchUpSink) {
        this.source = Objects.requireNonNull(source, "source");
        this.scheduler = new CatchUpScheduler<>(liveQuantum, capacity);
        this.catchUpSink = Objects.requireNonNull(catchUpSink, "catchUpSink");
    }

    synchronized boolean enqueueLive(Runnable work) {
        Objects.requireNonNull(work, "work");
        if (!hasAdmissionCapacity()) {
            return false;
        }
        return scheduler.enqueueLive(new Work(work, false));
    }

    synchronized boolean enqueueCatchUp(RunKey key, long exclusiveOffset) {
        return enqueueCatchUp(List.of(new CommittedDeltaSource.ReplayRequest(
                Objects.requireNonNull(key, "key"), exclusiveOffset)));
    }

    boolean enqueueCatchUp(List<CommittedDeltaSource.ReplayRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        final int maximum;
        synchronized (this) {
            maximum = admissionCapacity();
        }
        if (maximum <= 0) {
            return false;
        }
        Map<RunKey, Long> unique = new LinkedHashMap<>();
        for (CommittedDeltaSource.ReplayRequest request : requests) {
            Objects.requireNonNull(request, "request");
            if (unique.put(request.key(), request.exclusiveOffset()) != null) {
                throw new IllegalArgumentException("duplicate catch-up stream: " + request.key());
            }
            if (unique.size() > maximum) {
                return false;
            }
        }
        List<CommittedDeltaSource.ReplayCursor> cursors = new ArrayList<>(unique.size());
        for (Map.Entry<RunKey, Long> request : unique.entrySet()) {
            // Open before taking the coordinator lock. A failure here leaves
            // no shared queue state to roll back.
            cursors.add(Objects.requireNonNull(
                    source.open(request.getKey(), request.getValue()), "replay cursor"));
        }
        synchronized (this) {
            if (cursors.size() > admissionCapacity()) {
                return false;
            }
            for (CommittedDeltaSource.ReplayCursor cursor : cursors) {
                schedule(cursor);
            }
            reservedCatchUp += cursors.size();
            return true;
        }
    }

    boolean runNext() {
        Work work;
        synchronized (this) {
            var next = scheduler.next();
            if (next.isEmpty()) {
                return false;
            }
            work = next.get();
            if (work.catchUp()) {
                inFlightCatchUp++;
            }
        }
        try {
            work.action().run();
        } catch (RuntimeException | Error failed) {
            throw failed;
        }
        return true;
    }

    private synchronized void schedule(CommittedDeltaSource.ReplayCursor cursor) {
        if (!scheduler.enqueueCatchUp(new Work(() -> runCatchUp(cursor), true))) {
            throw new IllegalStateException("catch-up admission changed while enqueuing");
        }
    }

    private void runCatchUp(CommittedDeltaSource.ReplayCursor cursor) {
        boolean complete = false;
        try {
            var next = cursor.next();
            if (next.isEmpty()) {
                complete = true;
                return;
            }
            try {
                catchUpSink.accept(next.get());
            } catch (RuntimeException | Error failed) {
                cursor.retry();
                throw failed;
            }
        } finally {
            synchronized (this) {
                inFlightCatchUp--;
                if (complete) {
                    reservedCatchUp--;
                } else {
                    // The reserved slot is kept while the callback runs, so
                    // live work cannot consume the cursor's queue position.
                    schedule(cursor);
                }
            }
        }
    }

    private int admissionCapacity() {
        return scheduler.remainingCapacity() - inFlightCatchUp;
    }

    private boolean hasAdmissionCapacity() {
        return admissionCapacity() > 0;
    }
}
