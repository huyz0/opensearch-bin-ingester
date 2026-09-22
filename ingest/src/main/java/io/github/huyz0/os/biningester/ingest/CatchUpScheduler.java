// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;

/**
 * Selects bounded live and catch-up work for one consumer node (M8.24,
 * ADR-0065).
 *
 * <p>Live work may run first, but only for the configured quantum while
 * catch-up is pending. This makes the priority rule fair under continuous live
 * traffic instead of making catch-up progress depend on the live queue ever
 * becoming empty.
 */
final class CatchUpScheduler<T> {

    private final int liveQuantum;
    private final int capacity;
    private final ArrayDeque<T> live = new ArrayDeque<>();
    private final ArrayDeque<T> catchUp = new ArrayDeque<>();
    private int liveSinceCatchUp;

    CatchUpScheduler(int liveQuantum) {
        this(liveQuantum, 64);
    }

    CatchUpScheduler(int liveQuantum, int capacity) {
        if (liveQuantum <= 0) {
            throw new IllegalArgumentException("live quantum must be positive: " + liveQuantum);
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("scheduler capacity must be positive: " + capacity);
        }
        this.liveQuantum = liveQuantum;
        this.capacity = capacity;
    }

    boolean enqueueLive(T work) {
        if (size() >= capacity) {
            return false;
        }
        live.add(Objects.requireNonNull(work, "work"));
        return true;
    }

    boolean enqueueCatchUp(T work) {
        if (size() >= capacity) {
            return false;
        }
        catchUp.add(Objects.requireNonNull(work, "work"));
        return true;
    }

    private int size() {
        return live.size() + catchUp.size();
    }

    int remainingCapacity() {
        return capacity - size();
    }

    Optional<T> next() {
        if (!catchUp.isEmpty() && (live.isEmpty() || liveSinceCatchUp >= liveQuantum)) {
            liveSinceCatchUp = 0;
            return Optional.of(catchUp.remove());
        }
        if (!live.isEmpty()) {
            liveSinceCatchUp++;
            return Optional.of(live.remove());
        }
        if (!catchUp.isEmpty()) {
            liveSinceCatchUp = 0;
            return Optional.of(catchUp.remove());
        }
        return Optional.empty();
    }
}
