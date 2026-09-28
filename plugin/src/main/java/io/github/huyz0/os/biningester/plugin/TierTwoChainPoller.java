// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Node-scoped Tier 2 chain poller. */
final class TierTwoChainPoller {
    @FunctionalInterface
    interface DeltaReader {
        Optional<CommitDelta> get(long epoch, long sequence) throws IOException;
    }

    private long epoch;
    private long currentSequence;
    private final DeltaReader reader;
    private final BooleanSupplier ingesterReachable;
    private final Consumer<CommitDelta> consumer;
    private long lastInterval = Long.MIN_VALUE;

    TierTwoChainPoller(long epoch, long currentSequence, DeltaReader reader,
            BooleanSupplier ingesterReachable, Consumer<CommitDelta> consumer) {
        if (epoch < -1 || currentSequence < -1 || (epoch == -1 && currentSequence != -1)) {
            throw new IllegalArgumentException("chain cursor is invalid");
        }
        this.epoch = epoch;
        this.currentSequence = currentSequence;
        this.reader = Objects.requireNonNull(reader, "reader");
        this.ingesterReachable = Objects.requireNonNull(ingesterReachable,
                "ingesterReachable");
        this.consumer = Objects.requireNonNull(consumer, "consumer");
    }

    synchronized void observeCursor(long observedEpoch, long observedSequence) {
        if (observedEpoch < 0 || observedSequence < 0) {
            return;
        }
        if (observedEpoch > epoch) {
            epoch = observedEpoch;
            currentSequence = observedSequence;
        } else if (observedEpoch == epoch && observedSequence > currentSequence) {
            currentSequence = observedSequence;
        }
    }

    /**
     * Runs at most one GET for a node and interval, including when it returns 404.
     *
     * <p>⚠️ THE READ RUNS OUTSIDE THIS MONITOR (M11.12, H7; M10.22 review R1).
     * {@link #observeCursor} takes it, and it is called by a catch-up delivery
     * holding a client's monitor -- which a non-emptying release needs -- so a
     * read stalled under it held that release for as long as the read lasted.
     * ⚠️ AND A DELTA THE CURSOR MOVED PAST WHILE IT WAS READ IS DROPPED: a
     * delivery already carried it, and handing it on would replay it.
     */
    void poll(long interval) {
        long readEpoch;
        long nextSequence;
        synchronized (this) {
            if (interval <= lastInterval) {
                return;
            }
            lastInterval = interval;
            if (epoch < 0 || ingesterReachable.getAsBoolean()
                    || currentSequence == Long.MAX_VALUE) {
                return;
            }
            readEpoch = epoch;
            nextSequence = currentSequence + 1;
        }
        Optional<CommitDelta> found;
        try {
            found = reader.get(readEpoch, nextSequence);
        } catch (IOException unavailable) {
            return;
        }
        if (found.isEmpty()) {
            return;
        }
        CommitDelta delta = found.orElseThrow();
        if (delta.sequence() != nextSequence) {
            throw new IllegalStateException("node-local reader returned the wrong chain delta");
        }
        synchronized (this) {
            if (epoch != readEpoch || currentSequence != nextSequence - 1) {
                return;
            }
            consumer.accept(delta);
            currentSequence = nextSequence;
        }
    }
}
