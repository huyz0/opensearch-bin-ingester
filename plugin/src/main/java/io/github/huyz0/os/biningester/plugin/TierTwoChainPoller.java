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

    /** Runs at most one GET for a node and interval, including when it returns 404. */
    synchronized void poll(long interval) {
        if (interval <= lastInterval) {
            return;
        }
        lastInterval = interval;
        if (epoch < 0 || ingesterReachable.getAsBoolean()
                || currentSequence == Long.MAX_VALUE) {
            return;
        }
        long nextSequence = currentSequence + 1;
        Optional<CommitDelta> found;
        try {
            found = reader.get(epoch, nextSequence);
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
        consumer.accept(delta);
        currentSequence = nextSequence;
    }
}
