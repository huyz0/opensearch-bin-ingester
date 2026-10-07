// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.helidon.metrics.api.Counter;
import io.helidon.metrics.api.Metrics;

/** The fixed, process-local ingester counters exported by Helidon's observer. */
final class IngesterMetrics {

    static final String ENDPOINTSLICE_WATCH_FAILURES =
            "biningester_endpointslice_watch_failures_total";
    static final String STUCK_INTENT_ATTEMPTS =
            "biningester_inbox_stuck_intent_attempts_total";
    /**
     * Durable-segment hints whose post failed (M13.70): each is a cache miss
     * and nothing worse, so it is swallowed -- and this is the only trace a
     * fleet losing every one of them leaves (a rolling {@code peer.tls}
     * change, a broken trust).
     */
    static final String DURABLE_HINTS_LOST = "biningester_durable_segment_hints_lost_total";

    private final Counter endpointSliceWatchFailures;
    private final Counter stuckIntentAttempts;
    private final Counter durableHintsLost;

    IngesterMetrics() {
        var registry = Metrics.globalRegistry();
        endpointSliceWatchFailures = registry.getOrCreate(Counter
                .builder(ENDPOINTSLICE_WATCH_FAILURES)
                .description("EndpointSlice watch connection and stream failures"));
        stuckIntentAttempts = registry.getOrCreate(Counter
                .builder(STUCK_INTENT_ATTEMPTS)
                .description("Failed inbox intent application attempts; retries count again"));
        durableHintsLost = registry.getOrCreate(Counter
                .builder(DURABLE_HINTS_LOST)
                .description("Durable-segment hints whose post to a peer failed"));
    }

    void durableHintLost() {
        durableHintsLost.increment();
    }

    long durableHintsLost() {
        return durableHintsLost.count();
    }

    void endpointSliceWatchFailed() {
        endpointSliceWatchFailures.increment();
    }

    void failedIntentBatch(long batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("failed intent batch size must be non-negative");
        }
        stuckIntentAttempts.increment(batchSize);
    }

    long endpointSliceWatchFailures() {
        return endpointSliceWatchFailures.count();
    }

    long stuckIntentAttempts() {
        return stuckIntentAttempts.count();
    }
}
