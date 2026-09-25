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

    private final Counter endpointSliceWatchFailures;
    private final Counter stuckIntentAttempts;

    IngesterMetrics() {
        var registry = Metrics.globalRegistry();
        endpointSliceWatchFailures = registry.getOrCreate(Counter
                .builder(ENDPOINTSLICE_WATCH_FAILURES)
                .description("EndpointSlice watch connection and stream failures"));
        stuckIntentAttempts = registry.getOrCreate(Counter
                .builder(STUCK_INTENT_ATTEMPTS)
                .description("Failed inbox intent application attempts; retries count again"));
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
