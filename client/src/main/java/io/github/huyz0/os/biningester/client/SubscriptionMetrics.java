// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongConsumer;

/** Process-local cumulative counters for subscription and progress diagnostics. */
public final class SubscriptionMetrics {

    public enum Counter {
        FALLBACK_TIER_PUSH_ENTRIES,
        FALLBACK_TIER_RECONNECT_ENTRIES,
        FALLBACK_TIER_POLL_CHAIN_ENTRIES,
        FALLBACK_TIER_RECOVER_ENTRIES,
        SUBSCRIPTION_RECONNECTS,
        POLL_FAILURE_UNAVAILABLE,
        POLL_FAILURE_REFUSED,
        POLL_FAILURE_SERVER_ERROR,
        POLL_FAILURE_UNREACHABLE,
        POLL_FAILURE_MALFORMED,
        POLL_FAILURE_CALLBACK,
        PROGRESS_PUSH_FAILURES,
        /** A segment too large for the node's hold, fetched (M10.25). */
        SEGMENT_HOLD_OVERSIZE_FETCHES,
        /** A segment the node's hold evicted, fetched again (M10.25). */
        SEGMENT_HOLD_REFETCHES_AFTER_EVICTION
    }

    private final EnumMap<Counter, Long> values = new EnumMap<>(Counter.class);
    private Map<Counter, LongConsumer> listeners = Map.of();
    private boolean installed;

    public SubscriptionMetrics() {
        for (Counter counter : Counter.values()) {
            values.put(counter, 0L);
        }
    }

    /** Increments the process-local source and any listener installed by the host runtime. */
    public synchronized void increment(Counter counter) {
        Objects.requireNonNull(counter, "counter");
        values.put(counter, values.get(counter) + 1);
        LongConsumer listener = listeners.get(counter);
        if (listener != null) {
            listener.accept(1);
        }
    }

    public synchronized long count(Counter counter) {
        return values.get(Objects.requireNonNull(counter, "counter"));
    }

    /**
     * Seeds host counters from current source totals, then attaches event listeners as one
     * synchronized handoff. Source events cannot fall between the seed and listener install.
     */
    public synchronized void install(Map<Counter, LongConsumer> sinks) {
        if (installed) {
            throw new IllegalStateException("subscription metric sinks are already installed");
        }
        EnumMap<Counter, LongConsumer> checked = new EnumMap<>(Counter.class);
        sinks.forEach((counter, sink) -> checked.put(
                Objects.requireNonNull(counter, "counter"),
                Objects.requireNonNull(sink, "sink")));
        checked.forEach((counter, sink) -> sink.accept(values.get(counter)));
        listeners = Map.copyOf(checked);
        installed = true;
    }
}
