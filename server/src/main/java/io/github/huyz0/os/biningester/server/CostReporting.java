// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.RefusedIndices;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the pod's top-K cost line on its own virtual thread (M11.5).
 *
 * <p>⚠️ **THE SCHEDULE ONLY WAKES IT; THE REPORTER's CLOCK DECIDES.** A tick
 * that arrives early emits nothing, so the line's cadence is the reporter's
 * and a test drives it with an injected clock rather than with this thread.
 */
final class CostReporting {

    private static final System.Logger LOG = System.getLogger(CostReporting.class.getName());

    /** Where the top-K cost line is logged: its own name, so it can be routed. */
    private static final System.Logger COST_LOG = System.getLogger("binstore.cost");

    private CostReporting() {
    }

    /**
     * M11.5: the pod's top-K cost line, from the ledger this pod's stores
     * charge, scheduled at {@code interval} (moved out of {@code Assembly} by
     * M11.24b).
     */
    static AutoCloseable scheduleTopK(IndexCostLedger costLedger, IndexCatalog catalog,
            CostTable costs, java.time.Duration interval, Clock clock, RefusedIndices refused) {
        return schedule(new CostTopKReporter(costLedger, catalog::namesById, costs, interval,
                clock, line -> COST_LOG.log(System.Logger.Level.INFO, line), refused), interval);
    }

    /** Ticks {@code reporter} every interval; what it returns stops it. A disabled one costs no thread. */
    static AutoCloseable schedule(CostTopKReporter reporter, java.time.Duration interval) {
        Objects.requireNonNull(reporter, "reporter");
        if (!reporter.enabled()) {
            return () -> { };
        }
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("cost-top-k").factory());
        long every = interval.toNanos();
        // ⚠️ A THROW OUT OF A SCHEDULED TASK CANCELS EVERY LATER RUN, silently,
        // as RetentionAssembly's tick says: caught and logged, the next runs.
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                reporter.tick();
            } catch (RuntimeException failed) {
                LOG.log(System.Logger.Level.WARNING,
                        () -> "a cost report tick failed; the next runs on schedule: " + failed);
            }
        }, every, every, TimeUnit.NANOSECONDS);
        return () -> {
            scheduler.shutdownNow();
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        };
    }
}
