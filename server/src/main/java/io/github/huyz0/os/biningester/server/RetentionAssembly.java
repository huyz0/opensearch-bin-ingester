// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.ingest.LeasedGc;
import io.github.huyz0.os.biningester.ingest.OrphanSweep;
import io.github.huyz0.os.biningester.ingest.RetentionLoop;
import io.github.huyz0.os.biningester.ingest.RetentionObservable;
import io.github.huyz0.os.biningester.ingest.RetentionRule;
import io.github.huyz0.os.biningester.ingest.SegmentGc;
import io.github.huyz0.os.biningester.ingest.StoreGcLease;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LeaseTimeline;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Builds the retention loop and its independent, UID-bearing GC lease. */
final class RetentionAssembly {

    private static final System.Logger LOG = System.getLogger(RetentionAssembly.class.getName());

    @FunctionalInterface
    interface LeaseManagerFactory {
        LeaseManager create(BinStore store, LeaseConfig config, Clock clock,
                LeaseChallenge challenge, LeaseTimeline timeline);
    }

    private RetentionAssembly() {
    }

    static RetentionLoop create(ServerConfig config, BinStore store, Clock clock,
            RetentionLoop.Source source, WatermarkTable watermarks,
            LeaseManagerFactory leaseManagerFactory,
            java.util.function.BooleanSupplier discretionaryAllowed) {
        RetentionConfig kept = config.retention();
        LeaseConfig gcLease = new LeaseConfig(config.prefix() + "/gc", config.podId(),
                config.endpoint(), config.podUid(), config.leaseTtl(),
                config.leaseRenewInterval());
        LeaseManager manager = leaseManagerFactory.create(store, gcLease, clock,
                LeaseChallenge.NEVER, LeaseTimeline.NONE);
        LeasedGc leased = new LeasedGc(new StoreGcLease(manager, clock), store);
        RetentionRule rule = new RetentionRule(clock, kept.minRetention(), kept.maxRetention(),
                RetentionRule.DEFAULT_SAFETY_MARGIN,
                (segmentKey, age) -> LOG.log(System.Logger.Level.ERROR,
                        () -> "a segment " + segmentKey
                                + " was deleted at the retention ceiling, " + age
                                + " old: a consumer had not read it and has lost data"),
                watermarks::of);
        RetentionObservable observable = new RetentionObservable(clock, kept.minRetention(),
                kept.maxRetention(), kept.reportTimeout(),
                alarm -> LOG.log(System.Logger.Level.WARNING, () -> "retention alarm "
                                + alarm.kind() + " on " + alarm.stream() + ": " + alarm.detail()));
        return new RetentionLoop(source, leased, rule, observable, clock, config.prefix(),
                kept.minRetention(), OrphanSweep.DEFAULT_GRACE, SegmentGc.DEFAULT_DELETE_BATCH,
                discretionaryAllowed);
    }

    /**
     * Runs the retention loop on its own virtual thread every
     * {@code passInterval}, and returns what stops it (extracted from
     * {@code Assembly} by M11.1, unchanged).
     */
    static AutoCloseable schedule(RetentionLoop retention, Duration passInterval) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("retention").factory());
        long every = passInterval.toNanos();
        // ⚠️ FIXED DELAY, NOT FIXED RATE, AND THE FIRST TICK IS ONE INTERVAL
        // IN. A slow pass must not queue a burst of catch-up passes behind it,
        // and a node that has just started owns nothing old enough to collect.
        scheduler.scheduleWithFixedDelay(() -> tickQuietly(retention), every, every,
                TimeUnit.NANOSECONDS);
        return () -> {
            scheduler.shutdownNow();
            // ⚠️ BOUNDED. A pass blocked on a store call is interrupted by
            // `shutdownNow`; one that is not answers within the bound or is
            // abandoned, because a shutdown that hung on GC would hold the
            // SEQUENCER term too -- and that is the lease whose release this
            // whole sequence exists to reach.
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        };
    }

    /**
     * ⚠️ **A THROW OUT OF A SCHEDULED TASK CANCELS EVERY LATER RUN OF IT**, with
     * nothing logged and nothing to notice: GC would stop on this node for good
     * and storage would grow until someone looked at a bill. The loop already
     * contains its own failures; this is the belt to that brace.
     */
    private static void tickQuietly(RetentionLoop retention) {
        try {
            retention.tick();
        } catch (RuntimeException failed) {
            TICK_LOG.log(System.Logger.Level.WARNING, () -> "a retention tick failed; the next one "
                    + "runs on schedule: " + failed);
        }
    }

    // ⚠️ ASSEMBLY'S LOGGER, AS BEFORE M11.1 moved this here: an operator's
    // filter on the logger name keeps matching.
    private static final System.Logger TICK_LOG = System.getLogger(Assembly.class.getName());
}
