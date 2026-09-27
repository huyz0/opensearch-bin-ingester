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
import java.time.Clock;
import java.util.Objects;

/** Builds the retention loop and its independent, UID-bearing GC lease. */
final class RetentionAssembly {

    private static final System.Logger LOG = System.getLogger(RetentionAssembly.class.getName());

    @FunctionalInterface
    interface LeaseManagerFactory {
        LeaseManager create(BinStore store, LeaseConfig config, Clock clock,
                LeaseChallenge challenge);
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
                LeaseChallenge.NEVER);
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
}
