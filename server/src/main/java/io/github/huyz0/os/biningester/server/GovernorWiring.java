// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import java.time.Clock;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * How the composition root builds the pod's one cost governor (M10.11,
 * ADR-0075).
 *
 * <p>⚠️ **ONE PER POD, BUILT BEFORE THE STORE THE GRAPH USES**, because the
 * governor sits in that store stack: every caller HANDED the node's store,
 * including one not yet written, is governed by construction rather than by
 * remembering to ask.
 *
 * <p>⚠️ **NOT EVERY CALLER IS HANDED IT.** {@code Assembly.store()} returns the
 * RAW backend, and {@code FrontDoor} passes that to the commit route's inbox
 * drain and to the segment-fetch route's GETs: those requests are neither
 * governed nor counted. Routing them through the node's store is M10.26.
 */
final class GovernorWiring {

    /** Builds the governor; a seam so a case can hand the root a drained one. */
    @FunctionalInterface
    interface GovernorFactory {
        CostGovernor create(ServerConfig config, Clock clock, LongSupplier flushSpacingMillis);
    }

    /** ADR-0075's defaults, sized against the configured segment. */
    static final GovernorFactory DEFAULT = (config, clock, spacing) -> new CostGovernor(
            CostGovernor.Settings.defaults(config.ingest().maxSegmentBytes()), clock, spacing);

    private GovernorWiring() {
    }

    /**
     * The flush spacing in force, read from the pod's ingest.
     *
     * <p>⚠️ **THE INGEST IS BUILT AFTER THE STORE**, and the store stack holds
     * the governor, so this answers the interval FLOOR until the ingest is
     * attached. The floor is the SHORTEST spacing, so the expected PUT count
     * is at its largest and no startup flush can read as a regression.
     */
    static final class Spacing implements LongSupplier {
        private final long floorMillis;
        private volatile DefaultIngest ingest;

        Spacing(ServerConfig config) {
            this.floorMillis = config.ingest().intervalFloor().toMillis();
        }

        void attach(DefaultIngest attached) {
            this.ingest = Objects.requireNonNull(attached, "ingest");
        }

        @Override
        public long getAsLong() {
            DefaultIngest current = ingest;
            return current == null ? floorMillis : current.flushSpacingMillis();
        }
    }
}
