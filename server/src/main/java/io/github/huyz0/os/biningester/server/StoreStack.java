// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import io.github.huyz0.os.biningester.binstore.HealthTrackingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.ingest.CommitChargingBinStore;
import java.time.Clock;

/**
 * The pod's store stack, bottom up: the counter over the raw backend, the
 * commit charge on the counter, the governor above both, the health tracker on
 * top -- with the governor, its metrics and the pod's one cost ledger that the
 * stack charges into. Moved out of {@link Assembly} unchanged by M11.24b.
 *
 * @param counting the counter over the raw backend
 * @param governor the pod's one cost governor
 * @param governorMetrics the governor's exported series (M10.27)
 * @param costLedger each index's apportioned share of this pod's requests
 * @param health the top of the stack: what every call of the node goes through
 * @param spacing the governor's flush-spacing source, attached to the ingest later
 */
record StoreStack(CountingBinStore counting, CostGovernor governor,
        GovernorMetrics governorMetrics, IndexCostLedger costLedger,
        HealthTrackingBinStore health, GovernorWiring.Spacing spacing) {

    static StoreStack over(BinStore raw, ServerConfig config, Clock clock,
            GovernorWiring.GovernorFactory governorFactory) {
        CountingBinStore counting = new CountingBinStore(raw);
        // ⚠️ THE GOVERNOR SITS ABOVE THE COUNTER (M10.11, ADR-0075), so a LIST it
        // refuses never reached the store and is never counted as a request.
        GovernorWiring.Spacing spacing = new GovernorWiring.Spacing(config);
        CostGovernor governor = governorFactory.create(config, clock, spacing);
        GovernorMetrics governorMetrics = GovernorMetrics.bind(governor);
        // ⚠️ THE COMMIT CHARGE SITS ON THE COUNTER (M11.22): every commit-log
        // PUT this pod counts is charged to the indices of the delta it carries.
        IndexCostLedger costLedger = new IndexCostLedger();
        HealthTrackingBinStore health = new HealthTrackingBinStore(new GoverningBinStore(
                new CommitChargingBinStore(counting, costLedger), governor), clock,
                HealthTrackingBinStore.DEFAULT_STALL, HealthTrackingBinStore.DEFAULT_FAILURES);
        return new StoreStack(counting, governor, governorMetrics, costLedger, health, spacing);
    }
}
