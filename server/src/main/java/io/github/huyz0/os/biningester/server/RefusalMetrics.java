// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.http.BulkService;
import io.github.huyz0.os.biningester.ingest.RefusedIndices;
import io.helidon.metrics.api.Counter;
import io.helidon.metrics.api.Metrics;
import java.util.Objects;

/**
 * The front door's {@code 429}s, exported pod-level (M12.5, M11 review F5):
 * lane admission and quota refusals under their own names, with NO label
 * (cost.md rule 16) -- which index a quota refused goes to the top-K cost line
 * through {@link RefusedIndices}, bounded there.
 *
 * <p>⚠️ Helidon's registry is process-wide and hands a second registration the
 * first counter, so in a JVM holding several pods these are shared and
 * cumulative, as {@link GovernorMetrics}'s counters are.
 */
final class RefusalMetrics implements BulkService.RefusalListener {

    static final String ADMISSION_REFUSALS = "binstore_ingest_admission_refusals_total";
    static final String QUOTA_REFUSALS = "binstore_ingest_quota_refusals_total";
    static final String REGISTRATION_WAIT_REFUSALS =
            "binstore_ingest_registration_wait_refusals_total";

    private final Counter admission;
    private final Counter quota;
    private final Counter registrationWait;
    private final RefusedIndices refused;

    RefusalMetrics(RefusedIndices refused) {
        this.refused = Objects.requireNonNull(refused, "refused");
        var registry = Metrics.globalRegistry();
        admission = registry.getOrCreate(Counter.builder(ADMISSION_REFUSALS)
                .description("Bulk requests refused 429 by the pod's in-flight budget"));
        quota = registry.getOrCreate(Counter.builder(QUOTA_REFUSALS)
                .description("Bulk requests refused 429 by an index's quota"));
        registrationWait = registry.getOrCreate(Counter.builder(REGISTRATION_WAIT_REFUSALS)
                .description("Explicit-partition bulk requests to an unregistered index refused"
                        + " 429 because as many writes already wait for registrations as may,"
                        + " for that index or across the ingester"));
    }

    @Override
    public void admissionRefused() {
        admission.increment();
    }

    @Override
    public void quotaRefused(String index) {
        quota.increment();
        refused.record(index);
    }

    @Override
    public void registrationWaitRefused() {
        registrationWait.increment();
    }
}
