// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.helidon.metrics.api.Counter;
import io.helidon.metrics.api.Gauge;
import io.helidon.metrics.api.Metrics;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The pod's cost governor, exported (M10.27, cost.md rule 17, observability.md
 * rule 4): without these, NFR-16's zero is asserted in tests and cannot be
 * observed on a fleet.
 *
 * <p>⚠️ **NO LABELS.** Research 15 §4 names {@code binstore_governor_refusals_total{class}},
 * but {@code class} is not on observability.md rule 1's closed allow-list, so
 * the two classes are two metrics and the front-page tile is their sum. Each is
 * one series: the refusals are the pod's, and a pod serves one trust domain.
 *
 * <p>⚠️ **THE GAUGES READ THE MOST RECENTLY BOUND GOVERNOR.** Helidon's registry
 * is process-wide and hands a second registration the first meter, so a gauge
 * bound to one governor would read that governor for ever. A process runs one
 * pod, so "most recently bound" is "the pod's"; in a test JVM assembling
 * several pods it is the last one. The counters are shared and cumulative, as
 * {@link IngesterMetrics}' are.
 */
final class GovernorMetrics {

    static final String REFUSALS = "binstore_governor_refusals_total";
    static final String LIST_REFUSALS = "binstore_governor_list_refusals_total";
    static final String DISCRETIONARY_REFUSALS =
            "binstore_governor_discretionary_refusals_total";
    static final String RATIO = "binstore_governor_ratio_to_expected";
    static final String ALARM = "binstore_governor_alarm";
    static final String KILL_SWITCH = "binstore_governor_kill_switch";

    private static final AtomicReference<CostGovernor> BOUND = new AtomicReference<>();

    private final Counter refusals;
    private final Counter listRefusals;
    private final Counter discretionaryRefusals;

    private GovernorMetrics() {
        var registry = Metrics.globalRegistry();
        refusals = registry.getOrCreate(Counter.builder(REFUSALS)
                .description("Cost governor refusals of every class; zero in steady state"));
        listRefusals = registry.getOrCreate(Counter.builder(LIST_REFUSALS)
                .description("Undeclared LISTs refused past the ceiling"));
        discretionaryRefusals = registry.getOrCreate(Counter.builder(DISCRETIONARY_REFUSALS)
                .description("Discretionary work (sweeps, prefetch) refused while halted"));
        registry.getOrCreate(Gauge.builder(RATIO, () -> read(CostGovernor::lastRatio))
                .description("Data PUTs over expected in the last completed window"));
        registry.getOrCreate(Gauge.builder(ALARM,
                () -> read(g -> g.alarmed() ? 1.0 : 0.0))
                .description("1 while the last window was at or above the 3x alarm"));
        registry.getOrCreate(Gauge.builder(KILL_SWITCH,
                () -> read(g -> g.killSwitchTripped() ? 1.0 : 0.0))
                .description("1 once the 100x kill switch has tripped, until reset"));
    }

    /**
     * Exports {@code governor}: its refusals counted as they happen, and the
     * gauges reading it from now on.
     */
    static GovernorMetrics bind(CostGovernor governor) {
        Objects.requireNonNull(governor, "governor");
        GovernorMetrics metrics = new GovernorMetrics();
        BOUND.set(governor);
        governor.onRefusal(new CostGovernor.RefusalListener() {
            @Override
            public void listRefused() {
                metrics.listRefusals.increment();
                metrics.refusals.increment();
            }

            @Override
            public void discretionaryRefused() {
                metrics.discretionaryRefusals.increment();
                metrics.refusals.increment();
            }
        });
        return metrics;
    }

    private static Double read(java.util.function.ToDoubleFunction<CostGovernor> gauge) {
        CostGovernor governor = BOUND.get();
        return governor == null ? 0.0 : gauge.applyAsDouble(governor);
    }
}
