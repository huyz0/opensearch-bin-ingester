// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionMetrics;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

class PluginMetricRegistryTest {

    private static final Map<String, Double> COUNTERS = Map.ofEntries(
            Map.entry("biningester_fallback_tier_push_entries_total", 1.0),
            Map.entry("biningester_fallback_tier_reconnect_entries_total", 2.0),
            Map.entry("biningester_fallback_tier_poll_chain_entries_total", 3.0),
            Map.entry("biningester_fallback_tier_recover_entries_total", 4.0),
            Map.entry("biningester_subscription_reconnects_total", 5.0),
            Map.entry("biningester_subscription_poll_failure_unavailable_total", 6.0),
            Map.entry("biningester_subscription_poll_failure_refused_total", 7.0),
            Map.entry("biningester_subscription_poll_failure_server_error_total", 8.0),
            Map.entry("biningester_subscription_poll_failure_unreachable_total", 9.0),
            Map.entry("biningester_subscription_poll_failure_malformed_total", 10.0),
            Map.entry("biningester_subscription_poll_failure_callback_total", 11.0),
            Map.entry("biningester_progress_push_failures_total", 12.0));

    @Test
    void exportsEveryFixedPluginCounterAndTheLiveTierGaugeWithoutTags() {
        NodeSubscriptions subscriptions = new NodeSubscriptions(new BareTransport(), 8);
        var source = subscriptions.metrics();
        int value = 1;
        for (SubscriptionMetrics.Counter counter : SubscriptionMetrics.Counter.values()) {
            for (int increment = 0; increment < value; increment++) {
                source.increment(counter);
            }
            value++;
        }
        BinStorePlugin plugin = new BinStorePlugin(subscriptions);
        Map<String, Double> actualCounters = new LinkedHashMap<>();
        Map<String, Supplier<Double>> gauges = new LinkedHashMap<>();
        AtomicReference<Boolean> allUnlabelled = new AtomicReference<>(true);
        MetricsRegistry registry = registry(actualCounters, gauges, allUnlabelled);

        Object gaugeRegistration = plugin.registerMetrics(registry);

        assertThat(actualCounters).containsExactlyInAnyOrderEntriesOf(COUNTERS);
        assertThat(gauges).containsOnlyKeys("biningester_fallback_current_tier");
        assertThat(gauges.get("biningester_fallback_current_tier").get()).isEqualTo(1.0);
        assertThat(allUnlabelled.get()).isTrue();
        assertThat(gaugeRegistration).isNotNull();

        source.increment(SubscriptionMetrics.Counter.SUBSCRIPTION_RECONNECTS);
        assertThat(actualCounters.get("biningester_subscription_reconnects_total"))
                .isEqualTo(6.0);
    }

    private static MetricsRegistry registry(Map<String, Double> counters,
            Map<String, Supplier<Double>> gauges, AtomicReference<Boolean> allUnlabelled) {
        return (MetricsRegistry) Proxy.newProxyInstance(MetricsRegistry.class.getClassLoader(),
                new Class<?>[] {MetricsRegistry.class}, (proxy, method, args) -> {
                    String name = (String) args[0];
                    if (method.getName().equals("createCounter")) {
                        AtomicReference<Double> count = new AtomicReference<>(0.0);
                        counters.put(name, 0.0);
                        return Proxy.newProxyInstance(Counter.class.getClassLoader(),
                                new Class<?>[] {Counter.class}, (counter, operation, values) -> {
                                    if (operation.getName().equals("add")) {
                                        count.updateAndGet(current -> current + (double) values[0]);
                                        counters.put(name, count.get());
                                    }
                                    return null;
                                });
                    }
                    if (method.getName().equals("createGauge")) {
                        @SuppressWarnings("unchecked")
                        Supplier<Double> value = (Supplier<Double>) args[3];
                        gauges.put(name, value);
                        allUnlabelled.set(allUnlabelled.get()
                                && ((Tags) args[4]).size() == 0);
                        return Proxy.newProxyInstance(java.io.Closeable.class.getClassLoader(),
                                new Class<?>[] {java.io.Closeable.class},
                                (handle, operation, values) -> null);
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static final class BareTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }
    }
}
