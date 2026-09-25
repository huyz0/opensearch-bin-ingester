// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.opensearch.plugins.TelemetryAwarePlugin;

class PluginMetricExportTest {

    @Test
    void pluginUsesOpenSearchTelemetryLifecycle() {
        assertThat(BinStorePlugin.class).isAssignableTo(TelemetryAwarePlugin.class);
    }
}
