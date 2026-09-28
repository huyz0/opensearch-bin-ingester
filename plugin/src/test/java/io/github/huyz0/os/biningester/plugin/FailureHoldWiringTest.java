// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

/**
 * The node's entry point hands the node hold the host's clock, so a failed
 * fetch is held per node in a running plugin and not only in a unit test
 * (M10.28): without it the hold never holds a failure.
 */
class FailureHoldWiringTest {

    @Test
    void createComponentsGivesTheNodeHoldTheThreadPoolsClock() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        SegmentSource failing = new SegmentSource() {
            @Override
            public byte[] fetch(Grant grant) throws IOException {
                calls.incrementAndGet();
                throw new IOException("503 from the store");
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                calls.incrementAndGet();
                throw new IOException("502 from the ingester");
            }
        };
        NodeSegmentSource hold = new NodeSegmentSource(failing, 1L << 20);
        NodeSubscriptions subscriptions = new NodeSubscriptions(new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        }, 16, hold);
        Settings settings = Settings.builder().put("node.name", "test-failure-hold").build();
        org.opensearch.threadpool.ThreadPool threadPool =
                new org.opensearch.threadpool.ThreadPool(settings) {
                    @Override
                    public long relativeTimeInMillis() {
                        return 5_000;
                    }
                };
        org.opensearch.cluster.service.ClusterService clusterService =
                new org.opensearch.cluster.service.ClusterService(settings,
                        new org.opensearch.common.settings.ClusterSettings(settings,
                                org.opensearch.common.settings.ClusterSettings
                                        .BUILT_IN_CLUSTER_SETTINGS),
                        threadPool) {
                    @Override
                    public void addListener(
                            org.opensearch.cluster.ClusterStateListener listener) {
                    }
                };
        try {
            BinStorePlugin.install(node -> subscriptions);
            new BinStorePlugin(settings).createComponents(null, clusterService, threadPool,
                    null, null, null, null, null, null, null, null);

            for (int run = 0; run < 4; run++) {
                assertThatThrownBy(() -> hold.fetchSegment("seg")).isInstanceOf(IOException.class);
            }
            assertThat(calls)
                    .as("⚠️ THE RUNNING NODE HOLDS THE FAILURE: four runs, one fetch")
                    .hasValue(1);
        } finally {
            BinStorePlugin.uninstall();
            subscriptions.close();
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10,
                    java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
