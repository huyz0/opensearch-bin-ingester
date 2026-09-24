// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.common.settings.Settings;
import org.opensearch.cluster.ClusterState;

/** Test fixture for the node-level catch-up scheduler wiring. */
final class CatchUpSchedulingWiringTest {
    private CatchUpSchedulingWiringTest() {}

    static void verifySchedulingAndRetry() {
        var transport = new CatchUpTransport();
        var subscriptions = new NodeSubscriptions(transport, 16);
        var key = new RunKey(java.util.UUID.fromString("00000000-0000-0000-0000-0000000000ac"), 0);
        subscriptions.clientFor(key);
        var positions = new MutableShardPositions();
        var settings = Settings.builder().put("node.name", "test-catch-up").build();
        var intervals = new ArrayList<org.opensearch.common.unit.TimeValue>();
        var scheduled = new ArrayList<Runnable>();
        var generic = new ManualExecutor();
        var threadPool = new org.opensearch.threadpool.ThreadPool(settings) {
            @Override
            public java.util.concurrent.ExecutorService generic() {
                return generic;
            }

            @Override
            public org.opensearch.threadpool.Scheduler.Cancellable scheduleWithFixedDelay(
                    Runnable command, org.opensearch.common.unit.TimeValue interval, String executor) {
                intervals.add(interval);
                scheduled.add(command);
                return new org.opensearch.threadpool.Scheduler.Cancellable() {
                    @Override public boolean cancel() { return true; }
                    @Override public boolean isCancelled() { return false; }
                };
            }
        };
        var clusterService = new org.opensearch.cluster.service.ClusterService(settings,
                new org.opensearch.common.settings.ClusterSettings(settings,
                        org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool) {
            @Override public ClusterState state() { return ClusterState.EMPTY_STATE; }
            @Override public void addListener(org.opensearch.cluster.ClusterStateListener listener) { }
        };
        try {
            BinStorePlugin.install(node -> subscriptions);
            new BinStorePlugin(subscriptions, positions).createComponents(null, clusterService, threadPool,
                    null, null, null, null, null, null, null, null);

            assertThat(intervals).containsExactly(
                    BinStorePlugin.PROGRESS_INTERVAL.get(settings),
                    BinStorePlugin.PROGRESS_INTERVAL.get(settings));
            assertThat(generic.tasks).hasSize(1);
            generic.tasks.get(0).run();
            scheduled.forEach(Runnable::run);
            assertThat(transport.requests).isZero();

            positions.ready = java.util.Optional.of(List.of(
                    new io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream(key, 0)));
            scheduled.forEach(Runnable::run);
            assertThat(transport.requests).isEqualTo(1);
        } finally {
            BinStorePlugin.uninstall();
            subscriptions.close();
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10,
                    java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static final class MutableShardPositions extends ShardPositions {
        private java.util.Optional<List<io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>>
                ready = java.util.Optional.empty();

        @Override
        java.util.Optional<List<io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>>
                catchUpSnapshot(ClusterState state) {
            return ready;
        }
    }

    private static final class ManualExecutor extends java.util.concurrent.AbstractExecutorService {
        private final List<Runnable> tasks = new ArrayList<>();

        @Override public void shutdown() { }
        @Override public List<Runnable> shutdownNow() { return List.of(); }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) {
            return true;
        }
        @Override public void execute(Runnable command) { tasks.add(command); }
    }

    private static final class CatchUpTransport implements SubscriptionTransport {
        private int requests;

        @Override public AutoCloseable subscribe(RunKey key, Listener listener) { return () -> { }; }
        @Override public void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) { }
        @Override
        public CatchUpResult requestCatchUp(io.github.huyz0.os.biningester.format.CatchUpRequestFrame request,
                java.util.function.Consumer<io.github.huyz0.os.biningester.format.SubscriptionEvent> lane) {
            requests++;
            return CatchUpResult.UNSUPPORTED;
        }
    }
}
