// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.client.SubscriptionTransport.CatchUpResult;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.Optional;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.junit.jupiter.api.Test;

class NodeCatchUpDeliveryTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000ab");

    private static final class FakeTransport implements SubscriptionTransport {
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicReference<CatchUpRequestFrame> request = new AtomicReference<>();
        private CatchUpResult result = CatchUpResult.COMPLETE;
        private boolean failOnce;
        private List<SubscriptionEvent> events = List.of();
        private Runnable beforeEvents = () -> { };

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame frame,
                java.util.function.Consumer<io.github.huyz0.os.biningester.format.SubscriptionEvent> lane)
                throws IOException {
            requests.incrementAndGet();
            request.set(frame);
            beforeEvents.run();
            events.forEach(lane);
            if (failOnce) {
                failOnce = false;
                throw new IOException("temporary transport failure");
            }
            return result;
        }
    }

    @Test
    void waitsForCompleteSnapshotIncludingLateShardAndSendsOneNodeRequest() {
        FakeTransport transport = new FakeTransport();
        AtomicReference<Optional<List<CatchUpRequestFrame.Stream>>> positions =
                new AtomicReference<>(Optional.empty());
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            clients.clientFor(new RunKey(INDEX, 0));
            clients.clientFor(new RunKey(INDEX, 1));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, clients, positions::get);

            coordinator.attempt();
            assertThat(transport.requests.get()).isZero();

            // Shard 1 started later and has never committed; it is still part
            // of the all-local-shards snapshot, at offset zero.
            positions.set(Optional.of(List.of(
                    new CatchUpRequestFrame.Stream(new RunKey(INDEX, 0), 42),
                    new CatchUpRequestFrame.Stream(new RunKey(INDEX, 1), 0))));
            coordinator.attempt();

            assertThat(transport.requests.get()).isEqualTo(1);
            assertThat(transport.request.get().streams()).containsExactly(
                    new CatchUpRequestFrame.Stream(new RunKey(INDEX, 0), 42),
                    new CatchUpRequestFrame.Stream(new RunKey(INDEX, 1), 0));
            coordinator.attempt();
            assertThat(transport.requests.get()).isEqualTo(1);
        }
    }

    @Test
    void unreadableSnapshotRetriesAndUnsupportedPeerKeepsLiveSubscription() {
        FakeTransport transport = new FakeTransport();
        AtomicReference<Optional<List<CatchUpRequestFrame.Stream>>> positions =
                new AtomicReference<>(Optional.empty());
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            clients.clientFor(new RunKey(INDEX, 0));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, clients, positions::get);
            coordinator.attempt();
            assertThat(transport.requests.get()).isZero();

            positions.set(Optional.of(List.of(
                    new CatchUpRequestFrame.Stream(new RunKey(INDEX, 0), 0))));
            transport.result = CatchUpResult.UNSUPPORTED;
            coordinator.attempt();
            coordinator.attempt();

            assertThat(transport.requests.get()).isEqualTo(1);
            assertThat(clients.openClients()).isEqualTo(1);
        }
    }

    @Test
    void retriesTransientTransportFailureWithSameSnapshot() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.failOnce = true;
        RunKey key = new RunKey(INDEX, 0);
        transport.events = List.of(event(key, segment(key)));
        AtomicReference<Optional<List<CatchUpRequestFrame.Stream>>> positions =
                new AtomicReference<>(Optional.of(List.of(
                        new CatchUpRequestFrame.Stream(key, 7))));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8, grant -> {
            throw new AssertionError("inline replay retry must not fetch the object");
        })) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, clients, positions::get);
            coordinator.attempt();
            UUID requestId = transport.request.get().requestId();
            coordinator.attempt();

            assertThat(transport.requests.get()).isEqualTo(2);
            assertThat(transport.request.get().requestId()).isEqualTo(requestId);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(7);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(requestId)).isTrue();
        }
    }

    @Test
    void routesOnlyRequestedReplayToHeldClientsWithoutFetchingStore() throws Exception {
        FakeTransport transport = new FakeTransport();
        RunKey held = new RunKey(INDEX, 0);
        RunKey unheld = new RunKey(INDEX, 1);
        byte[] segment = segment(held);
        transport.events = List.of(event(held, segment), event(unheld, segment));
        AtomicInteger sourceCalls = new AtomicInteger();
        AtomicReference<Optional<List<CatchUpRequestFrame.Stream>>> positions =
                new AtomicReference<>(Optional.of(List.of(
                        new CatchUpRequestFrame.Stream(held, 7))));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8,
                grant -> {
                    sourceCalls.incrementAndGet();
                    throw new AssertionError("inline catch-up must not fetch an object");
        })) {
            var client = clients.clientFor(held);
            var released = clients.clientFor(unheld);
            transport.beforeEvents = () -> clients.release(unheld);
            positions.set(Optional.of(List.of(
                    new CatchUpRequestFrame.Stream(held, 7),
                    new CatchUpRequestFrame.Stream(unheld, 7))));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, clients, positions::get);
            coordinator.attempt();

            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(7);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(released.readNext(Duration.ZERO)).isEmpty();
            assertThat(clients.deliverCatchUp(transport.request.get().requestId(),
                    event(unheld, segment))).isFalse();
            assertThat(sourceCalls.get()).isZero();
            assertThat(clients.openClients()).isEqualTo(1);
        }
    }

    @Test
    void releaseBetweenClientLookupAndCatchUpLockPreventsStaleDelivery() throws Exception {
        FakeTransport transport = new FakeTransport();
        RunKey key = new RunKey(INDEX, 0);
        UUID requestId = UUID.randomUUID();
        byte[] bytes = segment(key);
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            var client = clients.clientFor(key);
            AtomicReference<Boolean> routed = new AtomicReference<>();
            java.util.concurrent.CountDownLatch entering =
                    new java.util.concurrent.CountDownLatch(1);
            Thread callback;
            synchronized (client) {
                callback = Thread.ofVirtual().start(() -> {
                    entering.countDown();
                    routed.set(clients.deliverCatchUp(requestId,
                            event(key, bytes)));
                });
                assertThat(entering.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(awaits(callback, Thread.State.BLOCKED, Duration.ofSeconds(5)))
                        .as("delivery has read the held client and is waiting on its lifecycle lock")
                        .isTrue();
                clients.release(key);
            }
            callback.join();

            assertThat(routed.get()).isFalse();
            assertThat(clients.openClients()).isZero();
        }
    }

    private static boolean awaits(Thread thread, Thread.State expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (thread.getState() != expected && System.nanoTime() < deadline) {
            Thread.yield();
        }
        return thread.getState() == expected;
    }

    @Test
    void routingSnapshotWaitsForStartedShardsAndDistinguishesZeroFromUnreadable() {
        ClusterState initializing = routingState(false);
        assertThat(ShardPositions.catchUpSnapshot(initializing, route ->
                java.util.OptionalLong.of(route.id() == 0 ? 42 : 0))).isEmpty();

        ClusterState started = routingState(true);
        assertThat(ShardPositions.catchUpSnapshot(started, route ->
                java.util.OptionalLong.of(route.id() == 0 ? 42 : 0)))
                .contains(List.of(new CatchUpRequestFrame.Stream(new RunKey(INDEX, 0), 42),
                        new CatchUpRequestFrame.Stream(new RunKey(INDEX, 1), 0)));
        assertThat(ShardPositions.catchUpSnapshot(started, route ->
                route.id() == 1 ? java.util.OptionalLong.empty()
                        : java.util.OptionalLong.of(42))).isEmpty();
    }

    private static ClusterState routingState(boolean started) {
        IndexMetadata metadata = IndexMetadata.builder("logs")
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.uuid", INDEX.toString())
                        .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE))
                .build();
        IndexRoutingTable.Builder index = IndexRoutingTable.builder(metadata.getIndex());
        for (int shardNumber = 0; shardNumber < 2; shardNumber++) {
            ShardId id = new ShardId(metadata.getIndex(), shardNumber);
            ShardRouting route = ShardRouting.newUnassigned(id, true,
                            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                    .initialize("node-a", null, 0L);
            if (started) {
                route = route.moveToStarted();
            }
            index.addIndexShard(new IndexShardRoutingTable.Builder(id).addShard(route).build());
        }
        var node = new org.opensearch.cluster.node.DiscoveryNode("node-a",
                new org.opensearch.core.common.transport.TransportAddress(
                        java.net.InetAddress.getLoopbackAddress(), 9300), java.util.Map.of(),
                java.util.Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(metadata, false).build())
                .routingTable(RoutingTable.builder().add(index.build()).build())
                .nodes(DiscoveryNodes.builder().localNodeId("node-a").add(node).build())
                .build();
    }

    private static SubscriptionEvent event(RunKey key, byte[] segment) {
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment", 7, 1,
                FetchMode.INLINE, segment);
    }

    private static byte[] segment(RunKey key) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                "{\"id\":\"doc\"}".getBytes(StandardCharsets.UTF_8)), 7L);
        return writer.toByteArray(11L);
    }
}
