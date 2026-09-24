// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import java.util.UUID;
import org.junit.jupiter.api.Test;
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

class ShardPositionsCatchUpLimitTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-000000000010");

    @Test
    void snapshotRetainsStreamsAboveTheOriginalV1RequestLimit() {
        assertThat(ShardPositions.catchUpSnapshot(routingState(CatchUpRequestFrame.MAX_STREAMS_V1),
                route -> java.util.OptionalLong.of(0)))
                .hasValueSatisfying(streams -> assertThat(streams)
                        .hasSize(CatchUpRequestFrame.MAX_STREAMS_V1));
        assertThat(ShardPositions.catchUpSnapshot(
                routingState(CatchUpRequestFrame.MAX_STREAMS_V1 + 1),
                route -> java.util.OptionalLong.of(0)))
                .hasValueSatisfying(streams -> assertThat(streams)
                        .hasSize(CatchUpRequestFrame.MAX_STREAMS_V1 + 1));
    }

    @Test
    void shardWithoutPersistedBatchStartBeginsCatchUpAtZero() {
        assertThat(ShardPositions.catchUpBatchStart(null)).isZero();
        assertThat(ShardPositions.catchUpBatchStart(new BinStoreOffset(37).asString()))
                .isEqualTo(37);
    }

    private static ClusterState routingState(int shardCount) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();
        int remaining = shardCount;
        int indexNumber = 0;
        while (remaining > 0) {
            int count = Math.min(remaining, 1024);
            UUID indexUuid = indexNumber == 0 ? INDEX : new UUID(0L, indexNumber + 1L);
            IndexMetadata indexMetadata = IndexMetadata.builder("bounded-catch-up-" + indexNumber)
                    .settings(Settings.builder()
                            .put("index.version.created", Version.CURRENT.id)
                            .put("index.number_of_shards", count)
                            .put("index.number_of_replicas", 0)
                            .put("index.uuid", indexUuid.toString())
                            .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE))
                    .build();
            IndexRoutingTable.Builder indexRouting = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int shardNumber = 0; shardNumber < count; shardNumber++) {
                ShardId id = new ShardId(indexMetadata.getIndex(), shardNumber);
                ShardRouting route = ShardRouting.newUnassigned(id, true,
                                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                        .initialize("node-a", null, 0L)
                        .moveToStarted();
                indexRouting.addIndexShard(new IndexShardRoutingTable.Builder(id).addShard(route).build());
            }
            metadata.put(indexMetadata, false);
            routing.add(indexRouting.build());
            remaining -= count;
            indexNumber++;
        }
        var localNode = new org.opensearch.cluster.node.DiscoveryNode("node-a",
                new org.opensearch.core.common.transport.TransportAddress(
                        java.net.InetAddress.getLoopbackAddress(), 9300), java.util.Map.of(),
                java.util.Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata.build())
                .routingTable(routing.build())
                .nodes(DiscoveryNodes.builder().localNodeId("node-a").add(localNode).build())
                .build();
    }
}
