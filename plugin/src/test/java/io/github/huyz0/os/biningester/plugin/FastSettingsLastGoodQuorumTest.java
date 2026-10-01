// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;

/**
 * All three last good settings are kept, the quorum included (M13.23 review
 * round 3, T2).
 *
 * <p>⚠️ THE OTHER LAST-GOOD CASES CANNOT SEE THE QUORUM: they keep
 * {@code wal=false}, or {@code wal=true} at the default quorum of 2. A
 * registrar that kept the timer and wal but reset the quorum passed them --
 * and re-pushed a {@code q = 3} index at {@code q = 2} on the next
 * reconnect, for a typo in its timer.
 */
class FastSettingsLastGoodQuorumTest {

    private static final String NODE = "node-a";
    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";
    private static final String PARAM = "index.ingestion_source.param.";

    private static final class Recording implements SubscriptionTransport {
        final List<IndexRegistration> registered = Collections.synchronizedList(new ArrayList<>());

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(IndexRegistration registration) {
            registered.add(registration);
        }
    }

    private static ClusterState state(Map<String, String> params) {
        Settings.Builder settings = Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.uuid", UUID)
                .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE);
        params.forEach((k, v) -> settings.put(PARAM + k, v));
        IndexMetadata meta = IndexMetadata.builder("orders").settings(settings).build();
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        ShardRouting assigned = ShardRouting.newUnassigned(shardId, true,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                .initialize(NODE, null, 0L);
        DiscoveryNode node = new DiscoveryNode(NODE,
                new TransportAddress(java.net.InetAddress.getLoopbackAddress(), 9300), Map.of(),
                java.util.Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(meta, false).build())
                .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(meta.getIndex())
                        .addIndexShard(new IndexShardRoutingTable.Builder(shardId)
                                .addShard(assigned).build()).build()).build())
                .nodes(DiscoveryNodes.builder().add(node).localNodeId(NODE).build())
                .build();
    }

    @Test
    void aQUORUMOfThreeIsKeptThroughATimerTypoAndAReconnect() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState good = state(Map.of("wal", "true", "wal_quorum", "3", "flush_timer", "2s"));
        ClusterState typo = state(Map.of("wal", "true", "wal_quorum", "3", "flush_timer", "2S"));
        registrar.clusterChanged(new ClusterChangedEvent("test", good,
                ClusterState.builder(ClusterName.DEFAULT).build()));

        registrar.clusterChanged(new ClusterChangedEvent("test", typo, good));
        registrar.onReconnect();

        IndexRegistration repushed = transport.registered.get(transport.registered.size() - 1);
        assertThat(transport.registered).hasSize(2);
        assertThat(repushed.walQuorum()).as("the quorum the operator set, not the default")
                .isEqualTo(3);
        assertThat(repushed.wal()).isTrue();
        assertThat(repushed.flushTimerMillis()).isEqualTo(2_000);
    }
}
