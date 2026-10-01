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
import org.opensearch.cluster.metadata.AliasMetadata;
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
 * What a refused index keeps, and when it stops keeping it (M13.23 review
 * round 2, P1 and T2).
 *
 * <p>⚠️ ONLY THE SETTINGS, NEVER THE PLACEMENT. Holding the whole last good
 * registration froze the aliases with it: a rollover while a setting was
 * malformed pushed the old index still claiming the write alias, and after
 * an ingester restart every write to the alias went to the rolled-over index.
 *
 * <p>⚠️ AND FORGOTTEN WHEN THE INDEX LEAVES THE NODE. Kept, it re-pushes a
 * stale shape when the index comes back -- a {@code wal=false} from before,
 * over a {@code wal=true} another node has pushed since -- silently lowering
 * the durability the operator set.
 */
class FastSettingsLastGoodTest {

    private static final String NODE = "node-a";
    private static final String OLD = "nVzgup36TLqWp7VBBREj1w";
    private static final String NEW = "PZtwuWJyTbi4LRGDmoUfzA";
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

        List<IndexRegistration> of(String name) {
            return registered.stream().filter(r -> r.indexName().equals(name)).toList();
        }
    }

    private static IndexMetadata index(String name, String uuid, Map<String, String> params,
            String... aliases) {
        Settings.Builder settings = Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.uuid", uuid)
                .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE);
        params.forEach((k, v) -> settings.put(PARAM + k, v));
        IndexMetadata.Builder builder = IndexMetadata.builder(name).settings(settings);
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).build());
        }
        return builder.build();
    }

    private static ClusterState state(IndexMetadata... indices) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();
        for (IndexMetadata meta : indices) {
            metadata.put(meta, false);
            ShardId shardId = new ShardId(meta.getIndex(), 0);
            ShardRouting assigned = ShardRouting.newUnassigned(shardId, true,
                            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                    .initialize(NODE, null, 0L);
            routing.add(IndexRoutingTable.builder(meta.getIndex()).addIndexShard(
                    new IndexShardRoutingTable.Builder(shardId).addShard(assigned).build()).build());
        }
        DiscoveryNode node = new DiscoveryNode(NODE,
                new TransportAddress(java.net.InetAddress.getLoopbackAddress(), 9300), Map.of(),
                java.util.Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata.build())
                .routingTable(routing.build())
                .nodes(DiscoveryNodes.builder().add(node).localNodeId(NODE).build())
                .build();
    }

    private static ClusterState none() {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }

    private static void apply(IndexRegistrar registrar, ClusterState to, ClusterState from) {
        registrar.clusterChanged(new ClusterChangedEvent("test", to, from));
    }

    @Test
    void anALIASMovedWhileASettingIsRefusedStillMoves() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState before = state(index("old", OLD, Map.of("wal", "true"), "logs"));
        ClusterState rolled = state(index("old", OLD, Map.of("wal", "yes")),
                index("new", NEW, Map.of(), "logs"));
        apply(registrar, before, none());

        apply(registrar, rolled, before);
        registrar.onReconnect();

        IndexRegistration old = transport.of("old").get(transport.of("old").size() - 1);
        assertThat(old.aliases())
                .as("the rollover released `logs` from the old index; a refused setting must "
                        + "not freeze its placement, or the write alias resolves to the old "
                        + "index after the ingester restarts")
                .doesNotContain("logs");
        assertThat(old.wal())
                .as("while the SETTINGS stay at their last good value").isTrue();
    }

    @Test
    void theLASTGoodShapeIsForgottenWhenTheIndexLeavesTheNode() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState here = state(index("o", OLD, Map.of("wal", "false")));
        ClusterState gone = state();
        ClusterState backMalformed = state(index("o", OLD, Map.of("wal", "yes")));
        apply(registrar, here, none());

        apply(registrar, gone, here);
        apply(registrar, backMalformed, gone);

        assertThat(transport.of("o"))
                .as("back on this node with a malformed value, it has no last good shape here: "
                        + "re-pushing the old wal=false could overwrite a wal=true another node "
                        + "pushed meanwhile -- a durability silently lowered")
                .hasSize(1);
    }
}
