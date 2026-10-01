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
 * A malformed fast-mode setting is refused strictly, for its index alone, and
 * the index keeps working (M13.23 review round 1, P1-P6 and T1-T3).
 *
 * <p>⚠️ "KEEPS ITS LAST GOOD SHAPE" IS ASSERTED ACROSS A RECONNECT. The first
 * draft only checked that no NEW push went out, which a registrar that forgot
 * the index also satisfied -- and forgetting it is the bug: after an ingester
 * restart (its catalog is in memory) the index then had no registration at
 * all, every write to it refused at {@code pendingTimeout}, for a typo.
 */
class FastSettingsRefusalTest {

    private static final String NODE = "node-a";
    private static final String UUID_1 = "nVzgup36TLqWp7VBBREj1w";
    private static final String UUID_2 = "PZtwuWJyTbi4LRGDmoUfzA";
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

    private static IndexMetadata index(String name, String uuid, Map<String, String> params) {
        Settings.Builder settings = Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.uuid", uuid)
                .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE);
        params.forEach((k, v) -> settings.put(PARAM + k, v));
        return IndexMetadata.builder(name).settings(settings).build();
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
    void aREFUSEDIndexKeepsItsLastGoodShapeThroughAReconnect() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState good = state(index("orders", UUID_1, Map.of("flush_timer", "2s")));
        ClusterState typo = state(index("orders", UUID_1, Map.of("flush_timer", "2s",
                "wal", "true", "wal_quorum", "5")));
        apply(registrar, good, none());

        apply(registrar, typo, good);

        assertThat(registrar.registeredIndices())
                .as("the refused index is still one this node has a live registration for")
                .isEqualTo(1);
        registrar.onReconnect();
        List<IndexRegistration> pushed = transport.of("orders");
        assertThat(pushed)
                .as("after the ingester restarts -- its catalog is in memory -- the index is "
                        + "registered again, or every write to it is refused at pendingTimeout")
                .hasSize(2);
        assertThat(pushed.get(1))
                .as("and at its LAST GOOD shape, not the malformed one nor the defaults")
                .isEqualTo(pushed.get(0));
        assertThat(pushed.get(1).flushTimerMillis()).isEqualTo(2_000);
    }

    @Test
    void aMALFORMEDFlushTimerIsRefusedNeverDefaulted() {
        for (String timer : new String[] {"fast", "0ms", "-1"}) {
            Recording transport = new Recording();
            IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

            apply(registrar, state(index("t", UUID_1, Map.of("flush_timer", timer))), none());

            assertThat(transport.of("t"))
                    .as("flush_timer [%s] is not a positive duration; read as the 5 s "
                            + "default it is a deadline nobody chose", timer)
                    .isEmpty();
            assertThat(registrar.refusedSettings()).isEqualTo(1);
        }
    }

    @Test
    void walIsEXACTLYTrueOrFalse() {
        for (String wal : new String[] {"TRUE", " true", "False"}) {
            Recording transport = new Recording();
            IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

            apply(registrar, state(index("w", UUID_1, Map.of("wal", wal))), none());

            assertThat(transport.of("w")).as("wal [%s]", wal).isEmpty();
            assertThat(registrar.refusedSettings()).isEqualTo(1);
        }
    }

    @Test
    void aBADQuorumIsRefusedEvenWithWalOff() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

        apply(registrar, state(index("q", UUID_1, Map.of("wal", "false", "wal_quorum", "5"))),
                none());

        assertThat(transport.of("q"))
                .as("a quorum of 5 is a typo whatever wal says, live the moment wal turns on")
                .isEmpty();
        assertThat(registrar.refusedSettings()).isEqualTo(1);
    }

    @Test
    void aValueFIXEDThenBrokenAgainIsCountedAgain() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState bad = state(index("o", UUID_1, Map.of("wal", "yes")));
        ClusterState fixed = state(index("o", UUID_1, Map.of("wal", "true")));

        apply(registrar, bad, none());
        apply(registrar, fixed, bad);
        apply(registrar, bad, fixed);

        assertThat(registrar.refusedSettings())
                .as("two separate mistakes, two refusals: the dedupe is per standing value")
                .isEqualTo(2);
    }

    @Test
    void aRefusalIsFORGOTTENWithItsIndex() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        IndexMetadata other = index("other", UUID_2, Map.of());
        ClusterState bad = state(index("o", UUID_1, Map.of("wal", "yes")), other);
        ClusterState gone = state(other);

        apply(registrar, bad, none());
        apply(registrar, gone, bad);
        apply(registrar, bad, gone);

        assertThat(registrar.refusedSettings())
                .as("the index left this node and came back still malformed: that is "
                        + "logged and counted again, not remembered as already reported")
                .isEqualTo(2);
    }
}
