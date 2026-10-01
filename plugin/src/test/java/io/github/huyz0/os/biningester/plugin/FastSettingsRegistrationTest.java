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
 * The three fast-mode settings reach the ingester (M13.23, criterion 7).
 *
 * <p>⚠️ A LIVE CHANGE IS THE HALF THAT MATTERS. OpenSearch 3.8.0 registers
 * {@code index.ingestion_source.param.*} as {@code IndexScope} and
 * {@code Dynamic}, so an operator flips {@code wal} on a running index; a
 * registrar that read the settings only when it first saw the index would
 * leave the ingester writing it in the old mode for as long as the node
 * lives, and every test that creates the index with its final settings stays
 * green.
 *
 * <p>⚠️ AND A MALFORMED VALUE STOPS ONE INDEX, NEVER THE PUSH. The params are
 * free-form -- OpenSearch validates none of them -- so {@code wal_quorum: 5}
 * reaches the registrar. Thrown out of the push loop it would stop every
 * index's registration on the node; dropped silently it would leave the index
 * in a mode its operator did not choose.
 */
class FastSettingsRegistrationTest {

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

        IndexRegistration last(String name) {
            IndexRegistration found = null;
            for (IndexRegistration r : registered) {
                if (r.indexName().equals(name)) {
                    found = r;
                }
            }
            return found;
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

    private static ClusterChangedEvent change(ClusterState to, ClusterState from) {
        return new ClusterChangedEvent("test", to, from);
    }

    @Test
    void theThreeSETTINGSReachTheRegistration() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(index("fast", UUID_1,
                Map.of("flush_timer", "250ms", "wal", "true", "wal_quorum", "3")));

        registrar.clusterChanged(change(state, ClusterState.builder(ClusterName.DEFAULT).build()));

        IndexRegistration pushed = transport.last("fast");
        assertThat(pushed).isNotNull();
        assertThat(pushed.flushTimerMillis()).as("flush_timer, parsed as a duration")
                .isEqualTo(250);
        assertThat(pushed.wal()).as("wal").isTrue();
        assertThat(pushed.walQuorum()).as("wal_quorum").isEqualTo(3);
    }

    @Test
    void anIndexSettingNONEOfThemIsPushedAtTheDefaults() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

        registrar.clusterChanged(change(state(index("plain", UUID_1, Map.of())),
                ClusterState.builder(ClusterName.DEFAULT).build()));

        IndexRegistration pushed = transport.last("plain");
        assertThat(pushed.flushTimerMillis()).isEqualTo(IndexRegistration.DEFAULT_FLUSH_TIMER_MILLIS);
        assertThat(pushed.wal()).isFalse();
        assertThat(pushed.walQuorum()).isEqualTo(IndexRegistration.DEFAULT_WAL_QUORUM);
    }

    @Test
    void aLIVEChangeOnTheIndexIsPushedAgain() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState before = state(index("live", UUID_1, Map.of()));
        ClusterState after = state(index("live", UUID_1, Map.of("wal", "true", "wal_quorum", "1")));

        registrar.clusterChanged(change(before, ClusterState.builder(ClusterName.DEFAULT).build()));
        registrar.clusterChanged(change(after, before));

        assertThat(transport.registered).as("one push per shape: the update is a change")
                .hasSize(2);
        assertThat(transport.last("live").wal())
                .as("the operator flipped wal on the running index; the ingester must hear it")
                .isTrue();
        assertThat(transport.last("live").walQuorum()).isEqualTo(1);
    }

    @Test
    void aMALFORMEDValueStopsThatIndexOnlyAndIsCounted() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState good = state(index("bad", UUID_1, Map.of()),
                index("other", UUID_2, Map.of()));
        ClusterState malformed = state(index("bad", UUID_1, Map.of("wal", "true", "wal_quorum", "5")),
                index("other", UUID_2, Map.of("flush_timer", "1s")));

        registrar.clusterChanged(change(good, ClusterState.builder(ClusterName.DEFAULT).build()));
        registrar.clusterChanged(change(malformed, good));

        assertThat(transport.last("other").flushTimerMillis())
                .as("the OTHER index's change still went out: one bad value never stops the push")
                .isEqualTo(1_000);
        assertThat(transport.last("bad").wal())
                .as("the malformed index keeps its last accepted shape rather than a guessed one")
                .isFalse();
        assertThat(registrar.refusedSettings())
                .as("and the refusal is counted, so an operator can see a setting was ignored")
                .isEqualTo(1);
    }

    @Test
    void aWALValueThatIsNotABooleanIsRefusedNotReadAsFalse() {
        Recording transport = new Recording();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

        registrar.clusterChanged(change(state(index("typo", UUID_1, Map.of("wal", "yes"))),
                ClusterState.builder(ClusterName.DEFAULT).build()));

        assertThat(transport.last("typo"))
                .as("`wal: yes` read leniently as false is a durability the operator did not "
                        + "choose, silently -- ADR-0013's warning about modes nobody can see")
                .isNull();
        assertThat(registrar.refusedSettings()).isEqualTo(1);
    }
}
