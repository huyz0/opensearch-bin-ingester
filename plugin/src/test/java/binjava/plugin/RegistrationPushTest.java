// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
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

/**
 * The node tells the ingester what its indices look like (M6.7, FR-16,
 * ADR-0015 § 2).
 *
 * <p>⚠️ THE PUSH IS PER NODE AND PER CHANGE, NEVER PER SHARD AND NEVER ON A
 * TIMER, and this class is the only thing in M6 that observes it. FR-16 is
 * otherwise served by three criteria that all stay green against a listener
 * built in {@code createShardConsumer} — which runs once per SHARD, so an
 * eight-shard node sends eight identical copies of one index's shape on every
 * cluster-state change.
 *
 * <p>⚠️ AND A FAILED PUSH IS RETRIED. One throw that is never retried satisfies
 * every count here exactly, and then every index on that node falls out of the
 * ingester's pending pool at {@code pendingTimeout}: a sustained stream of
 * refused writes for a fault that lasted one message.
 */
class RegistrationPushTest {

    private static final String NODE = "node-a";
    private static final String UUID_1 = "nVzgup36TLqWp7VBBREj1w";
    private static final String UUID_2 = "PZtwuWJyTbi4LRGDmoUfzA";

    /** Takes registrations, and can be told to refuse a stated number of them. */
    private static class RecordingTransport implements SubscriptionTransport {
        private final List<IndexRegistration> registered =
                java.util.Collections.synchronizedList(new ArrayList<>());
        private int refusalsLeft;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(IndexRegistration registration) {
            if (refusalsLeft > 0) {
                refusalsLeft--;
                throw new IllegalStateException("the subscription is reconnecting");
            }
            registered.add(registration);
        }

        private List<String> names() {
            return registered.stream().map(IndexRegistration::indexName).toList();
        }

        @SuppressWarnings("unused")
        private void ignore(Delivery delivery) {
        }
    }

    private static IndexMetadata index(String name, String uuid, int shards, String... aliases) {
        return index(name, uuid, shards, shards, aliases);
    }

    private static IndexMetadata index(String name, String uuid, int shards, int routingShards,
            String... aliases) {
        return partitioned(name, uuid, shards, routingShards, 1, aliases);
    }

    /** ⚠️ {@code routing_partition_size} is a FIELD of the registration, and an
     * index that sets it is one the ingester must REFUSE to place by routing
     * alone -- its shard depends on the document _id too. A registrar that sent
     * 1 for every index takes that refusal away and turns a routed search into
     * a silent miss. */
    private static IndexMetadata partitioned(String name, String uuid, int shards,
            int routingShards, int partitionSize, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing_partition_size", partitionSize)
                        .put("index.uuid", uuid)
                        .put(IndexRegistrar.SOURCE_TYPE, BinStorePlugin.TYPE))
                .setRoutingNumShards(routingShards);
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).build());
        }
        return builder.build();
    }

    /** An index this plugin does NOT ingest: no `ingestion_source.type`. */
    private static IndexMetadata foreignIndex(String name, String uuid) {
        return IndexMetadata.builder(name)
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.uuid", uuid))
                .build();
    }

    /**
     * Every shard of every index given is assigned to {@code hostedOn}, and an
     * index whose {@code hostedOn} is null is in the cluster but on some other
     * node.
     */
    private static ClusterState state(List<IndexMetadata> indices, List<String> hostedOn) {
        return state(indices, hostedOn, NODE);
    }

    /** ⚠️ {@code localNode} is what the registrar reads out of the event. */
    private static ClusterState state(List<IndexMetadata> indices, List<String> hostedOn,
            String localNode) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();
        for (int i = 0; i < indices.size(); i++) {
            IndexMetadata meta = indices.get(i);
            metadata.put(meta, false);
            String node = hostedOn.get(i);
            IndexRoutingTable.Builder table = IndexRoutingTable.builder(meta.getIndex());
            for (int shard = 0; shard < meta.getNumberOfShards(); shard++) {
                ShardId shardId = new ShardId(meta.getIndex(), shard);
                ShardRouting assigned = ShardRouting.newUnassigned(shardId, true,
                                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"))
                        .initialize(node, null, 0L);
                table.addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(assigned).build());
            }
            routing.add(table.build());
        }
        // ⚠️ REAL NODES, not just a local id. `RoutingNodes.node(id)` answers
        // null for a node the state does not know at all -- which is the
        // TRANSIENT view -- and an EMPTY RoutingNode for one that is in the
        // cluster and hosts nothing. The registrar treats those two
        // differently, so a fixture that cannot produce both cannot tell them
        // apart either.
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().localNodeId(localNode);
        for (String id : new java.util.LinkedHashSet<>(
                java.util.stream.Stream.concat(hostedOn.stream(), java.util.stream.Stream.of(localNode))
                        .toList())) {
            nodes.add(node(id));
        }
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata.build())
                .routingTable(routing.build())
                .nodes(nodes.build())
                .build();
    }

    private static org.opensearch.cluster.node.DiscoveryNode node(String id) {
        return new org.opensearch.cluster.node.DiscoveryNode(id,
                // ⚠️ A DISTINCT PORT PER NODE: DiscoveryNodes refuses two nodes
                // sharing a transport address, which is how a fixture that
                // gives every node port 0 learns it built one node twice.
                new org.opensearch.core.common.transport.TransportAddress(
                        java.net.InetAddress.getLoopbackAddress(),
                        9300 + Math.floorMod(id.hashCode(), 100)),
                java.util.Map.of(),
                java.util.Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
    }

    private static ClusterChangedEvent change(ClusterState to, ClusterState from) {
        return new ClusterChangedEvent("test", to, from);
    }

    private static ClusterState empty() {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }

    /**
     * ⚠️ K = 1 AND K = 8 IN ONE CASE, because the defect being caught is a
     * count that scales with K and both numbers are needed to tell a per-node
     * push from a per-shard one: at K = 1 they are indistinguishable.
     */
    @Test
    void onCONNECTOnePushPerINDEXWhateverTheSHARDCount() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(
                List.of(index("one-shard", UUID_1, 1), index("eight-shard", UUID_2, 8)),
                List.of(NODE, NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("TWO pushes for NINE shards: a listener built in createShardConsumer would "
                        + "send nine, eight of them identical copies of one index's shape, and "
                        + "every other criterion in M6 stays green while it does")
                .containsExactlyInAnyOrder("one-shard", "eight-shard");
        assertThat(registrar.pushes()).isEqualTo(2);
    }

    @Test
    void theRegistrationCarriesTheINDEXSShapeAndItsALIASES() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs-000001", UUID_1, 4, 16, "logs")),
                List.of(NODE));

        registrar.clusterChanged(change(state, empty()));

        IndexRegistration pushed = transport.registered.get(0);
        assertThat(pushed.indexUuid())
                .as("the UUID, not the name: stream identity is (indexUUID, partition), so a "
                        + "rolled-over alias pointing at a new index is a new stream set")
                .isEqualTo(UUID_1);
        assertThat(pushed.aliases())
                .as("and the aliases, or a producer writing to `logs` reaches an ingester that "
                        + "has never heard the name")
                .containsExactly("logs");
        assertThat(pushed.numShards()).isEqualTo(4);
        assertThat(pushed.routingNumShards())
                .as("and the SPLIT shape: a split index hashes over routingNumShards and "
                        + "divides by the factor, so sending only numShards places every "
                        + "record on the wrong shard of a split index")
                .isEqualTo(16);
        assertThat(pushed.routingFactor()).isEqualTo(4);
    }

    @Test
    void anIRRELEVANTChangeCostsNOPush() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 2)), List.of(NODE));
        registrar.clusterChanged(change(state, empty()));

        registrar.clusterChanged(change(state, state));
        registrar.clusterChanged(change(state, state));

        assertThat(registrar.pushes())
                .as("the push is DIFFED, which is what makes 'on change, not on a timer' worth "
                        + "having: cluster state is published constantly on a busy cluster, and "
                        + "a push per publication is a poll wearing an event's clothes")
                .isEqualTo(1);
    }

    @Test
    void aRELEVANTChangePushesTheNEWShape() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState before = state(List.of(index("logs-000001", UUID_1, 2)), List.of(NODE));
        registrar.clusterChanged(change(before, empty()));
        ClusterState after = state(List.of(index("logs-000001", UUID_1, 2, 2, "logs")),
                List.of(NODE));

        registrar.clusterChanged(change(after, before));

        assertThat(registrar.pushes())
                .as("an alias added is a change the ingester MUST hear: until it does, a write "
                        + "to `logs` is an unknown index and waits in the pending pool until it "
                        + "is refused")
                .isEqualTo(2);
        assertThat(transport.registered.get(1).aliases()).containsExactly("logs");
    }

    @Test
    void anIndexThisNodeDoesNOTHostIsNOTPushed() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(
                List.of(index("mine", UUID_1, 1), index("theirs", UUID_2, 1)),
                List.of(NODE, "node-b"));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("the node that hosts an index is the one that pushes its shape -- every "
                        + "node pushing every index makes the message count scale with the "
                        + "CLUSTER's index count on every node, which is the shape "
                        + "non-negotiable 6 forbids one layer up")
                .containsExactly("mine");
    }

    @Test
    void anIndexThisPluginDoesNOTIngestIsNOTPushed() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(
                List.of(index("ingested", UUID_1, 1), foreignIndex("ordinary", UUID_2)),
                List.of(NODE, NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("a node hosts ordinary indices too, and pushing their shapes fills the "
                        + "ingester's catalog with indices nothing will ever write to it")
                .containsExactly("ingested");
    }

    /**
     * A push that fails is RETRIED (M6 criterion 12).
     *
     * <p>⚠️ THIS IS THE CASE THAT SEPARATES A CORRECT REGISTRAR FROM ONE THAT
     * SATISFIES EVERY COUNT ABOVE AND IS BADLY WRONG. One throw, never retried,
     * and this index's shape never reaches the ingester: every routed write to
     * it waits out {@code pendingTimeout} and is refused, for a fault that
     * lasted one message.
     */
    @Test
    void aFAILEDPushIsRETRIEDImmediately() {
        RecordingTransport transport = new RecordingTransport();
        transport.refusalsLeft = 2;
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("two refusals and the third attempt lands -- no timer, no scheduler, and "
                        + "no cluster-state change needed to recover from a message lost to a "
                        + "connection that was already coming back")
                .containsExactly("logs");
        assertThat(registrar.pushFailures()).isEqualTo(2);
        assertThat(registrar.pushes()).isEqualTo(1);
    }

    @Test
    void aPushThatKeepsFailingIsCARRIEDToTheNextChange() {
        RecordingTransport transport = new RecordingTransport();
        transport.refusalsLeft = IndexRegistrar.ATTEMPTS;
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("PREMISE: every attempt was refused, so nothing was registered")
                .isEmpty();
        assertThat(registrar.registeredIndices())
                .as("and it is NOT recorded as accepted -- recording it before the push "
                        + "succeeds makes the diff below find it unchanged forever, which is "
                        + "the never-retried failure one step more subtle")
                .isZero();

        registrar.clusterChanged(change(state, state));

        assertThat(transport.names())
                .as("the very next cluster-state change re-sends it, though the state ITSELF "
                        + "did not change: a carried failure is exactly the case the diff must "
                        + "not skip")
                .containsExactly("logs");
    }

    @Test
    void anIndexTHISNodeStopsHostingIsPushedAGAINWhenItComesBack() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState here = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
        ClusterState moved = state(List.of(index("logs", UUID_1, 1)), List.of("node-b"));
        registrar.clusterChanged(change(here, empty()));

        registrar.clusterChanged(change(moved, here));
        registrar.clusterChanged(change(here, moved));

        assertThat(registrar.pushes())
                .as("a shard moving away and back re-pushes: remembering a shape for an index "
                        + "this node no longer hosts means a change made while it was away is "
                        + "diffed against a stale memory and never sent")
                .isEqualTo(2);
        assertThat(transport.names()).containsExactly("logs", "logs");
    }

    @Test
    void aNodeHostingNOTHINGPushesNOTHING() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE), "node-z");

        registrar.clusterChanged(change(state, empty()));

        assertThat(registrar.pushes())
                .as("every node is in this state between joining the cluster and being "
                        + "allocated anything, and it is not an error")
                .isZero();
    }

    @Test
    void theRegistrationCarriesROUTINGPARTITIONSIZEWhenTheIndexSetsIt() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(
                List.of(partitioned("tenants", UUID_1, 8, 8, 4)), List.of(NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.registered.get(0).routingPartitionSize())
                .as("sending 1 for an index that sets 4 takes away the ingester's REFUSAL to "
                        + "place it -- its shard depends on the document _id as well as the "
                        + "routing value, so the write is then placed by murmur3 alone and "
                        + "the routed search that looks for it misses, silently")
                .isEqualTo(4);
    }

    @Test
    void theALIASESAreSORTEDSoAReORDERIsNotAChange() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs-000001", UUID_1, 1, 1, "zeta", "alpha")),
                List.of(NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.registered.get(0).aliases())
                .as("the registration is diffed by equality and its aliases are a List, so "
                        + "the order OpenSearch happens to give them would otherwise read as "
                        + "a change and re-push on every cluster-state update")
                .containsExactly("alpha", "zeta");
    }

    /**
     * A RECONNECT re-pushes, which is the "on connect" half of ADR-0015 § 2.
     *
     * <p>⚠️ THE CHANGE HALF CANNOT SERVE IT. The ingester's catalog is in
     * memory and dies with it; after it restarts, this node's memory of what
     * was accepted still holds every shape, so the diff finds nothing due and a
     * steady cluster sends nothing for hours. Every routed write on this node
     * pends and is refused at `pendingTimeout` against a healthy ingester.
     */
    @Test
    void aRECONNECTReePUSHESWithoutWaitingForAClusterChange() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 2)), List.of(NODE));
        registrar.clusterChanged(change(state, empty()));
        registrar.clusterChanged(change(state, state));
        assertThat(registrar.pushes())
                .as("PREMISE: the diff really does suppress the repeat, or the case below "
                        + "cannot tell a reconnect from an ordinary change")
                .isEqualTo(1);

        registrar.onReconnect();

        assertThat(transport.names())
                .as("the ingester that restarted learns every shape again, with no "
                        + "cluster-state change needed -- and none may come for hours")
                .containsExactly("logs", "logs");
    }

    @Test
    void aRECONNECTBeforeANYClusterStateIsNOTAnError() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);

        registrar.onReconnect();

        assertThat(registrar.pushes())
                .as("a node that has seen no cluster state hosts nothing to push, and the "
                        + "transport may well connect before the node has joined")
                .isZero();
    }

    /**
     * A transient state with no routing node for this node FORGETS NOTHING.
     */
    @Test
    void aStateWithNoROUTINGNodeForThisNodeDoesNotFORGETEverything() {
        RecordingTransport transport = new RecordingTransport();
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
        registrar.clusterChanged(change(state, empty()));
        ClusterState blank = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().localNodeId(NODE).build())
                .build();

        registrar.clusterChanged(change(blank, state));
        registrar.clusterChanged(change(state, blank));

        assertThat(registrar.pushes())
                .as("treating 'no routing node' as 'hosts nothing' empties the memory, and "
                        + "the next ordinary state re-pushes every index at once -- a burst "
                        + "caused by a transient view, on the node least able to absorb it")
                .isEqualTo(1);
    }

    /**
     * The pushes do NOT run on the thread that delivered the event.
     *
     * <p>⚠️ {@code clusterService.addListener} hands this class the cluster
     * APPLIER thread, which applies every cluster-state update on the node.
     * Each push is up to three network calls with no timeout of its own: forty
     * indices due against an unreachable ingester is 120 serial attempts with
     * allocation, mappings and the ack to the cluster manager stopped behind
     * them.
     */
    @Test
    void thePUSHESRunOnTheEXECUTORAndNotOnTheCallersThread() {
        RecordingTransport transport = new RecordingTransport();
        List<Runnable> submitted = new ArrayList<>();
        IndexRegistrar registrar = new IndexRegistrar(transport, submitted::add);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.registered)
                .as("nothing reached the transport on the caller's thread")
                .isEmpty();
        assertThat(submitted).hasSize(1);
        submitted.get(0).run();
        assertThat(transport.names()).containsExactly("logs");
    }

    @Test
    void anIRRELEVANTChangeSUBMITSNothingAtAll() {
        RecordingTransport transport = new RecordingTransport();
        List<Runnable> submitted = new ArrayList<>();
        IndexRegistrar registrar = new IndexRegistrar(transport, submitted::add);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
        registrar.clusterChanged(change(state, empty()));
        submitted.forEach(Runnable::run);
        submitted.clear();

        registrar.clusterChanged(change(state, state));

        assertThat(submitted)
                .as("a cluster-state update that changes nothing this node ingests costs not "
                        + "even a task on the pool -- cluster state is published constantly on "
                        + "a busy cluster")
                .isEmpty();
    }





}
