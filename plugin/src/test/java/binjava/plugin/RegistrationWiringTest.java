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
 * The registrar is WIRED to the node, and its pushes hold no lock over the
 * wire (M6.7, FR-16).
 *
 * <p>⚠️ SPLIT FROM {@code RegistrationPushTest} BECAUSE OF THE 700-LINE CAP,
 * and the seam is a real one: that file counts messages against a
 * deterministic executor, and this one is about the WIRING and the two
 * THREADS. A case here needs a real pool, a parked transport or a
 * {@code ClusterService}; none of those belongs in a counting case.
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
class RegistrationWiringTest {

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
     * The registrar is actually WIRED to the node (M6.7 round 1's test major).
     *
     * <p>⚠️ MEASURED: deleting the whole installation left `:plugin:test`
     * green, because every case here builds an {@code IndexRegistrar} by hand.
     * FR-16 could have shipped with no listener registered, no shape ever
     * reaching the ingester, and the suite green.
     */
    @Test
    void thePluginREGISTERSTheRegistrarAsAClusterStateLISTENER() {
        RecordingTransport transport = new RecordingTransport();
        NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 16);
        List<org.opensearch.cluster.ClusterStateListener> listeners = new ArrayList<>();

        BinStorePlugin.installRegistrar(subscriptions, listeners::add, Runnable::run);

        assertThat(listeners).hasSize(1);
        ClusterState state = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
        listeners.get(0).clusterChanged(change(state, empty()));
        assertThat(transport.names())
                .as("and the listener the node holds is one that really pushes -- adding an "
                        + "object that is never driven is the same defect wearing a wire")
                .containsExactly("logs");
    }

    @Test
    void aDeploymentWithNOSubscriptionsRegistersNOTHING() {
        List<org.opensearch.cluster.ClusterStateListener> listeners = new ArrayList<>();

        BinStorePlugin.installRegistrar(null, listeners::add, Runnable::run);

        assertThat(listeners)
                .as("a node that did not configure this plugin has no transport to push on, "
                        + "and a registrar built without one would refuse on that node's every "
                        + "cluster-state change")
                .isEmpty();
    }

    /**
     * {@code createComponents} really does the wiring (M6.7 round 2's test
     * major).
     *
     * <p>⚠️ MEASURED: with the call deleted from {@code createComponents},
     * every case here stayed green, because they call {@code installRegistrar}
     * directly. Round 1's finding was made testable and the untested half
     * moved one frame up the stack.
     */
    @Test
    void theNODESEntryPointIsWhatInstallsTheRegistrar() {
        RecordingTransport transport = new RecordingTransport();
        NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 16);
        List<org.opensearch.cluster.ClusterStateListener> listeners = new ArrayList<>();
        org.opensearch.common.settings.Settings settings =
                Settings.builder().put("node.name", "test").build();
        org.opensearch.threadpool.ThreadPool threadPool =
                new org.opensearch.threadpool.ThreadPool(settings);
        org.opensearch.cluster.service.ClusterService clusterService =
                new org.opensearch.cluster.service.ClusterService(settings,
                        new org.opensearch.common.settings.ClusterSettings(settings,
                                org.opensearch.common.settings.ClusterSettings
                                        .BUILT_IN_CLUSTER_SETTINGS),
                        threadPool) {
                    @Override
                    public void addListener(
                            org.opensearch.cluster.ClusterStateListener listener) {
                        listeners.add(listener);
                    }
                };
        try {
            BinStorePlugin.install(subscriptions);
            new BinStorePlugin().createComponents(null, clusterService, threadPool, null, null,
                    null, null, null, null, null, null);

            assertThat(listeners)
                    .as("the node's own entry point is what must do it: a plugin whose "
                            + "createComponents installs nothing pushes no shape ever, and "
                            + "every case that calls installRegistrar directly stays green "
                            + "while it does")
                    .hasSize(1);
        } finally {
            BinStorePlugin.install(null);
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10,
                    java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * A push in flight does NOT stop the applier thread (M6.7 round 2's
     * blocking major).
     *
     * <p>⚠️ MEASURED ON THE ONE-LOCK VERSION: with {@code register} parked, a
     * second {@code clusterChanged} from another thread did not return within
     * two seconds. The applier thread was blocked on the MONITOR rather than
     * on the socket, which stops every cluster-state update on the node --
     * allocation, mappings, the ack to the cluster manager -- behind a hanging
     * connect, exactly the outcome moving the push to a pool was meant to
     * prevent.
     */
    @Test
    void aPUSHInFlightDoesNotBLOCKTheApplierThread() throws Exception {
        java.util.concurrent.CountDownLatch parked = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        RecordingTransport transport = new RecordingTransport() {
            @Override
            public void register(IndexRegistration registration) {
                entered.countDown();
                try {
                    parked.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.register(registration);
            }
        };
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            IndexRegistrar registrar = new IndexRegistrar(transport, pool);
            ClusterState first = state(List.of(index("logs", UUID_1, 1)), List.of(NODE));
            registrar.clusterChanged(change(first, empty()));
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS))
                    .as("PREMISE: a push really is in flight and parked")
                    .isTrue();

            ClusterState second = state(List.of(index("logs", UUID_1, 1),
                    index("metrics", UUID_2, 1)), List.of(NODE, NODE));
            java.util.concurrent.CompletableFuture<Void> applier =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () -> registrar.clusterChanged(change(second, first)));

            applier.get(10, java.util.concurrent.TimeUnit.SECONDS);
            parked.countDown();
        } finally {
            // ⚠️ `shutdownNow`, NOT `shutdown`: a case that fails while a push
            // is parked leaves a pool thread waiting on a latch nobody will
            // count down, and a non-daemon thread that never exits turns a
            // failing test into a HUNG build -- which is how the failure is
            // reported as nothing at all.
            pool.shutdownNow();
            assertThat(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
        assertThat(transport.names())
                .as("and both indices are pushed once the transport comes back -- the applier "
                        + "returning promptly must not cost the registration")
                .containsExactlyInAnyOrder("logs", "metrics");
    }

    /**
     * The push order is the NEWEST state's, not the order a multi-threaded
     * pool happened to run two tasks in (M6.7 round 2).
     */
    @Test
    void aBURSTOfChangesPushesTheNEWESTShapeLAST() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        List<Runnable> submitted = new ArrayList<>();
        IndexRegistrar registrar = new IndexRegistrar(transport, submitted::add);
        ClusterState claimed = state(List.of(index("logs-000001", UUID_1, 1, 1, "logs")),
                List.of(NODE));
        ClusterState released = state(List.of(index("logs-000001", UUID_1, 1)), List.of(NODE));

        registrar.clusterChanged(change(claimed, empty()));
        registrar.clusterChanged(change(released, claimed));
        // ⚠️ RUN IN THE ORDER A SCALING POOL MIGHT: the task submitted for the
        // FIRST event runs last. `threadPool.generic()` is multi-threaded and
        // orders nothing between tasks.
        for (int i = submitted.size() - 1; i >= 0; i--) {
            submitted.get(i).run();
        }

        assertThat(transport.registered.get(transport.registered.size() - 1).aliases())
                .as("the LAST push carries the newest shape, whichever order the tasks ran: "
                        + "the ingester's catalog keeps whichever arrived last, so a stale "
                        + "push landing last leaves `logs` pointing at an index that released "
                        + "it -- and `accepted` holds the stale value too, so nothing notices")
                .isEmpty();
    }

    /**
     * The NEWEST shape arrives at the transport LAST, with two real threads
     * and a push parked mid-flight (M6.7 round 3's test major).
     *
     * <p>⚠️ THE SERIAL VERSION OF THIS CASE COULD NOT SEE IT. Collecting tasks
     * in a list and running them one after another falsifies only the
     * re-derive; with the serialising lock removed, task A parks inside
     * {@code register(claimed)}, task B pushes {@code released} and records it,
     * and A's in-flight frame lands LAST at the ingester -- whose catalog takes
     * whichever arrived last, so `logs` resolves to an index that released it,
     * and this node's memory holds the newer shape so the diff never notices.
     *
     * <p>⚠️ WHAT MAKES IT DETERMINISTIC is that the second event is given a
     * bounded chance to overtake before the parked frame is released. Correct
     * code never starts a second push at all -- the coalescing flag and the
     * serialising lock are two independent reasons -- so the grace expires and
     * the single task pushes the newest shape last.
     */
    @Test
    void theNEWESTShapeArrivesLASTWithTWORealThreads() throws Exception {
        java.util.concurrent.CountDownLatch parked = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        List<List<String>> arrivals =
                java.util.Collections.synchronizedList(new ArrayList<>());
        RecordingTransport transport = new RecordingTransport() {
            @Override
            public void register(IndexRegistration registration) {
                // ⚠️ PARKED ON THE SHAPE, NOT ON "THE FIRST CALL". Which push
                // happens to be first is exactly what this case is measuring,
                // so a gate on call order decides the answer before the
                // production code gets a say.
                if (!registration.aliases().isEmpty() && entered.getCount() > 0) {
                    entered.countDown();
                    try {
                        parked.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                arrivals.add(registration.aliases());
                super.register(registration);
            }
        };
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            IndexRegistrar registrar = new IndexRegistrar(transport, pool);
            ClusterState claimed = state(
                    List.of(index("logs-000001", UUID_1, 1, 1, "logs")), List.of(NODE));
            ClusterState released = state(
                    List.of(index("logs-000001", UUID_1, 1)), List.of(NODE));

            registrar.clusterChanged(change(claimed, empty()));
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS))
                    .as("PREMISE: the first push is in flight and parked, so a second one "
                            + "really can overtake it")
                    .isTrue();
            registrar.clusterChanged(change(released, claimed));
            // ⚠️ THE SECOND EVENT IS GIVEN ITS CHANCE TO OVERTAKE, and the
            // grace is what makes the case deterministic rather than a race
            // the fixture wins by accident. Unparking immediately lets the
            // parked frame land first even when nothing serialises the two,
            // which is the broken shape reading as green.
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS
                    .toNanos(500);
            while (arrivals.size() < 2 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            parked.countDown();
        } finally {
            // ⚠️ `shutdownNow`, NOT `shutdown`: a case that fails while a push
            // is parked leaves a pool thread waiting on a latch nobody will
            // count down, and a non-daemon thread that never exits turns a
            // failing test into a HUNG build -- which is how the failure is
            // reported as nothing at all.
            pool.shutdownNow();
            assertThat(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }

        assertThat(arrivals.get(arrivals.size() - 1))
                .as("the LAST frame to reach the transport is the newest shape -- the "
                        + "ingester's catalog keeps whichever arrived last, and a stale one "
                        + "landing last leaves `logs` pointing at an index that gave it up")
                .isEmpty();
    }

    /**
     * A push that exhausts its attempts does not abandon the indices behind it.
     */
    @Test
    void anINDEXWhosePushFAILSDoesNotStopTheNEXTIndexsPush() throws Exception {
        RecordingTransport transport = new RecordingTransport() {
            @Override
            public void register(IndexRegistration registration) {
                if (registration.indexName().equals("first")) {
                    throw new IllegalStateException("the subscription is reconnecting");
                }
                super.register(registration);
            }
        };
        IndexRegistrar registrar = new IndexRegistrar(transport, Runnable::run);
        ClusterState state = state(
                List.of(index("first", UUID_1, 1), index("second", UUID_2, 1)),
                List.of(NODE, NODE));

        registrar.clusterChanged(change(state, empty()));

        assertThat(transport.names())
                .as("one unreachable index must not cost every index after it in the loop: "
                        + "they are separate messages, and stopping at the first failure "
                        + "leaves the rest of this node's indices unregistered until some "
                        + "later change happens to reorder them")
                .containsExactly("second");
    }
}
