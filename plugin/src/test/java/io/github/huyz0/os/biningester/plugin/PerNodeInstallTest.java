// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.common.settings.Settings;

/**
 * Two nodes in ONE JVM hold two DIFFERENT subscriptions (M6.13, FR-7).
 *
 * <p>⚠️ IT IS NOT ONLY A TEST PROBLEM, and that is the reason to fix it rather
 * than work around it. The plugin's node state was a {@code static volatile}
 * field — one per JVM — and {@code InternalTestCluster} runs every node of a
 * cluster in one JVM, so "each node holds ONE subscription" reads as 1 however
 * many nodes there are: M6.9's multi-node criterion becomes unmeasurable, not
 * merely unmeasured. The same static is why nothing can hold two
 * ingester-facing identities in one process.
 *
 * <p>⚠️ THE KEY IS THE NODE'S OWN NAME, NEVER CALL ORDER. An install-order
 * queue satisfies every deterministic boot — hand out the first, then the
 * second — and hands node B node A's subscription the moment two nodes start
 * concurrently, or one restarts. OpenSearch supplies {@code node.name} to the
 * plugin's constructor in its settings, so the key is something the node says
 * rather than something the fixture assumed.
 */
class PerNodeInstallTest {

    @AfterEach
    void clearInstallation() {
        BinStorePlugin.uninstall();
    }

    private static final class NoopTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }
    }

    private static Settings nodeNamed(String name) {
        return Settings.builder().put("node.name", name).build();
    }

    @Test
    void TWONodesInONEJVMGetDISTINCTSubscriptions() {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));

        NodeSubscriptions a = new BinStorePlugin(nodeNamed("node-a")).subscriptions();
        NodeSubscriptions b = new BinStorePlugin(nodeNamed("node-b")).subscriptions();

        assertThat(a)
                .as("distinct OBJECTS, not merely distinct values: `SubscriptionHub` groups by "
                        + "subscriber IDENTITY, so two nodes sharing one instance are ONE "
                        + "subscriber to the ingester -- and a criterion counting "
                        + "subscriptions per node then reads 1 whatever the node count is")
                .isNotSameAs(b);
        assertThat(a).isNotNull();
        assertThat(b).isNotNull();
    }

    @Test
    void theSAMENodeAskingTWICEGetsTheSAMEInstance() {
        AtomicInteger built = new AtomicInteger();
        BinStorePlugin.install(node -> {
            built.incrementAndGet();
            return new NodeSubscriptions(new NoopTransport(), 16);
        });

        NodeSubscriptions first = new BinStorePlugin(nodeNamed("node-a")).subscriptions();
        NodeSubscriptions again = new BinStorePlugin(nodeNamed("node-a")).subscriptions();

        assertThat(first)
                .as("one node, one subscription, however many times the plugin is constructed "
                        + "-- a node whose plugin is rebuilt during a restart must not open a "
                        + "second subscriber and leave the first one taking deliveries")
                .isSameAs(again);
        assertThat(built.get()).isEqualTo(1);
    }

    /**
     * The key is the NODE's name, not the order the plugins were built.
     *
     * <p>⚠️ THE CASE AN INSTALL-ORDER QUEUE PASSES AND THIS ONE DOES NOT.
     * Handing out installed instances in order is green for every deterministic
     * boot; here the nodes are built in the reverse of the order they were
     * installed, and a queue hands each the other's.
     */
    @Test
    void theKeyIsTheNODESOwnNameAndNotTheORDEROfConstruction() {
        ConcurrentHashMap<String, NodeSubscriptions> made = new ConcurrentHashMap<>();
        BinStorePlugin.install(node -> {
            NodeSubscriptions subscriptions = new NodeSubscriptions(new NoopTransport(), 16);
            made.put(node, subscriptions);
            return subscriptions;
        });

        NodeSubscriptions second = new BinStorePlugin(nodeNamed("node-b")).subscriptions();
        NodeSubscriptions first = new BinStorePlugin(nodeNamed("node-a")).subscriptions();

        assertThat(made.get("node-b"))
                .as("each node gets the subscription made FOR IT -- an install-order queue "
                        + "hands node-a's to whichever node happened to start first, and a "
                        + "cluster's nodes start concurrently")
                .isSameAs(second);
        assertThat(made.get("node-a")).isSameAs(first);
    }

    /**
     * ⚠️ CONCURRENTLY, because "two nodes start at once" is the ordinary case
     * in {@code InternalTestCluster} and the one an install-order queue gets
     * wrong in a way a sequential case cannot see.
     */
    @Test
    @Timeout(60)
    void NODESStartingAtTheSAMEMomentStillGetTheirOWN() throws Exception {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));
        int nodes = 8;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<NodeSubscriptions> got = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < nodes; i++) {
            String name = "node-" + i;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                got.add(new BinStorePlugin(nodeNamed(name)).subscriptions());
            }));
        }
        go.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertThat(got).hasSize(nodes);
        assertThat(new java.util.IdentityHashMap<NodeSubscriptions, Boolean>() {{
            got.forEach(s -> put(s, Boolean.TRUE));
        }})
                .as("eight nodes, eight subscriptions -- by IDENTITY, because that is what "
                        + "the hub groups by")
                .hasSize(nodes);
    }

    @Test
    void aDeploymentThatInstalledNOTHINGStillCONSTRUCTS() {
        assertThatCode(() -> new BinStorePlugin(nodeNamed("node-a")))
                .as("CONSTRUCTING is not the place to refuse: this runs while the node is "
                        + "being built, and an exception here is a node that cannot start "
                        + "with a diagnostic pointing at the wrong thing. ⚠️ It does NOT mean "
                        + "such a node boots -- measured: null subscriptions make "
                        + "getIngestionConsumerFactories build a BinStoreConsumerFactory(null), "
                        + "which throws during boot. That is M6.16, not this case")
                .doesNotThrowAnyException();
        assertThat(new BinStorePlugin(nodeNamed("node-a")).subscriptions()).isNull();
    }

    @Test
    void theNODESNameIsREQUIRED() {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));

        assertThat(new BinStorePlugin(Settings.EMPTY).subscriptions())
                .as("a node with no name in its settings is not a node this plugin can key "
                        + "state by, and inventing one would hand two such nodes the same "
                        + "state -- which is the static this task removes, wearing a default")
                .isNull();
    }

    /**
     * Installing again FORGETS every node's state (M6.13 round 1).
     *
     * <p>⚠️ MEASURED: with the clear deleted, all of {@code :plugin:test} was
     * green and {@code :plugin:clusterTest} FAILED. Eight IT classes each
     * install in a static block, nothing forks a JVM per class, and
     * {@code OpenSearchSingleNodeTestCase} reuses the node name — so the second
     * class's node was handed the FIRST class's transport. That is this task's
     * acceptance (3) in its literal words, and {@code @AfterEach uninstall()}
     * hides it from every case here.
     */
    @Test
    void INSTALLINGAgainFORGETSWhatTheLastInstallationBuilt() {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));
        NodeSubscriptions first = new BinStorePlugin(nodeNamed("node-a")).subscriptions();

        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));
        NodeSubscriptions afterReinstall = new BinStorePlugin(nodeNamed("node-a")).subscriptions();

        assertThat(afterReinstall)
                .as("a node name that outlives one installation must not inherit its state: "
                        + "two test classes in one JVM install in turn and the framework hands "
                        + "the second node the same name, so keeping the map makes the second "
                        + "class's node speak to the first class's transport")
                .isNotSameAs(first);
    }

    /**
     * EXACTLY ONE public constructor (M6.13 round 1, acceptance (2)).
     *
     * <p>⚠️ MEASURED: making the test-only constructor public leaves all of
     * {@code :plugin:test} green and makes the plugin UNLOADABLE by a real
     * node — {@code PluginsService.loadPlugin} throws "no unique public
     * constructor" on {@code getConstructors().length > 1}, before it looks at
     * any signature. The class javadoc has named that trap since M1 and nothing
     * asserted it.
     */
    @Test
    void thePluginHasEXACTLYOnePUBLICConstructor() {
        assertThat(BinStorePlugin.class.getConstructors())
                .as("OpenSearch refuses a plugin with two -- and every in-process test stays "
                        + "green while it does, because they all construct it directly")
                .hasSize(1);
        assertThat(BinStorePlugin.class.getConstructors()[0].getParameterTypes())
                .as("and the one it has is the one PluginsService looks for: (Settings), "
                        + "which is what carries the node.name this state is keyed by "
                        + "(ADR-0048)")
                .containsExactly(Settings.class);
    }
}
