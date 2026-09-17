// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import java.util.Map;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

/**
 * Registers this ingestion source with an OpenSearch node.
 *
 * <p>WARNING: extends {@code Plugin} AS WELL AS implementing
 * {@code IngestionConsumerPlugin}, because node-level shared state has nowhere
 * else to live: the factory is called once per shard, so a subscription or cache
 * built there is multiplied by the shard count (criterion 6, cost rule R5).
 *
 * <p>The index opts in with:
 * <pre>
 * index.ingestion_source.type: BINSTORE
 * index.ingestion_source.mapper_type: default
 * </pre>
 */
public final class BinStorePlugin extends Plugin implements IngestionConsumerPlugin {

    /** The value of {@code index.ingestion_source.type}. */
    public static final String TYPE = "BINSTORE";

    /**
     * How a deployment says what a node's subscriptions ARE, keyed by the node
     * that asks (M6.13).
     *
     * <p>⚠️ A FACTORY RATHER THAN AN INSTANCE, and that is the whole of this
     * task. The field it replaces was one {@code NodeSubscriptions} per JVM,
     * which is correct in production -- a JVM is a node -- and WRONG wherever
     * two nodes share a process: {@code InternalTestCluster} runs every node of
     * a cluster in one, so "each node holds ONE subscription" reads as 1
     * however many nodes there are, and M6.9's multi-node criterion becomes
     * unmeasurable rather than merely unmeasured. Handing out instances in
     * INSTALL ORDER would pass every deterministic boot and hand node B node
     * A's subscription the moment two start at once.
     *
     * <p>⚠️ IT IS STILL STATIC, because a node constructs this plugin
     * REFLECTIVELY and nothing can be passed in. What changed is that the
     * static is now a factory and a map keyed by the node's own name, so the
     * STATE is per node even though the entry point cannot be.
     */
    private static volatile java.util.function.Function<String, NodeSubscriptions> factory;

    /**
     * ⚠️ KEYED BY {@code node.name}, WHICH THE NODE SUPPLIES. OpenSearch puts
     * it in the settings it hands the plugin's constructor, so the key is
     * something the node says rather than something a fixture assumed -- and
     * {@code computeIfAbsent} means one node asking twice, as it does across a
     * restart, gets the same subscription rather than a second subscriber the
     * first one's deliveries still reach.
     */
    private static final java.util.Map<String, NodeSubscriptions> PER_NODE =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final NodeSubscriptions subscriptions;

    /**
     * The only public constructor: the node calls this one reflectively.
     *
     * <p>WARNING: it takes {@code Settings} rather than nothing, and
     * {@code PluginsService} supports exactly this -- it looks for a single
     * public constructor taking {@code (Settings, Path)}, then {@code
     * (Settings)}, then none. A plugin with TWO public constructors is refused
     * outright ("no unique public constructor"), which is why the test-only one
     * below is package-private.
     *
     * <p>⚠️ AND THE SETTINGS ARE WHAT MAKE THE STATE PER NODE. With a no-arg
     * constructor the plugin has nothing to key by, and the only thing left is
     * call order -- see {@link #factory}.
     */
    public BinStorePlugin(org.opensearch.common.settings.Settings settings) {
        java.util.function.Function<String, NodeSubscriptions> installed = factory;
        String nodeName = settings == null ? "" : settings.get("node.name", "");
        // ⚠️ A NODE WITH NO NAME GETS NOTHING rather than a shared default:
        // keying two unnamed nodes together is the static this task removes,
        // wearing a default's clothes. Every real node has one.
        this.subscriptions = installed == null || nodeName.isEmpty()
                ? null
                : PER_NODE.computeIfAbsent(nodeName, installed);
    }

    /**
     * Used by in-process tests that own the lifetime themselves.
     *
     * <p>WARNING: PACKAGE-PRIVATE, not public. OpenSearch requires exactly ONE
     * public constructor and refuses to load a plugin otherwise -- "no unique
     * public constructor". A second public one leaves every in-process test
     * green while making the plugin unloadable by a real node; only booting one
     * finds it.
     */
    BinStorePlugin(NodeSubscriptions subscriptions) {
        this.subscriptions = subscriptions;
    }

    /** What this node's plugin instance holds, or null where none was installed. */
    NodeSubscriptions subscriptions() {
        return subscriptions;
    }

    /**
     * Installs the factory a reflectively-constructed plugin will ask for its
     * node's subscriptions.
     *
     * <p>⚠️ CALLED ONCE PER DEPLOYMENT, BEFORE ITS NODES START, and asked once
     * per NODE. In production a JVM is one node and the distinction costs
     * nothing; in a test cluster it is the difference between measuring
     * per-node behaviour and measuring one shared object eight times.
     *
     * <p>⚠️ AND IT FORGETS THE PREVIOUS INSTALLATION'S NODES, because "once per
     * process" is exactly what a test JVM does NOT do: eight IT classes install
     * from static initialisers in one JVM, nothing forks per class, and
     * {@code OpenSearchSingleNodeTestCase} reuses the node name -- so without
     * the clear the second class's node is handed the first class's transport.
     * MEASURED: deleting it leaves {@code :plugin:test} green and fails
     * {@code :plugin:clusterTest}.
     */
    public static void install(java.util.function.Function<String, NodeSubscriptions> factory) {
        BinStorePlugin.factory = factory;
        PER_NODE.clear();
    }

    /**
     * Forgets the factory and every node's state -- for a test that owns both.
     *
     * <p>⚠️ PACKAGE-PRIVATE, like the test-only constructor and for the same
     * reason: nothing outside {@code binjava.plugin} has any business tearing a
     * running node's subscriptions out from under it, and both test source sets
     * are in this package.
     */
    static void uninstall() {
        factory = null;
        PER_NODE.clear();
    }

    /**
     * WARNING: the raw {@code IngestionConsumerFactory} is UPSTREAM's signature,
     * not a slip here -- {@code IngestionConsumerPlugin} declares
     * {@code Map<String, IngestionConsumerFactory>} with no type arguments, and
     * parameterising it would fail to override. Suppressed on this one method
     * rather than relaxing -Werror for the module.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new BinStoreConsumerFactory(subscriptions));
    }

    /**
     * Starts the registration push for this node (M6.7, FR-16).
     *
     * <p>⚠️ HERE AND NOT IN {@code createShardConsumer}, which runs once per
     * SHARD: a listener built there pushes one copy of an index's shape per
     * shard of it this node holds, and every other M6 criterion stays green
     * while it does. {@code RegistrationPushTest} counts at K = 1 and K = 8.
     *
     * <p>⚠️ NOTHING IS RETURNED TO THE NODE. The registrar has no lifecycle of
     * its own -- it holds no clock, no socket and no thread, and pushes only
     * when the cluster tells it something changed -- so registering it as a
     * listener is the whole of its installation.
     *
     * <p>⚠️ IT IS HANDED NO NODE ID. {@code clusterService.localNode()} is not
     * answerable here -- this runs while the node is being built, before it has
     * joined anything -- and every cluster-state event carries the local node's
     * id anyway.
     *
     * <p>⚠️ THE PUSHES RUN ON THE GENERIC POOL, NOT ON THE APPLIER THREAD that
     * delivers the event. They are network calls with up to three attempts
     * each, and the applier thread applies every cluster-state update on this
     * node -- allocation, mappings, the ack to the cluster manager.
     */
    @Override
    public java.util.Collection<Object> createComponents(
            org.opensearch.transport.client.Client client,
            org.opensearch.cluster.service.ClusterService clusterService,
            org.opensearch.threadpool.ThreadPool threadPool,
            org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
            org.opensearch.script.ScriptService scriptService,
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.env.Environment environment,
            org.opensearch.env.NodeEnvironment nodeEnvironment,
            org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositories) {
        installRegistrar(subscriptions, clusterService::addListener,
                threadPool.generic());
        return java.util.List.of();
    }

    /**
     * Registers this node's {@link IndexRegistrar}, or does nothing when this
     * deployment installed no subscriptions.
     *
     * <p>⚠️ PACKAGE-PRIVATE AND TAKING THE SINK RATHER THAN THE
     * {@code ClusterService}, so a T0 case can assert that a listener really is
     * added. Review MEASURED the alternative: deleting the body of
     * {@code createComponents} left `:plugin:test` green, because every case
     * built an {@code IndexRegistrar} by hand and nothing named the wiring --
     * so the whole of FR-16 could have shipped unregistered with the suite
     * green.
     *
     * <p>⚠️ A NODE WITH NO INSTALLED SUBSCRIPTIONS PUSHES NOTHING, and that is
     * the deployment that has not configured this plugin. Building a registrar
     * with no transport would refuse on the first cluster-state change of every
     * such node.
     */
    static void installRegistrar(NodeSubscriptions subscriptions,
            java.util.function.Consumer<org.opensearch.cluster.ClusterStateListener> sink,
            java.util.concurrent.Executor pusher) {
        if (subscriptions == null) {
            return;
        }
        sink.accept(new IndexRegistrar(subscriptions.transport(), pusher));
    }

    /**
     * Refuses an index configured for a mapper shape this plugin's records are
     * not in, AT CREATION (M6.8, FR-7).
     *
     * <p>⚠️ THE FACTORY'S OWN REFUSAL IS NOT ENOUGH, and a booting node is what
     * showed it. {@code DefaultStreamPoller} CATCHES whatever
     * {@code createShardConsumer} throws, logs "Failed to create consumer for
     * shard 0" at WARN, and retries -- forever. MEASURED on this tree before
     * this validator existed: the index went GREEN, the shard reported started,
     * the WARN repeated every poll, and nothing was ever indexed. That is
     * ADR-0020's "zero documents indexed and nothing in the log" arriving one
     * level up from where ADR-0020 found it.
     *
     * <p>⚠️ SO THE REFUSAL IS MOVED TO WHERE A HUMAN IS LOOKING: the create
     * request itself fails, with the setting and the value in the response.
     * The factory keeps its own check -- an index can be created before this
     * plugin is installed, or restored from a snapshot into a cluster that has
     * it -- but the one an operator meets is this one. (Not a settings update:
     * `mapper_type` is `Property.Final`.)
     */
    @Override
    public java.util.Collection<org.opensearch.index.IndexCreationValidator>
            getIndexCreationValidators() {
        return java.util.List.of((mapperService, indexSettings) ->
                BinStoreConsumerFactory.refuseUnservedMapper(indexSettings.getIndexMetadata()));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
