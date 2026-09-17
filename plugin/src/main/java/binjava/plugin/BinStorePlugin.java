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
     * WARNING: a NODE INSTANTIATES THIS REFLECTIVELY, so it must have a no-arg
     * constructor. An earlier version took NodeSubscriptions directly, which
     * made the class test-friendly and UNLOADABLE by a real OpenSearch node --
     * a gap that only a booting node reveals, because every in-process test
     * constructs the plugin by hand.
     *
     * <p>WARNING: the node-level state is therefore installed rather than
     * injected. In a real node {@code createComponents} builds it; in a test the
     * harness installs one before the node starts. It stays a per-node singleton
     * either way, which is what criterion 6 requires.
     */
    private static volatile NodeSubscriptions installed;

    private final NodeSubscriptions subscriptions;

    /** The only public constructor: the node calls this one reflectively. */
    public BinStorePlugin() {
        this.subscriptions = installed;
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

    /**
     * Installs the node-level subscriptions a reflectively-constructed plugin
     * will pick up.
     *
     * <p>WARNING: static, and that is a real cost. A node hosts ONE of these, so
     * it is correct per JVM in production and a shared fixture in tests. It is
     * the price of a plugin the node constructs itself, and it is recorded here
     * rather than hidden.
     */
    public static void install(NodeSubscriptions subscriptions) {
        installed = subscriptions;
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

    @Override
    public String getType() {
        return TYPE;
    }
}
