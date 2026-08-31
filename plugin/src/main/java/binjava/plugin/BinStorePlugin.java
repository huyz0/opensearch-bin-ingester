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

    @Override
    public String getType() {
        return TYPE;
    }
}
