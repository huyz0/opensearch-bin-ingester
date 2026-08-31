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

    private final NodeSubscriptions subscriptions;

    /** Used by tests and by createComponents; the node owns the lifetime. */
    public BinStorePlugin(NodeSubscriptions subscriptions) {
        this.subscriptions = subscriptions;
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
