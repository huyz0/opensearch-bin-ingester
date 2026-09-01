// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.format.RunKey;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Makes one shard consumer per shard, all sharing the node's subscriptions.
 *
 * <p>WARNING: the factory is called ONCE PER SHARD, so it must not own anything
 * node-scoped. It is handed {@link NodeSubscriptions} rather than building one,
 * which is what keeps 100 shards from opening 100 subscriptions (criterion 6).
 *
 * <p>WARNING: only the {@code IndexMetadata} overload is implemented. The older
 * {@code initialize(IngestionSource)} + {@code createShardConsumer(clientId, shardId)}
 * two-step is deprecated forRemoval upstream, and the metadata overload is the
 * one that carries the index UUID this plugin needs to derive stream identity.
 */
public final class BinStoreConsumerFactory
        implements IngestionConsumerFactory<BinStoreShardConsumer, BinStoreOffset> {

    private final NodeSubscriptions subscriptions;

    public BinStoreConsumerFactory(NodeSubscriptions subscriptions) {
        this.subscriptions = Objects.requireNonNull(subscriptions, "subscriptions");
    }

    /**
     * WARNING: an OpenSearch index UUID is BASE64URL, not the hyphenated form.
     * {@code UUID.fromString} throws on every real index -- "Invalid UUID
     * string: nVzgup36TLqWp7VBBREj1w" -- and no in-process test could catch it,
     * because they all build a RunKey directly from a java.util.UUID. Only a
     * booting node hands over a real one.
     */
    static UUID indexUuidOf(String openSearchIndexUuid) {
        byte[] raw = Base64.getUrlDecoder().decode(openSearchIndexUuid);
        if (raw.length != 16) {
            throw new IllegalArgumentException(
                    "an index UUID decodes to 16 bytes, not " + raw.length);
        }
        ByteBuffer b = ByteBuffer.wrap(raw);
        return new UUID(b.getLong(), b.getLong());
    }

    @Override
    public BinStoreOffset parsePointerFromString(String pointer) {
        return BinStoreOffset.fromString(pointer);
    }

    @Override
    public BinStoreShardConsumer createShardConsumer(String clientId, int shardId,
            IndexMetadata indexMetadata) {
        // WARNING: the stream is (index UUID, shard), not (index NAME, shard).
        // An index deleted and recreated with the same name is a DIFFERENT
        // stream, and reusing the name would resume the new index from the old
        // one's offsets.
        RunKey key = new RunKey(indexUuidOf(indexMetadata.getIndexUUID()), shardId);
        return new BinStoreShardConsumer(shardId, key, subscriptions);
    }
}
