// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.format.RunKey;
import java.util.Objects;
import java.util.UUID;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
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
     *
     * <p>WARNING: the rule itself moved to {@link RunKey#ofIndexUuid} at M7.2,
     * because the ingester now derives the same stream from the uuid a progress
     * frame carries. Two implementations of this mapping would not throw when
     * they disagreed -- they would produce two streams for one index, and a
     * watermark that never moves is data that is never deleted or data that is
     * deleted early, depending which way the disagreement falls.
     */
    static UUID indexUuidOf(String openSearchIndexUuid) {
        return RunKey.ofIndexUuid(openSearchIndexUuid, 0).indexId();
    }

    @Override
    public BinStoreOffset parsePointerFromString(String pointer) {
        return BinStoreOffset.fromString(pointer);
    }

    /**
     * The mapper shape this plugin's records are in, and the only one it can
     * serve (ADR-0020).
     *
     * <p>⚠️ IT IS A CLOSED ENUM UPSTREAM, so a plugin cannot register its own
     * mapper: whatever reaches {@code getPayload()} must already match one of
     * the three built-in shapes, and the envelope this project writes is the
     * DEFAULT one.
     */
    private static final IngestionMessageMapper.MapperType SERVED =
            IngestionMessageMapper.MapperType.DEFAULT;

    /** The setting an operator would have to change, named in the refusal. */
    static final String MAPPER_TYPE = "index.ingestion_source.mapper_type";

    /**
     * Refuses an index configured for a mapper shape this plugin's records are
     * not in (M6.8, FR-7).
     *
     * <p>⚠️ HERE, AND NOT PER RECORD. A mismatch discovered inside
     * {@code MessageProcessorRunnable} fails every batch with an exception that
     * it logs and SWALLOWS -- ADR-0020 records the shape and records that it
     * cost most of a session to find, because the index reports healthy and
     * holds nothing.
     *
     * <p>⚠️ AND THE REFUSAL NAMES THE SETTING AND THE VALUE, because the
     * operator's next question is what to change and to what.
     *
     * <p>⚠️ IT IS NOT THE ONE AN OPERATOR MEETS, and that is why
     * {@code BinStorePlugin.getIndexCreationValidators} calls it too.
     * {@code DefaultStreamPoller} CATCHES what this throws, logs it at WARN and
     * retries forever -- measured on a booting node: the index green, the shard
     * started, the warning every poll, nothing indexed. This check still earns
     * its place for an index created before the plugin was installed, or
     * restored from a snapshot into a cluster that has it -- never for a
     * settings update, because `mapper_type` is `Property.Final`. The
     * create-time validator is what fails where a human is looking.
     */
    static void refuseUnservedMapper(IndexMetadata indexMetadata) {
        IngestionSource source = indexMetadata.getIngestionSource();
        // ⚠️ SCOPED TO THIS PLUGIN'S OWN INGESTION TYPE. The create-time
        // validator is registered with `MetadataCreateIndexService` and runs on
        // EVERY index creation on the node, this plugin's or not: unscoped, a
        // node also running another ingestion plugin would have its
        // `ingestion_source.type: kafka` index refused by us, with a message
        // about OUR records and OUR ADR. The factory path cannot make that
        // mistake -- `createShardConsumer` only fires for BINSTORE -- and
        // hoisting the same function into a global hook is what loses it.
        if (source == null || !BinStorePlugin.TYPE.equalsIgnoreCase(source.getType())
                || source.getMapperType() == SERVED) {
            return;
        }
        throw new IllegalArgumentException("index " + indexMetadata.getIndex().getName()
                + " sets " + MAPPER_TYPE + "=" + source.getMapperType().getName()
                + ", and this plugin's records are in the " + SERVED.getName()
                + " mapper's shape and no other (ADR-0020) -- serving it anyway would fail "
                + "every batch inside a processor that logs and swallows, which presents as "
                + "an index that reports healthy and holds nothing");
    }

    @Override
    public BinStoreShardConsumer createShardConsumer(String clientId, int shardId,
            IndexMetadata indexMetadata) {
        refuseUnservedMapper(indexMetadata);
        // WARNING: the stream is (index UUID, shard), not (index NAME, shard).
        // An index deleted and recreated with the same name is a DIFFERENT
        // stream, and reusing the name would resume the new index from the old
        // one's offsets.
        RunKey key = new RunKey(indexUuidOf(indexMetadata.getIndexUUID()), shardId);
        return new BinStoreShardConsumer(shardId, key, subscriptions);
    }
}
