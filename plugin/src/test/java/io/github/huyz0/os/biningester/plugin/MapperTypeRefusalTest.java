// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;

/**
 * An index this plugin cannot serve fails where an OPERATOR sees it (M6.8,
 * FR-7).
 *
 * <p>⚠️ THE FAILURE MODE BEING PREVENTED IS "ZERO DOCUMENTS INDEXED AND
 * NOTHING IN THE LOG". ADR-0020 records the shape: a mismatch between what the
 * payload IS and what the configured mapper expects fails every batch inside
 * {@code MessageProcessorRunnable}, which logs and swallows — an index that
 * reports healthy and holds nothing, which cost most of a session to find once
 * already.
 *
 * <p>⚠️ THE PAYLOAD IS THE {@code DEFAULT} MAPPER'S SHAPE AND NOTHING ELSE
 * (ADR-0020). {@code MapperType} is a closed enum, so this plugin cannot
 * register its own; what it can do is refuse, at shard creation, an index
 * configured for a shape its records are not in.
 */
class MapperTypeRefusalTest {

    private static final String UUID_1 = "nVzgup36TLqWp7VBBREj1w";

    private static final class NoopTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }
    }

    private static IndexMetadata index(String mapperType) {
        return index(mapperType, BinStorePlugin.TYPE);
    }

    private static IndexMetadata index(String mapperType, String sourceType) {
        Settings.Builder settings = Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.uuid", UUID_1)
                // ⚠️ SEGMENT replication, because pull-based ingestion refuses
                // DOCUMENT replication outright -- a fixture that leaves it out
                // fails inside OpenSearch's own settings validation and never
                // reaches this plugin at all.
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", sourceType)
                .put("index.ingestion_source.pointer.init.reset", "earliest");
        if (mapperType != null) {
            settings.put("index.ingestion_source.mapper_type", mapperType);
        }
        return IndexMetadata.builder("logs").settings(settings.build()).build();
    }

    private static BinStoreConsumerFactory factory() {
        return new BinStoreConsumerFactory(new NodeSubscriptions(new NoopTransport(), 16));
    }

    @Test
    void aMAPPERTypeThisPluginCannotServeIsREFUSEDAndTheMessageNamesTheSETTING() {
        assertThatThrownBy(() -> factory().createShardConsumer("client-1", 0,
                index("raw_payload")))
                .as("refused BEFORE any record is read -- rather than per record inside a "
                        + "processor that logs and swallows, which presents as an index that "
                        + "reports healthy and holds nothing (ADR-0020). What an operator "
                        + "actually meets is BinStorePlugin's creation validator calling this: "
                        + "the poller CATCHES what this throws and retries forever")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index.ingestion_source.mapper_type")
                .hasMessageContaining("raw_payload")
                .as("and it names what this plugin DOES serve, because the operator's next "
                        + "question is what to set instead")
                .hasMessageContaining("default");
    }

    @Test
    void theOTHERUnservedTypeIsREFUSEDToo() {
        assertThatThrownBy(() -> factory().createShardConsumer("client-1", 0,
                index("field_mapping")))
                .as("a refusal that names ONE unserved value leaves the other silently "
                        + "accepted, and MapperType has exactly three members -- the one this "
                        + "plugin's records are in, and two it cannot produce")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field_mapping");
    }

    @Test
    void theDEFAULTMapperIsSERVED() {
        assertThatCode(() -> factory().createShardConsumer("client-1", 0, index("default")))
                .as("PREMISE: the refusal is scoped to the types this plugin cannot serve. A "
                        + "guard that refused every index would be caught by every other "
                        + "cluster test, but only after they had all gone red for a reason "
                        + "none of them names")
                .doesNotThrowAnyException();
    }

    // ⚠️ AN INDEX THAT NAMES NO `mapper_type` HAS NO CASE OF ITS OWN, and that
    // is deliberate rather than an omission. OpenSearch resolves the absent
    // setting to DEFAULT before this plugin sees it, so such an index is
    // BYTE-FOR-BYTE the case above by the time it arrives -- no mutation can
    // tell the two apart, and a case that cannot be made to fail for its own
    // reason is coverage rather than verification (testing.md rules 8-10).


    /**
     * ANOTHER plugin's index is not this plugin's to refuse (M6.8 round 1).
     *
     * <p>⚠️ THE SAME FUNCTION IS REGISTERED AS AN INDEX-CREATION VALIDATOR, and
     * OpenSearch runs every plugin's validators on EVERY index creation. A
     * node that also runs another ingestion plugin would have its
     * {@code ingestion_source.type: kafka} index refused by us — with a message
     * about our records and our ADR — for a mapper type that is perfectly
     * correct for the plugin that actually serves it. The factory path cannot
     * make that mistake, because {@code createShardConsumer} only fires for
     * BINSTORE; hoisting the check into a global hook is what loses the scope.
     */
    @Test
    void anotherINGESTIONPluginsIndexIsNotThisPluginsToREFUSE() {
        assertThatCode(() -> BinStoreConsumerFactory.refuseUnservedMapper(
                index("field_mapping", "kafka")))
                .as("a mapper type this plugin cannot serve is none of its business on an "
                        + "index it does not ingest")
                .doesNotThrowAnyException();
    }

    /**
     * An index that is not pull-ingested at all is not this plugin's business
     * (M6.8 round 2).
     *
     * <p>⚠️ THE VALIDATOR RUNS ON EVERY INDEX CREATION ON THE NODE, and
     * {@code getIngestionSource()} is null for every ordinary index. Dropping
     * the null guard makes this plugin NPE on `PUT /my-index` — every index
     * creation in the cluster, for a check about ingestion.
     */
    @Test
    void anORDINARYIndexWithNoIngestionSourceAtAllIsNOTThisPluginsBusiness() {
        IndexMetadata ordinary = IndexMetadata.builder("ordinary")
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.uuid", UUID_1)
                        .build())
                .build();

        assertThatCode(() -> BinStoreConsumerFactory.refuseUnservedMapper(ordinary))
                .as("no ingestion source, no opinion: this runs on the create path of every "
                        + "index on the node, and a plugin that threw there would break index "
                        + "creation for a cluster that merely has it installed")
                .doesNotThrowAnyException();
    }
}
