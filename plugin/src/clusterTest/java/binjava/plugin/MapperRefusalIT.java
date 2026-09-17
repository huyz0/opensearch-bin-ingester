// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import java.util.Collection;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * T4 — an index this plugin cannot serve fails where an OPERATOR sees it
 * (M6.8, FR-7, M6 criterion 4).
 *
 * <p>⚠️ THE FAILURE MODE BEING PREVENTED IS "ZERO DOCUMENTS INDEXED AND NOTHING
 * IN THE LOG". ADR-0020 records the shape and records that it cost most of a
 * session to find: a mismatch between what the payload IS and what the mapper
 * expects fails every batch inside {@code MessageProcessorRunnable}, which logs
 * it and swallows it. The index is green, the shard is started, and it holds
 * nothing.
 *
 * <p>⚠️ AND IT IS ASSERTED ON A BOOTING NODE BECAUSE IT IS INVISIBLE AT THE SPI
 * BOUNDARY — which this case MEASURED rather than assumed. With the refusal
 * only in {@code createShardConsumer}, the index went GREEN and held nothing:
 * {@code DefaultStreamPoller} catches what the factory throws, logs "Failed to
 * create consumer for shard 0" at WARN, and retries every poll, forever. That
 * is exactly ADR-0020's failure mode one level up from where ADR-0020 found it.
 * So the refusal an operator meets is an index-creation validator, and this
 * case asserts the CREATE fails.
 *
 * <p>⚠️ JUnit 4, because {@code OpenSearchSingleNodeTestCase} extends
 * {@code LuceneTestCase} and runs under RandomizedRunner. Written as Jupiter it
 * would compile and never execute — and {@code check-tdd} does not see these
 * methods either (AGENTS.md § Gates records that blind spot), so the red for
 * this one was observed by hand: with the refusal removed, the index below goes
 * GREEN and holds nothing.
 */
public class MapperRefusalIT extends OpenSearchSingleNodeTestCase {

    static {
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));
    }

    private static final class NoopTransport implements binjava.client.SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(binjava.format.RunKey key, Listener listener) {
            return () -> { };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    public void testAnUnservedMapperTypeIsREFUSEDAtIndexCREATION() {
        Exception refused = expectThrows(Exception.class, () ->
                client().admin().indices().prepareCreate("poison")
                        .setSettings(poisonSettings("raw_payload"))
                        .get());

        String message = String.valueOf(refused.getMessage())
                + String.valueOf(refused.getCause() == null ? "" : refused.getCause()
                        .getMessage());
        assertTrue("the CREATE fails, where a human is looking -- found: " + message,
                message.contains(BinStoreConsumerFactory.MAPPER_TYPE));
        assertTrue("and it names the value that was set, because the operator's next question "
                        + "is what to change and to what -- found: " + message,
                message.contains("raw_payload"));
        assertFalse("and no index is left behind half-created",
                client().admin().indices().prepareExists("poison").get().isExists());
    }

    public void testTheSERVEDMapperTypeStillCreates() {
        client().admin().indices().prepareCreate("served")
                .setSettings(poisonSettings("default")).get();

        assertTrue("PREMISE: the validator refuses the MAPPER TYPE and not pull-based "
                        + "ingestion itself -- a validator that refused everything would make "
                        + "the case above pass for the wrong reason",
                client().admin().indices().prepareExists("served").get().isExists());
    }

    private static Settings poisonSettings(String mapperType) {
        return Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.replication.type", "SEGMENT")
                .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                // ⚠️ A VALID VALUE OF A CLOSED ENUM, not a typo: core refuses a
                // misspelling itself, so a case using one would assert
                // OpenSearch's own validation rather than this plugin's.
                .put("index.ingestion_source.mapper_type", mapperType)
                .put("index.ingestion_source.pointer.init.reset", "earliest")
                .put("index.ingestion_source.poll.timeout", 200)
                .put("index.ingestion_source.error_strategy", "BLOCK")
                .build();
    }
}
