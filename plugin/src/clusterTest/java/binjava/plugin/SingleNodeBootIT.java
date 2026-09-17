// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import java.util.Collection;
import java.util.Map;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * ⚠️ T4: a REAL single-node OpenSearch cluster, started in-process.
 *
 * <p>This is the tier the milestone's acceptance criteria live in, and it is the
 * only one that can answer "searchable" — that is a property of Lucene and the
 * ingestion engine, not of our seams. Every other test in this repository stops
 * at the SPI boundary.
 *
 * <p>⚠️ A probe class asserting only that the framework was on the classpath
 * lived here while the six environment fixes below were being found, and was
 * REMOVED once a node actually booted: a node starting IS the stronger form of
 * that assertion, and a test that only names a class constrains nothing once a
 * test uses it.
 *
 * <p>⚠️ JUnit 4, because {@code OpenSearchSingleNodeTestCase} extends
 * {@code LuceneTestCase} and runs under RandomizedRunner. Written as Jupiter it
 * would compile and never execute.
 */
public class SingleNodeBootIT extends OpenSearchSingleNodeTestCase {

    static {
        // ⚠️ INSTALLED BEFORE THE NODE STARTS. OpenSearchSingleNodeTestCase
        // boots the node in setUp(), and the node constructs the plugin
        // reflectively during that -- so anything the plugin needs must already
        // be in place. A @Before would be too late.
        BinStorePlugin.install(node -> new NodeSubscriptions(new NoopTransport(), 16));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // ⚠️ The node constructs this ITSELF, reflectively. A plugin whose only
        // constructor takes its collaborators cannot be loaded here at all —
        // which is precisely the gap this test exists to close.
        return pluginList(BinStorePlugin.class);
    }

    public void testTheNodeStartsWithThePluginLoaded() {
        assertNotNull("a node must be running", client());
        // A trivial round trip proves the node is actually serving, not merely
        // constructed.
        client().admin().indices().prepareCreate("probe").get();
        assertTrue(client().admin().indices().prepareExists("probe").get().isExists());
    }

    public void testThePluginRegistersItsIngestionConsumerFactory() {
        // ⚠️ Constructed the way the NODE constructs it -- reflectively, from
        // this node's own settings -- so this exercises the path a real cluster
        // takes rather than a test-only one. `PluginsService` looks for one
        // public constructor taking (Settings, Path), then (Settings), then
        // none; the state has to be keyed by something the node supplies
        // (M6.13), and `node.name` is what it supplies.
        BinStorePlugin plugin = new BinStorePlugin(
                getInstanceFromNode(org.opensearch.cluster.service.ClusterService.class)
                        .getSettings());
        // ⚠️ THE BOOT IS WHAT PROVES THIS, NOT THE LINE BELOW, and review
        // measured the difference: reading `node.nodename` instead of
        // `node.name` fails BOTH cases in this class during setUp, with an NPE
        // inside `BinStoreConsumerFactory`, before this assertion is ever
        // reached. The line is a statement of the fact for a reader -- a real
        // node's settings carry `node.name`, which every T0 case takes on
        // faith because it builds the settings itself -- and the node
        // starting at all is the check.
        assertNotNull("the node's own settings key this plugin's state (ADR-0048)",
                plugin.subscriptions());
        Map<String, ?> factories = plugin.getIngestionConsumerFactories();
        assertEquals("BINSTORE", plugin.getType());
        assertTrue("the factory must be registered under getType()",
                factories.containsKey("BINSTORE"));
    }

    /** A transport that never delivers, for the registration check alone. */
    private static final class NoopTransport implements binjava.client.SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(binjava.format.RunKey key, Listener listener) {
            return () -> { };
        }
    }
}
