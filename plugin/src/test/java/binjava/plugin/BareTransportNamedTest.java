// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/**
 * A node on the bare-transport {@code NodeSubscriptions} is NAMED in its log
 * (M8.35, FR-16).
 *
 * <p>⚠️ **ITS REGISTRAR IS REACHED BY NO RECONNECT**, correctly: a bare
 * transport has no reconnect to wire. But that is M6.15's failure, a node that
 * never re-pushes after the ingester restarts, and without a line saying so an
 * operator cannot tell which deployment has it.
 */
class BareTransportNamedTest {

    /** Accepts everything and does nothing: only the branch is under test. */
    private static final class NoOpTransport implements binjava.client.SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(binjava.format.RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(binjava.format.IndexRegistration registration) {
        }
    }

    private static List<LogRecord> capturing(Runnable action) {
        List<LogRecord> seen = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(BinStorePlugin.class.getName());
        Handler probe = new Handler() {
            @Override
            public void publish(LogRecord record) {
                seen.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(probe);
        try {
            action.run();
        } finally {
            logger.removeHandler(probe);
        }
        return seen;
    }

    @Test
    void aBARETransportRegistrarIsLOGGEDAsReachedByNoReconnect() {
        NodeSubscriptions bare = new NodeSubscriptions(new NoOpTransport(), 16);
        List<org.opensearch.cluster.ClusterStateListener> listeners = new ArrayList<>();

        List<LogRecord> seen = capturing(() ->
                BinStorePlugin.installRegistrar(bare, listeners::add, Runnable::run));

        assertThat(listeners).as("the premise: it still installs one").hasSize(1);
        assertThat(seen)
                .as("⚠️ ONE INFO LINE NAMES IT, greppable by what it means")
                .anySatisfy(r -> {
                    assertThat(r.getLevel()).isEqualTo(Level.INFO);
                    assertThat(r.getMessage()).contains("no reconnect");
                });
    }

    @Test
    void aCHANNELRegistrarLogsNOTHING() {
        NoOpTransport transport = new NoOpTransport();
        NodeSubscriptions channelled = new NodeSubscriptions(
                new NodeChannel(onReconnect -> transport), 16);

        List<LogRecord> seen = capturing(() -> BinStorePlugin.installRegistrar(channelled,
                new ArrayList<org.opensearch.cluster.ClusterStateListener>()::add,
                Runnable::run));

        assertThat(seen).as("the line is the bare branch's, not every node's").isEmpty();
    }
}
