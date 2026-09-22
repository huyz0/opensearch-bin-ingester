// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
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
    private static final class NoOpTransport implements io.github.huyz0.os.biningester.client.SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(io.github.huyz0.os.biningester.format.RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) {
        }
    }

    private static List<String> capturing(Runnable action) {
        List<String> seen = new CopyOnWriteArrayList<>();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Logger logger = context.getLogger(BinStorePlugin.class.getName());
        CapturingAppender probe = new CapturingAppender(seen);
        org.apache.logging.log4j.Level oldLevel = logger.getLevel();
        logger.setLevel(org.apache.logging.log4j.Level.ALL);
        logger.addAppender(probe);
        probe.start();
        try {
            action.run();
        } finally {
            logger.removeAppender(probe);
            probe.stop();
            logger.setLevel(oldLevel);
        }
        return seen;
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> seen;

        CapturingAppender(List<String> seen) {
            super("bare-transport-log-probe", null, null, false, Property.EMPTY_ARRAY);
            this.seen = seen;
        }

        @Override
        public void append(LogEvent event) {
            seen.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
        }
    }

    @Test
    void aBARETransportRegistrarIsLOGGEDAsReachedByNoReconnect() {
        NodeSubscriptions bare = new NodeSubscriptions(new NoOpTransport(), 16);
        List<org.opensearch.cluster.ClusterStateListener> listeners = new ArrayList<>();

        List<String> seen = capturing(() ->
                BinStorePlugin.installRegistrar(bare, listeners::add, Runnable::run));

        assertThat(listeners).as("the premise: it still installs one").hasSize(1);
        assertThat(seen)
                .as("⚠️ ONE INFO LINE NAMES IT, greppable by what it means")
                .anySatisfy(message -> assertThat(message)
                        .startsWith("INFO ").contains("no reconnect"));
    }

    @Test
    void aCHANNELRegistrarLogsNOTHING() {
        NoOpTransport transport = new NoOpTransport();
        NodeSubscriptions channelled = new NodeSubscriptions(
                new NodeChannel(onReconnect -> transport), 16);

        List<String> seen = capturing(() -> BinStorePlugin.installRegistrar(channelled,
                new ArrayList<org.opensearch.cluster.ClusterStateListener>()::add,
                Runnable::run));

        assertThat(seen).as("the line is the bare branch's, not every node's").isEmpty();
    }
}
