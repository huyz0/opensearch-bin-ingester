// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.Logger;
import org.junit.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/** The plugin's System.Logger records must reach the node's Log4j output. */
public class PluginLoggingIT extends OpenSearchSingleNodeTestCase {

    private static final FailingTransport TRANSPORT = new FailingTransport();

    static {
        BinStorePlugin.install(node -> new NodeSubscriptions(TRANSPORT, 16));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(BinStorePlugin.class);
    }

    @Test
    public void testThreePluginSystemLoggerPathsReachLog4j() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Logger root = context.getLogger(LogManager.ROOT_LOGGER_NAME);
        CapturingAppender appender = new CapturingAppender();
        org.apache.logging.log4j.Level oldLevel = root.getLevel();
        Logger binStoreLogger = context.getLogger(BinStorePlugin.class.getName());
        Logger registrarLogger = context.getLogger(IndexRegistrar.class.getName());
        Logger progressLogger = context.getLogger(ProgressReporter.class.getName());
        org.apache.logging.log4j.Level oldBinStoreLevel = binStoreLogger.getLevel();
        org.apache.logging.log4j.Level oldRegistrarLevel = registrarLogger.getLevel();
        org.apache.logging.log4j.Level oldProgressLevel = progressLogger.getLevel();
        root.addAppender(appender);
        root.setLevel(org.apache.logging.log4j.Level.ALL);
        binStoreLogger.setLevel(org.apache.logging.log4j.Level.ALL);
        registrarLogger.setLevel(org.apache.logging.log4j.Level.ALL);
        progressLogger.setLevel(org.apache.logging.log4j.Level.ALL);
        appender.start();
        try {
            createIndex("logs", Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.replication.type", "SEGMENT")
                    .put("index.ingestion_source.type", BinStorePlugin.TYPE)
                    .put("index.ingestion_source.mapper_type", "default")
                    .put("index.ingestion_source.pointer.init.reset", "earliest")
                    .put("index.ingestion_source.poll.timeout", 200)
                    .put("index.ingestion_source.error_strategy", "BLOCK")
                    .build());
            ensureGreen("logs");

            new ProgressReporter(TRANSPORT, () -> {
                throw new IllegalStateException("progress log probe");
            }).report();
            BinStorePlugin.installRegistrar(new NodeSubscriptions(TRANSPORT, 16),
                    ignored -> { }, Runnable::run);

            assertBusy(() -> org.assertj.core.api.Assertions.assertThat(appender.messages())
                    .anySatisfy(message -> org.assertj.core.api.Assertions.assertThat(message)
                            .contains("could not push the registration")));
        } finally {
            root.removeAppender(appender);
            appender.stop();
            root.setLevel(oldLevel);
            binStoreLogger.setLevel(oldBinStoreLevel);
            registrarLogger.setLevel(oldRegistrarLevel);
            progressLogger.setLevel(oldProgressLevel);
        }

        org.assertj.core.api.Assertions.assertThat(appender.messages())
                .anySatisfy(message -> org.assertj.core.api.Assertions.assertThat(message)
                        .contains("shard positions could not be read"));
        org.assertj.core.api.Assertions.assertThat(appender.messages())
                .anySatisfy(message -> org.assertj.core.api.Assertions.assertThat(message)
                        .contains("no reconnect reaches"));
    }

    private static final class FailingTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) {
            throw new IllegalStateException("registration log probe");
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("binstore-log-probe", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getLoggerName() + " " + event.getLevel() + " "
                    + event.getMessage().getFormattedMessage());
        }

        List<String> messages() {
            return List.copyOf(messages);
        }
    }
}
