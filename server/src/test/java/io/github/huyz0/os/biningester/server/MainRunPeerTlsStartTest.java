// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The peer files are read BEFORE the node starts, and {@code off}'s warning is
 * the start's own (M13.52b review round 2, T6): a refused start leaves the
 * store untouched -- no lease taken, nothing written -- and an {@code off}
 * start warns, naming the four routes.
 */
class MainRunPeerTlsStartTest {

    @TempDir
    Path dir;

    private static String fixture(String name) throws Exception {
        return Path.of(MainRunPeerTlsStartTest.class.getResource("/peer-tls/" + name).toURI())
                .toString().replace('\\', '/');
    }

    private Path config(String... peer) throws Exception {
        Path store = dir.resolve("store");
        Path file = dir.resolve("node.properties");
        List<String> lines = new java.util.ArrayList<>(List.of(
                "pod.id=pod1", "pod.uid=uid-pod1", "pod.az=az-a", "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a", "store.kind=local-fs",
                "store.root=" + store.toString().replace('\\', '/'),
                "endpoint=http://localhost:0", "http.port=0",
                "producer.subject=producer-1", "producer.allowed-indices=logs"));
        lines.addAll(List.of(peer));
        Files.writeString(file, String.join("\n", lines));
        return file;
    }

    @Test
    void aREFUSEDStartLeavesTheStoreUntouched() throws Exception {
        Path config = config("peer.tls=mutual", "peer.port=0",
                "peer.tls.cert=" + fixture("pod1.pem"), "peer.tls.key=" + fixture("pod1.key"),
                "peer.tls.ca=" + dir.resolve("absent-ca.pem").toString().replace('\\', '/'));

        assertThatThrownBy(() -> Main.run(config.toString()))
                .isInstanceOf(ConfigurationException.class);

        // ⚠️ NOTHING STARTED: a node started first would hold the lease
        // under the store root for its TTL after the refusal.
        Path store = dir.resolve("store");
        long written = Files.exists(store)
                ? countFiles(store) : 0;
        assertThat(written).as("objects written under %s", store).isZero();
    }

    private static long countFiles(Path root) throws Exception {
        try (Stream<Path> all = Files.walk(root)) {
            return all.filter(Files::isRegularFile).count();
        }
    }

    @Test
    void anOFFStartWarnsNamingTheFourRoutes() throws Exception {
        List<String> warned = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(Main.class.getName());
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warned.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(capture);
        try (IngesterNode node = Main.run(config("peer.tls=off", "peer.port=0").toString())) {
            assertThat(node).isNotNull();
        } finally {
            logger.removeHandler(capture);
        }

        assertThat(warned).anySatisfy(message -> assertThat(message)
                .contains("/ctl/commit", "/ctl/drain", "/ctl/durable-segment", "/ctl/fast"));
    }
}
