// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.client.InstallationSecret;
import io.github.huyz0.os.biningester.client.NodeLocalStoreReaderClient;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The installed client and executable reader exchange one bounded object across JVMs. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NodeLocalStoreReaderProcessIT {
    @TempDir Path directory;

    @Test
    void initSecretCommandCreatesAnOwnerOnlyInstallationSecret() throws Exception {
        Path secret = directory.resolve("reader.secret");
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.NodeLocalStoreReaderMain",
                "--init-secret", secret.toString())).start();
        assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(process.exitValue()).isZero();
        assertThat(Files.size(secret)).isEqualTo(32);
        assertThat(InstallationSecret.read(secret)).hasSize(32);
    }

    @Test
    void separateReaderUsesTheOwnerOnlySecretAndLoopbackOnlyStorePath() throws Exception {
        Path root = directory.resolve("store");
        Path secret = directory.resolve("reader.secret");
        Path config = directory.resolve("reader.properties");
        Path log = directory.resolve("reader.log");
        String prefix = "bins/reader-process";
        String bucket = "reader-bucket";
        String payload = "reader-process-payload";
        String key = new SegmentKey(prefix, 1_700_000_000_000L, "reader", 1, 48).key();
        String pointer = prefix + "/ctl/log/0/0000000000000001/ckpt/LATEST";
        InstallationSecret.create(secret);
        try (var store = LocalFsBinStore.at(root.toString())) {
            store.put(key, Body.ofBytes(payload.getBytes(StandardCharsets.UTF_8)));
            store.put(pointer, Body.ofBytes(new byte[] {4, 5}));
        }
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        Files.writeString(config, "reader.bind=127.0.0.1\nreader.port=" + port
                + "\nreader.bucket=" + bucket + "\nreader.secret-file=" + propertyPath(secret)
                + "\nreader.max-object-bytes=1024\nstore.kind=local-fs\nstore.root=" + propertyPath(root)
                + "\nstore.prefix=" + prefix + "\n", StandardCharsets.UTF_8);
        String parentUser = ProcessHandle.current().info().user().orElseThrow();
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.NodeLocalStoreReaderMain", config.toString()))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            byte[] received = null;
            try (var client = new NodeLocalStoreReaderClient(URI.create("http://127.0.0.1:" + port),
                    secret, Duration.ofSeconds(1), 1024)) {
                while (System.nanoTime() < deadline && process.isAlive()) {
                    try (var body = client.get(bucket, prefix, key)) {
                        received = body.readAllBytes();
                        break;
                    } catch (java.io.IOException starting) {
                        Thread.sleep(100);
                    }
                }
                assertThat(client.stat(bucket, prefix, pointer)).hasValue(2);
                assertThatThrownBy(() -> client.get("wrong-bucket", prefix, key))
                        .isInstanceOf(java.io.IOException.class).hasMessageContaining("403");
                assertThatThrownBy(() -> client.stat(bucket, prefix, key))
                        .isInstanceOf(java.io.IOException.class).hasMessageContaining("403");
            }
            assertThat(process.isAlive()).withFailMessage("reader exited: %s",
                    Files.readString(log, StandardCharsets.UTF_8)).isTrue();
            assertThat(ProcessHandle.of(process.pid()).orElseThrow().info().user()).contains(parentUser);
            assertThat(received).isEqualTo(payload.getBytes(StandardCharsets.UTF_8));
        } finally {
            process.destroy();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
        String output = Files.readString(log, StandardCharsets.UTF_8);
        assertThat(output).doesNotContain(payload);

        Path nonLoopback = directory.resolve("non-loopback.properties");
        Files.writeString(nonLoopback, Files.readString(config).replace("reader.bind=127.0.0.1",
                "reader.bind=0.0.0.0"), StandardCharsets.UTF_8);
        Process refused = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "io.github.huyz0.os.biningester.server.NodeLocalStoreReaderMain",
                nonLoopback.toString())).redirectErrorStream(true).start();
        assertThat(refused.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(refused.exitValue()).isNotZero();
    }

    private static String propertyPath(Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/');
    }
}
