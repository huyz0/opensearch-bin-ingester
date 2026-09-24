// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.client.InstallationSecret;
import io.github.huyz0.os.biningester.client.NodeLocalStoreReaderClient;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeLocalStoreReaderMainTest {
    @TempDir Path directory;

    @Test
    void executableAssemblyServesOnlyTheConfiguredLoopbackNamespace() throws Exception {
        String prefix = "bins/reader-main";
        String bucket = "reader-bucket";
        String key = new SegmentKey(prefix, 1_700_000_000_000L, "pod", 1, 48).key();
        String pointer = prefix + "/ctl/log/0/0000000000000001/ckpt/LATEST";
        Path root = directory.resolve("store");
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        try (var store = LocalFsBinStore.at(root.toString())) {
            store.put(key, Body.ofBytes("payload".getBytes(StandardCharsets.UTF_8)));
            store.put(pointer, Body.ofBytes(new byte[] {1, 2}));
        }
        Properties settings = new Properties();
        settings.setProperty("reader.bind", "127.0.0.1");
        settings.setProperty("reader.port", "0");
        settings.setProperty("reader.bucket", bucket);
        settings.setProperty("reader.secret-file", secret.toString());
        settings.setProperty("reader.max-object-bytes", "16");
        settings.setProperty("store.kind", "local-fs");
        settings.setProperty("store.root", root.toString());
        settings.setProperty("store.prefix", prefix);

        int port;
        try (var running = NodeLocalStoreReaderMain.start(settings);
                var client = new NodeLocalStoreReaderClient(
                        URI.create("http://127.0.0.1:" + running.port()), secret,
                        Duration.ofSeconds(2), 16);
                var body = client.get(bucket, prefix, key)) {
            port = running.port();
            assertThat(body.readAllBytes()).isEqualTo("payload".getBytes(StandardCharsets.UTF_8));
            assertThat(client.stat(bucket, prefix, pointer)).hasValue(2);
            assertThat(client.stat(bucket, prefix,
                    prefix + "/ctl/log/0/0000000000000002/ckpt/LATEST")).isEmpty();
            String missing = new SegmentKey(prefix, 1_700_000_000_001L, "pod", 2, 48).key();
            assertThatThrownBy(() -> client.get(bucket, prefix, missing))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("500");
            assertThatThrownBy(() -> client.stat(bucket, prefix, key))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("403");
            assertThatThrownBy(() -> client.get("other-bucket", prefix, key))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("403");
            try (var http = HttpClient.newHttpClient()) {
                for (String path : new String[] {"/v1/object", "/v1/stat"}) {
                    var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                            .POST(HttpRequest.BodyPublishers.noBody()).build();
                    assertThat(http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode())
                            .isEqualTo(405);
                }
            }
        }
        try (var afterClose = new NodeLocalStoreReaderClient(URI.create("http://127.0.0.1:" + port),
                secret, Duration.ofSeconds(1), 16)) {
            assertThatThrownBy(() -> afterClose.get(bucket, prefix, key))
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void refusesAnyConfiguredNonLoopbackAddressBeforeOpeningTheStore() {
        Properties settings = new Properties();
        settings.setProperty("reader.bind", "0.0.0.0");
        assertThatThrownBy(() -> NodeLocalStoreReaderMain.start(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("127.0.0.1");
    }

    @Test
    void refusesS3ReaderBucketMismatchBeforeOpeningTheStore() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        Properties settings = new Properties();
        settings.setProperty("reader.bind", "127.0.0.1");
        settings.setProperty("reader.port", "0");
        settings.setProperty("reader.bucket", "tenant-a");
        settings.setProperty("reader.secret-file", secret.toString());
        settings.setProperty("store.kind", "s3");
        settings.setProperty("store.bucket", "tenant-b");
        settings.setProperty("store.prefix", "bins/tenant-a");
        assertThatThrownBy(() -> NodeLocalStoreReaderMain.start(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reader.bucket").hasMessageContaining("store.bucket");
    }

    @Test
    void acceptsTheInclusivePortRangeAndRefusesValuesOutsideIt() {
        assertThat(NodeLocalStoreReaderMain.validatePort(0)).isZero();
        assertThat(NodeLocalStoreReaderMain.validatePort(65535)).isEqualTo(65535);
        assertThatThrownBy(() -> NodeLocalStoreReaderMain.validatePort(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NodeLocalStoreReaderMain.validatePort(65536))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
