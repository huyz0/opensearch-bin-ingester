// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeLocalStoreReaderClientTest {
    @TempDir Path directory;

    @Test
    void readsSecretFromTheOwnerProtectedFileAndBoundsTheResponse() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/object", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] object = "exact".equals(exchange.getRequestHeaders().getFirst("X-Bin-Key"))
                    ? new byte[] {4, 5} : new byte[] {1, 2, 0};
            exchange.sendResponseHeaders(200, object.length);
            exchange.getResponseBody().write(object);
            exchange.close();
        });
        server.start();
        try (var client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()), secret,
                Duration.ofSeconds(2), 2);
                var response = client.get("bucket", "prefix", "prefix/allowed")) {
            assertThat(response.readNBytes(2)).containsExactly(1, 2);
            assertThatThrownBy(response::read).isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("exceeds");
            assertThat(authorization.get()).startsWith("Bearer ");
            try (var scalar = client.get("bucket", "prefix", "scalar")) {
                assertThat(scalar.read()).isEqualTo(1);
            }
            try (var exact = client.get("bucket", "prefix", "exact")) {
                assertThat(exact.read(new byte[0], 0, 0)).isZero();
                assertThat(exact.readNBytes(2)).containsExactly(4, 5);
                assertThat(exact.read()).isEqualTo(-1);
                assertThat(exact.read(new byte[1], 0, 1)).isEqualTo(-1);
            }
            try (var bulkOverflow = client.get("bucket", "prefix", "bulk")) {
                assertThat(bulkOverflow.readNBytes(2)).containsExactly(1, 2);
                assertThatThrownBy(() -> bulkOverflow.read(new byte[1], 0, 1))
                        .isInstanceOf(java.io.IOException.class).hasMessageContaining("exceeds");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void refusesRemoteEndpoints() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://example.com:8080"), directory.resolve("missing"),
                Duration.ofSeconds(1), 1024))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("loopback");
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://localhost:8080"), directory.resolve("missing"),
                Duration.ofSeconds(1), 1024))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("loopback");
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:8080/not-root"), secret,
                Duration.ofSeconds(1), 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsInclusiveResponseLimitAndRejectsValuesOutsideTheConfiguredRange() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        URI endpoint = URI.create("http://127.0.0.1:1");
        try (var minimum = new NodeLocalStoreReaderClient(endpoint, secret, Duration.ofSeconds(1), 1);
                var maximum = new NodeLocalStoreReaderClient(endpoint, secret, Duration.ofSeconds(1),
                        64L << 20)) {
            assertThat(minimum).isNotNull();
            assertThat(maximum).isNotNull();
        }
        var closed = new NodeLocalStoreReaderClient(endpoint, secret, Duration.ofSeconds(1), 1);
        closed.close();
        assertThatThrownBy(() -> closed.get("bucket", "prefix", "key"))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("closed");
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(endpoint, secret,
                Duration.ofSeconds(1), 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(endpoint, secret,
                Duration.ofSeconds(1), (64L << 20) + 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(URI.create("http://127.0.0.1:0"),
                secret, Duration.ofSeconds(1), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://user@127.0.0.1:8080"), secret, Duration.ofSeconds(1), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:8080/?redirect=1"), secret,
                Duration.ofSeconds(1), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:8080/#remote"), secret,
                Duration.ofSeconds(1), 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeLocalStoreReaderClient(
                URI.create("https://127.0.0.1:8080"), secret,
                Duration.ofSeconds(1), 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void statsPointerSizeAndDistinguishesMissingFromRefused() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/stat", exchange -> {
            int status = switch (exchange.getRequestHeaders().getFirst("X-Bin-Key")) {
                case "pointer" -> 200;
                case "missing" -> 404;
                default -> 403;
            };
            byte[] body = status == 200 ? "42".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
                    : new byte[0];
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (var client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()), secret,
                Duration.ofSeconds(2), 1024)) {
            assertThat(client.stat("bucket", "prefix", "pointer")).hasValue(42);
            assertThat(client.stat("bucket", "prefix", "missing")).isEmpty();
            assertThatThrownBy(() -> client.stat("bucket", "prefix", "segment"))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("403");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void getIfPresentTreats404AsAnEmptyChainSlot() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/object", exchange -> {
            int status = "missing".equals(exchange.getRequestHeaders().getFirst("X-Bin-Key"))
                    ? 404 : 200;
            try {
                if (status == 404) {
                    exchange.sendResponseHeaders(status, -1);
                } else {
                    byte[] body = "delta".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                    exchange.sendResponseHeaders(status, body.length);
                    exchange.getResponseBody().write(body);
                }
            } catch (java.io.IOException disconnected) {
                // A disconnected client may close the response before the server finishes.
            } finally {
                exchange.close();
            }
        });
        server.start();
        try (var client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()), secret,
                Duration.ofSeconds(2), 1024)) {
            assertThat(client.getIfPresent("bucket", "prefix", "missing")).isEmpty();
            assertThatThrownBy(() -> client.get("bucket", "prefix", "missing"))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("404");
            try (var body = client.getIfPresent("bucket", "prefix", "present").orElseThrow()) {
                assertThat(new String(body.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII))
                        .isEqualTo("delta");
            }
            class TrackedBody extends java.io.ByteArrayInputStream {
                boolean closed;
                TrackedBody() { super(new byte[] {1}); }
                @Override public void close() throws java.io.IOException {
                    closed = true;
                    super.close();
                }
            }
            TrackedBody tracked = new TrackedBody();
            assertThat(NodeLocalStoreReaderClient.missingResponse(tracked)).isEmpty();
            assertThat(tracked.closed).isTrue();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void refusesEmptyMalformedAndOversizedStatBodies() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/stat", exchange -> {
            String key = exchange.getRequestHeaders().getFirst("X-Bin-Key");
            byte[] body = switch (key) {
                case "empty" -> new byte[0];
                case "oversized" -> "1".repeat(21).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                default -> "not-a-size".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            };
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (var client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()), secret,
                Duration.ofSeconds(2), 1024)) {
            for (String key : new String[] {"empty", "malformed", "oversized"}) {
                assertThatThrownBy(() -> client.stat("bucket", "prefix", key))
                        .isInstanceOf(java.io.IOException.class);
            }
        } finally {
            server.stop(0);
        }
    }

}
