// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The node-local reader's body deadline, residue from M10.22's review
 * (M11.12, H7): the exact failure a stalled body surfaces, the refusal of a
 * deadline that bounds nothing, and a timer that does not outlive its body.
 */
class NodeLocalStoreReaderDeadlineResidueTest {

    @TempDir
    Path dir;

    private HttpServer server;
    private final CountDownLatch finish = new CountDownLatch(1);

    @AfterEach
    void stop() {
        finish.countDown();
        if (server != null) {
            server.stop(0);
        }
    }

    private Path secret() throws IOException {
        Path secret = dir.resolve("secret");
        Files.write(secret, new byte[32]);
        Files.setPosixFilePermissions(secret, java.util.EnumSet.of(
                java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
        return secret;
    }

    private URI serve(boolean stall) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/object", exchange -> {
            exchange.sendResponseHeaders(200, stall ? 1_000 : 10);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(new byte[10]);
                out.flush();
                if (stall) {
                    finish.await(30, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void aStalledBodyFailsNamingItsDeadline() throws Exception {
        NodeLocalStoreReaderClient client = new NodeLocalStoreReaderClient(serve(true),
                secret(), Duration.ofSeconds(2), 1 << 20, Duration.ofMillis(500));

        CompletableFuture<Throwable> read = CompletableFuture.supplyAsync(() -> {
            try (InputStream in = client.get("bucket", "prefix", "prefix/ctl/log/0/x.delta")) {
                in.readAllBytes();
                return null;
            } catch (IOException e) {
                return e;
            }
        });

        assertThat(read.get(10, TimeUnit.SECONDS))
                .as("⚠️ THE DEADLINE BY NAME, not the JDK's closed")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeded its PT0.5S deadline");
    }

    @Test
    void aDeadlineThatBoundsNothingIsRefused() throws Exception {
        URI endpoint = URI.create("http://127.0.0.1:1");
        Path secret = secret();
        for (Duration bad : new Duration[] {Duration.ZERO, Duration.ofMillis(-1)}) {
            assertThatThrownBy(() -> new NodeLocalStoreReaderClient(endpoint, secret,
                    Duration.ofSeconds(2), 1 << 20, bad))
                    .as("deadline %s", bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("deadline is positive");
        }
    }

    @Test
    void aBodyReadAndClosedInTimeLeavesNoTimerQueued() throws Exception {
        NodeLocalStoreReaderClient client = new NodeLocalStoreReaderClient(serve(false),
                secret(), Duration.ofSeconds(2), 1 << 20, Duration.ofHours(1));
        int before = NodeLocalStoreReaderClient.pendingDeadlines();

        try (InputStream in = client.get("bucket", "prefix", "prefix/ctl/log/0/x.delta")) {
            assertThat(NodeLocalStoreReaderClient.pendingDeadlines())
                    .as("the premise: the body's timer is queued").isEqualTo(before + 1);
            in.readAllBytes();
        }

        assertThat(NodeLocalStoreReaderClient.pendingDeadlines())
                .as("⚠️ CANCELLED AND GONE, not queued for the hour it was set for")
                .isEqualTo(before);
    }
}
