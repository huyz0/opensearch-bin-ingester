// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

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
 * The node-local reader's BODY is bounded in time, not only its headers
 * (M10.22, harvested from `fb0f56b`).
 *
 * <p>⚠️ THE REQUEST TIMEOUT COVERS HEADERS ONLY: a reader that answers 200 and
 * then stalls mid-body held a Tier-2 read, and the node-wide monitor around
 * it, for as long as the loopback socket stayed open.
 */
class NodeLocalStoreReaderBodyDeadlineTest {

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

    @Test
    void aBodyThatStallsAfterItsHeadersFailsAtTheDeadline() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/object", exchange -> {
            exchange.sendResponseHeaders(200, 1_000);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(new byte[10]);
                out.flush();
                finish.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();
        Path secret = dir.resolve("secret");
        Files.write(secret, new byte[32]);
        Files.setPosixFilePermissions(secret, java.util.EnumSet.of(
                java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
        NodeLocalStoreReaderClient client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()), secret,
                Duration.ofSeconds(2), 1 << 20, Duration.ofMillis(500));

        CompletableFuture<Throwable> read = CompletableFuture.supplyAsync(() -> {
            try (InputStream in = client.get("bucket", "prefix", "prefix/ctl/log/0/x.delta")) {
                in.readAllBytes();
                return null;
            } catch (IOException e) {
                return e;
            }
        });

        assertThat(read.get(10, TimeUnit.SECONDS))
                .as("the stalled body fails at its deadline instead of holding the read")
                .isInstanceOf(IOException.class);
    }
}
