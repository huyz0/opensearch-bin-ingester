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
        // ⚠️ THE PRODUCTION CREATOR, NOT A HAND-ROLLED POSIX CHMOD (M12.25): it
        // falls back to an owner-only ACL where the file system has no POSIX
        // view, which a direct setPosixFilePermissions throws on (NTFS).
        InstallationSecret.create(secret);
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
        // ⚠️ ONLY DEADLINES OVER HALF AN HOUR OUT ARE COUNTED (M12.21, M11.12 T2):
        // the executor is JVM-wide, and another test's short timer running or
        // cancelled between two reads moved an unfiltered count.
        int before = NodeLocalStoreReaderClient.pendingDeadlines(Duration.ofMinutes(30));
        // ⚠️ A SHORT TIMER QUEUED BETWEEN THE READS, as another test's would be
        // (review T1): the filter is the fix, and without this nothing on the
        // queue could tell a filtered count from the whole queue's.
        NodeLocalStoreReaderClient shortLived = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:1"), dir.resolve("secret"), Duration.ofSeconds(2),
                1 << 20, Duration.ofMinutes(1));
        InputStream otherBody = shortLived.deadlined(new java.io.ByteArrayInputStream(new byte[0]));

        try (otherBody; InputStream in = client.get("bucket", "prefix",
                "prefix/ctl/log/0/x.delta")) {
            assertThat(NodeLocalStoreReaderClient.pendingDeadlines(Duration.ofMinutes(30)))
                    .as("the premise: the body's timer is queued, the short one not counted")
                    .isEqualTo(before + 1);
            in.readAllBytes();
            in.close();

            assertThat(NodeLocalStoreReaderClient.pendingDeadlines(Duration.ofMinutes(30)))
                    .as("⚠️ CANCELLED AND GONE, not queued for the hour it was set for")
                    .isEqualTo(before);
        }
    }

    /**
     * ⚠️ M12.17 (M11.12 R1): the deadline was recognised by its TEXT -- a
     * failure whose message held " deadline" was taken for this client's own
     * and passed through -- so after expiry any other IOException carrying
     * that word (a socket's read deadline, a proxy's) surfaced unnamed. The
     * type is the test now, not the text.
     */
    @Test
    void anotherFailureMentioningADeadlineAfterExpiryIsStillNamedAsTheBodyDeadline()
            throws Exception {
        NodeLocalStoreReaderClient client = new NodeLocalStoreReaderClient(
                URI.create("http://127.0.0.1:1"), secret(), Duration.ofSeconds(2), 1 << 20,
                Duration.ofMillis(50));
        CountDownLatch closed = new CountDownLatch(1);
        InputStream body = new InputStream() {
            @Override
            public int read() throws IOException {
                try {
                    closed.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IOException("socket read deadline passed");
            }

            @Override
            public void close() {
                closed.countDown();
            }
        };

        try (InputStream in = client.deadlined(body)) {
            assertThatThrownBy(in::read)
                    .as("⚠️ AFTER EXPIRY THE BODY DEADLINE IS NAMED, whatever the cause says")
                    .isInstanceOf(NodeLocalStoreReaderClient.BodyDeadlineException.class)
                    .hasMessageContaining("exceeded its PT0.05S deadline")
                    .hasCauseInstanceOf(IOException.class);
        }
    }
}
