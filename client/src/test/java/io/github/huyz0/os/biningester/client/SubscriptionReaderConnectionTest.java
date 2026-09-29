// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * M12.19c (M10.36 T1): a subscription reader's own client keeps its
 * connection alive across polls. A reader client built with keep-alive off --
 * a connect per poll, every 25 s per stream on the node -- passed every test.
 *
 * <p>⚠️ A RAW LOOPBACK SERVER, because what is under test is the CONNECTION:
 * it answers every request with an empty 200, the quiet poll, and records
 * which connection each came on.
 *
 * <p>⚠️ NOT PINNED HERE: that the connection is CLOSED when the subscription
 * ends (M10.35 T1). Closing a subscription also interrupts its reader, and
 * the outcome then depends on where the interrupt lands: blocked in a socket
 * read, the JDK closes the socket whatever the client does; between polls, the
 * connection was seen left open 10 s later in 3 of 7 runs of the real code.
 * Neither is a deterministic test of {@code closeResource}; M12.27 is the row.
 */
class SubscriptionReaderConnectionTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    private ServerSocket listening;
    private HttpSubscriptionTransport transport;
    private final AtomicInteger connections = new AtomicInteger();
    private final AtomicInteger requests = new AtomicInteger();
    private final List<String> closeHeaders = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() throws IOException {
        if (transport != null) {
            transport.close();
        }
        if (listening != null) {
            listening.close();
        }
    }

    private String serve() throws IOException {
        listening = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            while (!listening.isClosed()) {
                try {
                    Socket socket = listening.accept();
                    connections.incrementAndGet();
                    Thread.ofVirtual().start(() -> answer(socket));
                } catch (IOException closed) {
                    return;
                }
            }
        });
        return "http://127.0.0.1:" + listening.getLocalPort();
    }

    /** Answers every request on {@code socket} with an empty 200 until the peer closes it. */
    private void answer(Socket socket) {
        try (socket) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1));
            OutputStream out = socket.getOutputStream();
            while (true) {
                String line = in.readLine();
                if (line == null) {
                    break; // the peer closed its end
                }
                while (line != null && !line.isEmpty()) {
                    if (line.toLowerCase(java.util.Locale.ROOT).startsWith("connection:")) {
                        closeHeaders.add(line);
                    }
                    line = in.readLine();
                }
                requests.incrementAndGet();
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
            }
        } catch (IOException reset) {
            // a reset is a close as well
        }
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aReadersPollsShareOneKeptAliveConnection() throws Exception {
        transport = new HttpSubscriptionTransport(serve(), () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(200), Duration.ofSeconds(5));

        try (AutoCloseable subscription = transport.subscribe(STREAM, delivery -> { })) {
            await(() -> requests.get() >= 5, "five polls");
        }

        assertThat(connections.get())
                .as("⚠️ KEPT ALIVE (M10.36 T1): five polls or more on ONE connection, not a "
                        + "connect per poll")
                .isEqualTo(1);
        assertThat(closeHeaders).as("and none asked for the connection to be closed")
                .noneMatch(header -> header.toLowerCase(java.util.Locale.ROOT)
                        .contains("close"));

    }
}
