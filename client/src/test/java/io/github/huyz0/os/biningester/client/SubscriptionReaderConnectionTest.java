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
 * <p>⚠️ AND CLOSED WHEN THE SUBSCRIPTION ENDS (M13.4, M12.27, M10.35 T1).
 * Closing a subscription used to interrupt its reader, and an interrupt
 * landing mid-poll abandoned that poll's connection outside the reader's
 * cache, open until GC or the ingester's idle timeout. The close now only
 * stops the reader, so the cases below close many subscriptions mid-poll and
 * demand every connection closed by the CLIENT -- and, since the poll in hand
 * now completes after the close, that its answer is delivered to nobody.
 */
class SubscriptionReaderConnectionTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    private ServerSocket listening;
    private HttpSubscriptionTransport transport;
    private final AtomicInteger connections = new AtomicInteger();
    private final AtomicInteger requests = new AtomicInteger();
    /** Connections whose peer, the client, closed or reset them. */
    private final AtomicInteger closedByClient = new AtomicInteger();
    /** When set, the request numbered {@link #parkAt} waits for it, then gets {@link #parkedBody}. */
    private volatile java.util.concurrent.CountDownLatch parked;
    private volatile int parkAt;
    private volatile byte[] parkedBody = new byte[0];
    /** Requests whose line contains this are refused 503, as an ingester that cannot serve. */
    private volatile String refusedPath;
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
                String requestLine = line;
                while (line != null && !line.isEmpty()) {
                    if (line.toLowerCase(java.util.Locale.ROOT).startsWith("connection:")) {
                        closeHeaders.add(line);
                    }
                    line = in.readLine();
                }
                int number = requests.incrementAndGet();
                String refuse = refusedPath;
                if (refuse != null && requestLine.contains(refuse)) {
                    out.write("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
                            .getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                    continue;
                }
                java.util.concurrent.CountDownLatch park = parked;
                byte[] body = new byte[0];
                if (park != null && number == parkAt) {
                    park.await();
                    body = parkedBody;
                }
                out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length + "\r\n\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                out.write(body);
                out.flush();
            }
        } catch (IOException reset) {
            // a reset is a close as well
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        }
        closedByClient.incrementAndGet();
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

    /** How many subscriptions {@link #everyClosedSubscriptionsConnectionIsClosedByTheClient} ends. */
    private static final int CYCLES = 30;

    @Test
    void everyClosedSubscriptionsConnectionIsClosedByTheClient() throws Exception {
        transport = new HttpSubscriptionTransport(serve(), () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(200), Duration.ofSeconds(5));

        for (int cycle = 0; cycle < CYCLES; cycle++) {
            int before = requests.get();
            AutoCloseable subscription = transport.subscribe(STREAM, delivery -> { });
            await(() -> requests.get() >= before + 2, "two polls in cycle " + cycle);
            subscription.close();
            int opened = connections.get();
            int finalCycle = cycle;
            await(() -> closedByClient.get() >= opened, "cycle " + finalCycle
                    + ": every connection the reader opened closed by the client, not left for"
                    + " the ingester's idle timeout (M12.27)");
        }
    }

    /**
     * ⚠️ THE POLL IN HAND COMPLETES AFTER THE CLOSE NOW (M13.4 review T1), and
     * its answer must reach nobody -- stopped by the check before the
     * reconnect block, or failing that by `deliver`'s per-frame one, each
     * pinned alone below. The same answer to an OPEN subscription is delivered
     * first, so the frame is proved valid and the closed case cannot pass on
     * a frame that would not have decoded.
     */
    @Test
    void aPollAnsweredAfterItsSubscriptionClosedDeliversNothing() throws Exception {
        transport = new HttpSubscriptionTransport(serve(), () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(200), Duration.ofSeconds(5));
        parkedBody = frame(new io.github.huyz0.os.biningester.format.SubscriptionEvent("s", 1L,
                1L, STREAM, "seg/m13.4", 0L, 1, io.github.huyz0.os.biningester.format.FetchMode.INLINE,
                new byte[] {1}));

        AtomicInteger openDeliveries = new AtomicInteger();
        parkAt = 2;
        parked = new java.util.concurrent.CountDownLatch(1);
        try (AutoCloseable open = transport.subscribe(STREAM,
                delivery -> openDeliveries.incrementAndGet())) {
            await(() -> requests.get() >= 2, "the open subscription's parked poll");
            parked.countDown();
            await(() -> openDeliveries.get() == 1, "the premise: the answer is delivered to an "
                    + "open subscription");
        }

        await(() -> closedByClient.get() == connections.get(),
                "the open subscription's connection closed before the next part");
        AtomicInteger closedDeliveries = new AtomicInteger();
        int base = requests.get();
        int closedBefore = closedByClient.get();
        parkAt = base + 2;
        parked = new java.util.concurrent.CountDownLatch(1);
        AutoCloseable closing = transport.subscribe(STREAM,
                delivery -> closedDeliveries.incrementAndGet());
        await(() -> requests.get() >= base + 2, "the closing subscription's parked poll");
        closing.close();
        parked.countDown();
        await(() -> closedByClient.get() > closedBefore, "the reader read the answer and closed");

        assertThat(closedDeliveries).as("an answer read after the close reaches no listener")
                .hasValue(0);
    }

    /**
     * ⚠️ `deliver`'S PER-FRAME STOP CHECK, ALONE (M13.4 review round 2 T1): a
     * listener closes its subscription on the first of two frames in one
     * answer, and the second is not delivered. The check before the reconnect
     * block cannot stop this one: the answer arrived while still open.
     */
    @Test
    void aFrameAfterTheListenerClosedItsSubscriptionIsNotDelivered() throws Exception {
        transport = new HttpSubscriptionTransport(serve(), () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(200), Duration.ofSeconds(5));
        byte[] one = frame(event("seg/m13.4-a"));
        byte[] two = frame(event("seg/m13.4-b"));
        parkedBody = java.nio.ByteBuffer.allocate(one.length + two.length).put(one).put(two)
                .array();
        parkAt = 2;
        parked = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        AutoCloseable[] self = new AutoCloseable[1];
        self[0] = transport.subscribe(STREAM, delivery -> {
            delivered.incrementAndGet();
            try {
                self[0].close();
            } catch (Exception impossible) {
                throw new IllegalStateException(impossible);
            }
        });
        await(() -> requests.get() >= 2, "the parked poll");
        parked.countDown();
        await(() -> closedByClient.get() == connections.get(), "the reader closed its connection");

        assertThat(delivered).as("the frame after the listener's close is not delivered")
                .hasValue(1);
    }

    /**
     * ⚠️ THE CHECK BEFORE THE RECONNECT BLOCK, ALONE (M13.4 review P3, T3): a
     * handshake answered after the close counts no reconnect and runs no
     * {@code onReconnect}, which re-registers every index on the node.
     */
    @Test
    void aHandshakeAnsweredAfterTheCloseRegistersNothing() throws Exception {
        AtomicInteger reconnects = new AtomicInteger();
        transport = new HttpSubscriptionTransport(serve(), reconnects::incrementAndGet,
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5));
        parkAt = 1;
        parked = new java.util.concurrent.CountDownLatch(1);
        AutoCloseable subscription = transport.subscribe(STREAM, delivery -> { });
        await(() -> requests.get() >= 1, "the parked handshake");
        subscription.close();
        parked.countDown();
        await(() -> closedByClient.get() == connections.get(), "the reader closed its connection");

        assertThat(reconnects).as("no onReconnect for a closed subscription").hasValue(0);
        assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.PUSH))
                .as("and no entry into PUSH").isZero();
    }

    /**
     * ⚠️ A CLOSED SUBSCRIPTION LEAVES {@code tier()} AT ONCE (M13.4 review P4).
     * Unstopped by an interrupt, a reader in a backoff sleep lives on after its
     * close for up to 1.5x the retry ceiling -- ten seconds and more here -- and
     * a ladder it still held kept the node off PUSH, and so gated its Tier 3
     * recovery, while every live stream pushed.
     */
    @Test
    void aClosedSubscriptionBackingOffNoLongerCountsTowardTheTier() throws Exception {
        transport = new HttpSubscriptionTransport(serve(), () -> { }, Duration.ofSeconds(10),
                Duration.ofSeconds(10), Duration.ofSeconds(5));
        RunKey refused = new RunKey(UUID.randomUUID(), 0);
        refusedPath = refused.indexId().toString();
        AutoCloseable down = transport.subscribe(refused, delivery -> { });
        await(() -> requests.get() >= 1, "the refused stream's first poll");
        try (AutoCloseable up = transport.subscribe(STREAM, delivery -> { })) {
            await(() -> requests.get() >= 3, "the answered stream's polls");
            assertThat(transport.ingesterAnswers()).as("the premise: a stream is down")
                    .isFalse();

            down.close();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!transport.ingesterAnswers() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(transport.ingesterAnswers())
                    .as("the closed stream no longer holds the node off PUSH, within 2 s")
                    .isTrue();
        }
    }

    private static io.github.huyz0.os.biningester.format.SubscriptionEvent event(String segment) {
        return new io.github.huyz0.os.biningester.format.SubscriptionEvent("s", 1L, 1L, STREAM,
                segment, 0L, 1, io.github.huyz0.os.biningester.format.FetchMode.INLINE,
                new byte[] {1});
    }

    /** One length-prefixed frame, as the ingester writes a poll's answer. */
    private static byte[] frame(io.github.huyz0.os.biningester.format.SubscriptionEvent event) {
        byte[] bytes = event.encode();
        return java.nio.ByteBuffer.allocate(4 + bytes.length).putInt(bytes.length).put(bytes)
                .array();
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
