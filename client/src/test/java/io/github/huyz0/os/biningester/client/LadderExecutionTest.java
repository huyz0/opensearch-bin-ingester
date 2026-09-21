// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The fallback ladder EXECUTES on a real connection loss (M8.28, M5.91c,
 * FR-10, M8's criterion 20 as re-scoped by ADR-0057).
 *
 * <p>⚠️ **A POLICY NOTHING EXECUTES IS A POLICY THAT HAS NEVER BEEN WRONG.**
 * {@code FallbackLadder} shipped in M5 and nothing called it. The transport now
 * asks it which tier it is in at every transition, over a real socket: the
 * ingester stops answering, the consumer is in RECONNECT, it answers again,
 * the consumer is back in PUSH.
 *
 * <p>⚠️ **TIERS 2 AND 3 ARE NOT HERE, BY ADR-0057**: they read the store, and
 * a plugin that may hold no cloud SDK has no way to yet. What is asserted is
 * that the automatic ladder never ENTERS them on a connection loss that
 * reconnects -- tiers 0 and 1 cost zero GETs, by construction, because the
 * client holds no store.
 */
@Timeout(30)
class LadderExecutionTest {

    private HttpServer server;
    private final AtomicBoolean answering = new AtomicBoolean(true);
    private final java.util.concurrent.atomic.AtomicInteger refused =
            new java.util.concurrent.atomic.AtomicInteger();
    /** A stream whose polls are refused while {@link #answering} is false; null means all. */
    private volatile String failing;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            // an empty 200 is a poll with nothing new; a 503 is an ingester gone
            boolean down = !answering.get() && (failing == null
                    || exchange.getRequestURI().getPath().contains(failing));
            if (down) {
                refused.incrementAndGet();
            }
            exchange.sendResponseHeaders(down ? 503 : 200, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private static void await(String what, BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!done.getAsBoolean()) {
            assertThat(System.nanoTime()).as("never: %s", what).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    @Test
    void aCONNECTIONLostAndRegainedRunsTheLadderPUSHThenRECONNECTThenPUSH() throws Exception {
        HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                "http://127.0.0.1:" + server.getAddress().getPort(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(2),
                Duration.ofMillis(50));
        try (AutoCloseable subscription = transport.subscribe(new RunKey(UUID.randomUUID(), 0),
                (SubscriptionTransport.Listener) delivery -> { })) {
            await("the first answer", () -> transport.tier() == FallbackLadder.AutomaticTier.PUSH);

            answering.set(false);
            await("the loss", () -> transport.tier() == FallbackLadder.AutomaticTier.RECONNECT);
            // ⚠️ SEVERAL FAILED POLLS, or "entered once, not once per failed
            // poll" constrains nothing -- review MEASURED it green with every
            // failure counted as an entry.
            await("three refused polls", () -> refused.get() >= 3);
            answering.set(true);
            await("the return", () -> transport.tier() == FallbackLadder.AutomaticTier.PUSH);
        } finally {
            transport.close();
        }

        assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.PUSH))
                .as("⚠️ PUSH ON THE FIRST ANSWER AND AGAIN ON THE RETURN").isEqualTo(2);
        assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.RECONNECT))
                .as("⚠️ RECONNECT ENTERED ONCE, not once per failed poll").isEqualTo(1);
        assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.POLL_CHAIN)
                + transport.tierEntries(FallbackLadder.AutomaticTier.RECOVER))
                .as("⚠️ A LOSS THAT RECONNECTS NEVER DESCENDS TO THE STORE TIERS -- which "
                        + "ADR-0057 defers, and which cost GETs")
                .isZero();
    }

    @Test
    void ONESubscriptionDownOnASHAREDTransportIsTheNodesTIER() throws Exception {
        // ⚠️ A NODE SHARES ONE TRANSPORT ACROSS EVERY STREAM IT HOLDS, so a
        // transport-wide tier let B's return report the node as pushing while
        // A was still down. Review MEASURED the shared field doing exactly that.
        RunKey a = new RunKey(UUID.randomUUID(), 0);
        RunKey b = new RunKey(UUID.randomUUID(), 1);
        HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                "http://127.0.0.1:" + server.getAddress().getPort(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(2),
                Duration.ofMillis(50));
        try (AutoCloseable first = transport.subscribe(a,
                (SubscriptionTransport.Listener) delivery -> { });
                AutoCloseable second = transport.subscribe(b,
                        (SubscriptionTransport.Listener) delivery -> { })) {
            await("both answer", () -> transport.tierEntries(
                    FallbackLadder.AutomaticTier.PUSH) == 2);

            failing = a.indexId().toString();
            answering.set(false);
            await("A's loss", () -> transport.tier() == FallbackLadder.AutomaticTier.RECONNECT);
            await("A refused repeatedly", () -> refused.get() >= 3);

            assertThat(transport.tier())
                    .as("⚠️ B IS PUSHING AND A IS NOT: the node is in RECONNECT")
                    .isEqualTo(FallbackLadder.AutomaticTier.RECONNECT);
            assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.RECONNECT))
                    .as("ONE stream fell down the ladder, once").isEqualTo(1);
        } finally {
            transport.close();
        }
    }

    @Test
    void aTRANSPORTThatHasNeverAnsweredIsNOTInPUSH() {
        HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                "http://127.0.0.1:" + server.getAddress().getPort(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofSeconds(2));
        try {
            assertThat(transport.tier())
                    .as("nothing has answered yet, so nothing is being pushed")
                    .isEqualTo(FallbackLadder.AutomaticTier.RECONNECT);
            assertThat(transport.tierEntries(FallbackLadder.AutomaticTier.PUSH)).isZero();
        } finally {
            transport.close();
        }
    }
}
