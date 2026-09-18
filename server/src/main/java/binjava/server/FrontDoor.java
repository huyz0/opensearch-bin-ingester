// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.http.BulkService;
import binjava.http.CommitService;
import binjava.http.SubscriptionService;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.util.Objects;

/**
 * The HTTP surface of an assembled ingester node (M8.4, FR-1, FR-13).
 *
 * <p>⚠️ **THREE SERVICES ON ONE LISTENER, AND THAT IS A DECISION.** The
 * producer's {@code _bulk}, the peer's forwarded commit and the consumer's long
 * poll share a port because they share a pod identity: the {@code endpoint} a
 * lease publishes is the address peers forward to AND the address consumers
 * subscribe to, and splitting them would mean a second endpoint in the lease,
 * a second thing to configure and a second thing to get wrong. Separating them
 * later is a routing change and a config key, not a redesign.
 *
 * <p>⚠️ **THE COMMIT SERVICE IS GIVEN {@code Assembly::heldTerm} AND NOT THE
 * FLEET SEQUENCER.** A peer forwards a commit precisely because it is not the
 * leaseholder; answering it through the fleet sequencer would read the lease
 * and forward it onward — two pods bouncing one commit, and a resend authorised
 * on an outcome nobody knows. The receiving side commits locally or answers
 * 409, which is the whole shape of {@code SequencerTransport}.
 *
 * <p>⚠️ **THE SUPPLIER IS CALLED PER REQUEST**, so leadership that moves
 * between two commits is seen. A captured reference commits through a term
 * whose lease is gone, which epoch fencing catches one layer too late — after
 * this node has already answered 200.
 *
 * <p>⚠️ **THIS CLASS NAMES NO SOCKET AND READS NO CLOCK.** It takes the clock
 * and hands Helidon a port; {@code check-io-seam.sh} therefore still judges it,
 * which is the point of keeping the two files that must reach past a seam
 * ({@link Main} and {@link ConfigFile}) as small as they are.
 */
public final class FrontDoor implements AutoCloseable {

    private final WebServer server;

    private FrontDoor(WebServer server) {
        this.server = server;
    }

    /**
     * Binds and starts serving.
     *
     * <p>⚠️ **STARTED HERE RATHER THAN BY THE CALLER**, because a
     * {@code WebServer} that is built and never started is a node that holds a
     * lease, accepts nothing, and looks healthy to everything except a
     * producer.
     *
     * @param assembly the object graph to serve, already constructed
     * @param clock the subscription sweep's clock — ⚠️ the one a test moves,
     *     which is why it is a parameter and not {@code Clock.systemUTC()}
     */
    public static FrontDoor start(Assembly assembly, Clock clock) {
        Objects.requireNonNull(assembly, "assembly");
        Objects.requireNonNull(clock, "clock");
        ServerConfig config = assembly.config();
        WebServer server = build(config, assembly, clock);
        try {
            server.start();
        } catch (RuntimeException notBound) {
            // ⚠️ **THE TWO FAILURE SHAPES ARE BOTH REAL AND BOTH MEASURED.**
            // In a standalone JVM Helidon throws -- an `UncheckedIOException`
            // ("Failed to start server") wrapped by its own parallel start --
            // and under the test runner it returned -1 from `port()` instead,
            // silently. So both are converted here, at the one place that
            // knows which port was asked for, into ONE exception naming it.
            // ⚠️ AND IT IS CONVERTED RATHER THAN PROPAGATED because the raw one
            // names no port: "Failed to start server" tells an operator
            // nothing, and `Main` would print its stack trace at them.
            // ⚠️ **STOPPED FIRST, LIKE THE BRANCH BELOW.** Helidon starts its
            // listeners in PARALLEL, so a failure raised after one socket is
            // bound leaves that socket held -- and `IngesterNode.start`'s
            // unwind cannot release it, because `FrontDoor` was never
            // constructed and nothing else holds the `WebServer`. Invisible to
            // `Main`, which exits; visible to an embedder that catches this and
            // retries, and finds the port it just failed on still occupied.
            stopQuietly(server, notBound);
            throw new IllegalStateException("the front door did not bind port "
                    + config.httpPort() + " -- it is already in use", notBound);
        }
        if (server.port() <= 0) {
            // ⚠️ **HELIDON DOES NOT ALWAYS THROW WHEN THE PORT IS HELD. IT CAN
            // RETURN -1.** MEASURED on Helidon 4 against a plain
            // `ServerSocket` holding the port inside a Gradle test JVM:
            // `start()` returned normally and `port()` answered -1. Without
            // this the node comes up, TAKES AND RENEWS A TERM, and serves
            // nothing -- the fleet forwards every commit to an endpoint that
            // refuses connections, and nothing in the process says why.
            // ⚠️ THE LISTENER IS STOPPED BEFORE THROWING, and
            // `IngesterNode.start` closes the graph -- which releases the term
            // -- on exactly this exception.
            server.stop();
            throw new IllegalStateException("the front door did not bind port "
                    + config.httpPort() + " -- it is already in use");
        }
        return new FrontDoor(server);
    }

    /**
     * ⚠️ A failure to stop a server that failed to start is not the story, and
     * must not replace it — so it is suppressed onto the real one rather than
     * thrown.
     */
    private static void stopQuietly(WebServer server, Throwable primary) {
        try {
            server.stop();
        } catch (RuntimeException alsoFailed) {
            primary.addSuppressed(alsoFailed);
        }
    }

    private static WebServer build(ServerConfig config, Assembly assembly, Clock clock) {
        return WebServer.builder()
                .port(config.httpPort())
                .routing(HttpRouting.builder()
                        .register(new BulkService(assembly.ingest(), config.principal()))
                        .register(new CommitService(assembly::heldTerm))
                        .register(new SubscriptionService(assembly.hub(), assembly.catalog(),
                                assembly.watermarks(), clock)))
                .build();
    }

    /**
     * The port actually bound.
     *
     * <p>⚠️ **ASKED OF THE SERVER, NOT READ BACK OUT OF THE CONFIG**, because
     * a configured 0 means "whatever the kernel gave us" and a test that echoed
     * the 0 back would dial port 0.
     */
    public int port() {
        return server.port();
    }

    /**
     * Stops listening.
     *
     * <p>⚠️ **THIS IS NOT THE GRACEFUL SHUTDOWN.** M8.7 owns the ORDER —
     * telling subscribers, draining, flushing, then releasing the lease — and
     * the 30 s budget research 08 §7 measures. What this does is stop the
     * listener; a node closed through {@link Main} closes this first so that no
     * new write arrives while the writer is draining, and that ordering is the
     * only part of §7 this task claims.
     */
    @Override
    public void close() {
        server.stop();
    }
}
