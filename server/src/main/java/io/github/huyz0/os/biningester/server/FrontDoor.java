// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.http.BulkService;
import io.github.huyz0.os.biningester.http.CommitService;
import io.github.huyz0.os.biningester.http.DrainGate;
import io.github.huyz0.os.biningester.http.HealthService;
import io.github.huyz0.os.biningester.http.SubscriptionService;
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

    static final String LISTENER_STOPPED = "listener stopped";

    private final WebServer server;
    private final DrainGate gate;
    private final java.util.function.Consumer<String> journal;

    private FrontDoor(WebServer server, DrainGate gate,
            java.util.function.Consumer<String> journal) {
        this.server = server;
        this.gate = gate;
        this.journal = journal;
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
        return start(assembly, clock, event -> { });
    }

    /**
     * The same, telling {@code journal} when the gate's switches move and
     * when the listener stops (M8.7).
     */
    public static FrontDoor start(Assembly assembly, Clock clock,
            java.util.function.Consumer<String> journal) {
        return start(assembly, clock, journal, io.github.huyz0.os.biningester.binstore.CrossAzBytes.untracked());
    }

    /**
     * The same, counting the bytes this node serves to a consumer in another
     * zone (M9.2, NFR-5).
     *
     * <p>⚠️ **THE COUNTER IS THE NODE'S, NOT THE DOOR'S**, and it is passed in
     * rather than built here because the peer transport counts into the same
     * one: NFR-5 is one ratio per pod, and two counters would be two partial
     * answers with nothing saying so.
     */
    public static FrontDoor start(Assembly assembly, Clock clock,
            java.util.function.Consumer<String> journal,
            io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
        Objects.requireNonNull(crossAz, "crossAz");
        Objects.requireNonNull(assembly, "assembly");
        Objects.requireNonNull(journal, "journal");
        Objects.requireNonNull(clock, "clock");
        ServerConfig config = assembly.config();
        DrainGate gate = new DrainGate(journal);
        WebServer server = build(config, assembly, clock, gate, crossAz);
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
        return new FrontDoor(server, gate, journal);
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

    private static WebServer build(ServerConfig config, Assembly assembly, Clock clock,
            DrainGate gate, io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
        return WebServer.builder()
                // ⚠️ HELIDON'S OWN SHUTDOWN HOOK IS OFF. Left on, a `SIGTERM`
                // runs it alongside `Main`'s, and it stops the listener while
                // the drain is still at step 2. MEASURED (M8.7): a poll during
                // the drain was then refused a connection instead of being
                // answered 503, and `/ready` cannot answer at all. The node's
                // own sequence stops the listener, after the drain. ⚠️ NOT
                // PINNED: the two hooks race, and a test that saw it once
                // passed the next run.
                .shutdownHook(false)
                .port(config.httpPort())
                .routing(HttpRouting.builder()
                        // ⚠️ READY ONLY WHILE NOT DRAINING AND THE STORE ANSWERS
                        // (M8.15): a node partitioned from the store can only
                        // make a producer wait, so it asks not to be sent any.
                        .register(new HealthService(
                                () -> gate.ready() && assembly.storeHealthy()))
                        .register(new BulkService(assembly.ingest(), config.principal(), gate))
                        .register(new CommitService(assembly::heldTerm,
                                (term, pod) -> io.github.huyz0.os.biningester.sequencer.InboxDrain.drain(
                                        assembly.store(), config.prefix(), term, pod)))
                        .register(new SubscriptionService(assembly.hub(), assembly.catalog(),
                                assembly.watermarks(), clock, assembly.floors(), gate, crossAz)))
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
     * What this door admits, which {@link IngesterNode} drains on shutdown
     * (M8.7).
     */
    public DrainGate gate() {
        return gate;
    }

    /**
     * Stops listening.
     *
     * <p>⚠️ **THIS IS NOT THE GRACEFUL SHUTDOWN.** {@link ShutdownSequence}
     * is, and it calls this once the {@link #gate() gate} has been drained, so
     * no request is still inside when the listener goes.
     */
    @Override
    public void close() {
        server.stop();
        journal.accept(LISTENER_STOPPED);
    }
}
