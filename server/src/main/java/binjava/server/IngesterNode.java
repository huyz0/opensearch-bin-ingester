// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.http.DrainGate;
import binjava.http.EndpointSliceView;
import binjava.http.EndpointSliceWatch;
import binjava.http.HttpSequencerTransport;
import binjava.sequencer.LeaseChallenge;
import binjava.sequencer.SequencerTransport;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * One running ingester node: the object graph plus the door it is served
 * through (M8.4).
 *
 * <p>⚠️ **THIS IS THE PROCESS, AND {@link Main} IS ONLY ITS ARGV.** Everything
 * a node does — choosing a backend, taking a term, listening — happens here, so
 * that a test can start one and a chaos row (M8.8) can start one in a real
 * process without the two being different assemblies. A {@code main()} that
 * held the wiring would leave the tested path and the shipped path related only
 * by inspection.
 *
 * <p>⚠️ **THE ORDER IS STORE, GRAPH, DOOR — AND IT CLOSES IN REVERSE.** The
 * door goes up last because a listener that answers before the writer exists
 * accepts a write it cannot durably place; it comes down first because a write
 * accepted during a drain is a write acked after the flush that was supposed to
 * carry it. ⚠️ M8.7 owns the FULL shutdown sequence and its budget; what is
 * claimed here is this one ordering.
 */
public final class IngesterNode implements AutoCloseable {

    /**
     * ⚠️ **HOW LONG A FORWARDED COMMIT WAITS BEFORE IT IS AMBIGUOUS: THE LEASE
     * TTL.** Waiting longer only delays the follower. A leader that stopped
     * renewing has lost its term by then, and the follower must go and take
     * it. MEASURED (M8.12): a fixed 10 s here, under a 4 s TTL, kept every
     * follower forwarding to a SIGSTOPped leader for 10 s, and visibility
     * resumed in 10.6 s, past criterion 9's bound of the TTL plus 5 s.
     * ⚠️ **WHAT IT COSTS IS LIVENESS, NEVER SAFETY.** A leader can still be
     * renewing while a commit waits in its batch or on a slow delta PUT, so an
     * operator who sets a TTL shorter than the store's commit latency tail
     * turns ordinary commits ambiguous: the producer is told nothing and
     * retries, which costs duplicates. Nothing sets a floor under the TTL.
     * ⚠️ A timeout here yields an {@code IOException} and must never become a
     * 409: that conversion turns an outcome nobody knows into a licence to
     * resend elsewhere, which is how one batch gets two ranges of offsets.
     */
    static Duration peerCommitTimeout(ServerConfig config) {
        return config.leaseTtl();
    }

    /**
     * ⚠️ **HOW LONG SUBSCRIBERS ARE GIVEN TO LEAVE (§7 step 2, "wait
     * briefly").** A woken poll answers at once, so this bounds only a poll
     * that was between its admission and its wait.
     */
    static final Duration SUBSCRIBER_GRACE = Duration.ofSeconds(2);

    /**
     * ⚠️ **HOW LONG THE REQUESTS INSIDE THE DOOR ARE GIVEN (§7 step 3).** Each
     * one waits for the flush that makes it durable, and the drain forces one
     * flush per {@link #IN_FLIGHT_SLICE}, so a request inside normally
     * finishes in one slice. The bound is for a flush that is slow, and it
     * keeps the whole sequence inside §7's 30 s budget.
     */
    static final Duration IN_FLIGHT_BOUND = Duration.ofSeconds(20);

    static final Duration IN_FLIGHT_SLICE = Duration.ofMillis(100);

    private final Assembly assembly;
    private final FrontDoor door;
    private final SequencerTransport transport;
    private final Clock clock;
    private final java.util.List<String> journal;
    private volatile ShutdownSequence.Report lastShutdown;
    private volatile EndpointSliceWatch watch;
    private final java.util.concurrent.atomic.AtomicBoolean closing =
            new java.util.concurrent.atomic.AtomicBoolean();

    private IngesterNode(Assembly assembly, FrontDoor door, SequencerTransport transport,
            Clock clock, java.util.List<String> journal) {
        this.assembly = assembly;
        this.door = door;
        this.transport = transport;
        this.clock = clock;
        this.journal = journal;
    }

    /**
     * Builds and starts a node.
     *
     * @param clock ⚠️ the ONE clock the graph reads. {@link Main} passes the
     *     real one; a test passes its own, and nothing below this line calls
     *     {@code Clock.systemUTC()} for itself — which is what
     *     {@code check-io-seam.sh} enforces for every module but the two files
     *     named in its exempt list
     * @throws IOException if the store cannot be opened or the term cannot be
     *     taken
     */
    public static IngesterNode start(ServerConfig config, Clock clock) throws IOException {
        return start(config, clock, Optional::empty);
    }

    /**
     * The same, with the bearer token the {@code EndpointSlice} watch sends
     * (M8.13).
     *
     * @param token read afresh for every watch connection; {@link Main} reads
     *     it from {@link MembershipConfig#tokenFile()}, because a file read is
     *     I/O and that file is exempt from {@code check-io-seam.sh}
     */
    public static IngesterNode start(ServerConfig config, Clock clock,
            Supplier<Optional<String>> token) throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(token, "token");
        SequencerTransport transport = new HttpSequencerTransport(peerCommitTimeout(config));
        // ⚠️ THE VIEW IS THE CHALLENGE, and it exists only when a watch will
        // feed it. Without one the challenge is NEVER, which is exactly the
        // behaviour before M8.13: failover bounded by the TTL alone.
        EndpointSliceView view = config.membership().isPresent() ? new EndpointSliceView() : null;
        Assembly assembly = Assembly.open(config, transport, clock,
                view == null ? LeaseChallenge.NEVER : view);
        java.util.List<String> journal = new java.util.concurrent.CopyOnWriteArrayList<>();
        assembly.journal(journal::add);
        try {
            // ⚠️ BUILT BEFORE THE DOOR, STARTED AFTER IT: a malformed API URL
            // throws here, where the unwind below closes the graph and there is
            // no listener yet to leave bound.
            EndpointSliceWatch watch = null;
            if (view != null) {
                MembershipConfig membership = config.membership().get();
                watch = new EndpointSliceWatch(membership.apiBase(), membership.namespace(),
                        membership.service(), token, view);
            }
            IngesterNode node = new IngesterNode(assembly,
                    FrontDoor.start(assembly, clock, journal::add), transport, clock, journal);
            if (watch != null) {
                node.watch = watch.start();
            }
            return node;
        } catch (RuntimeException failed) {
            // ⚠️ A `RuntimeException` IS THE ONLY THING `FrontDoor.start` CAN
            // THROW -- Helidon reports a port already in use as one -- so this
            // catches what it declares and not what it might.
            // ⚠️ THE TERM IS ALREADY TAKEN BY THIS POINT. A port already in use
            // is the ordinary trigger, and without this the lease names a node
            // that is not listening, renewed every interval for the life of the
            // JVM -- the fleet stops committing and nothing says why. Same
            // failure `Assembly`'s own constructor guards one level down.
            closeQuietly(assembly, failed);
            throw failed;
        }
    }

    /** The graph, for a test that wants to look inside a running node. */
    public Assembly assembly() {
        return assembly;
    }

    /** The port the front door actually bound. */
    public int port() {
        return door.port();
    }

    /**
     * Stops the node, in research 08 §7's order ({@link ShutdownSequence}).
     *
     * <p>⚠️ **THE DOOR IS DRAINED BEFORE IT IS CLOSED, AND THE GRAPH IS CLOSED
     * EVEN IF EVERYTHING BEFORE IT THREW.** The graph's close is what RELEASES
     * THE LEASE, and a term left held turns every deploy into a TTL-long
     * visibility stall (§7 step 5).
     *
     * <p>⚠️ **IDEMPOTENT**, like {@link Assembly#close()}: the shutdown hook
     * and a caller's try-with-resources can both reach it.
     */
    @Override
    public void close() throws IOException {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        DrainGate gate = door.gate();
        ShutdownSequence.Report report = ShutdownSequence.run(new ShutdownSequence.Steps() {
            @Override
            public void failReadiness() {
                gate.failReadiness();
            }

            @Override
            public void releaseSubscribers() throws InterruptedException {
                gate.releasePollers();
                gate.awaitNoPollers(SUBSCRIBER_GRACE);
            }

            @Override
            public void finishInFlight() throws Exception {
                gate.refuseBulk();
                try {
                    // ⚠️ A FLUSH PER SLICE, because a request inside the door
                    // is waiting for one: its 202 is sent once its records
                    // are durable. Left to the flusher's own interval it would
                    // finish too, only later.
                    long slices = IN_FLIGHT_BOUND.toNanos() / IN_FLIGHT_SLICE.toNanos();
                    for (long i = 0; i < slices && gate.bulkInFlight() > 0; i++) {
                        assembly.flush();
                        gate.awaitNoBulk(IN_FLIGHT_SLICE);
                    }
                } finally {
                    // ⚠️ AND ONLY THEN THE LISTENER, which is §7's order: a
                    // request that arrives during the drain is answered 503
                    // rather than refused a connection. ⚠️ NOT PINNED, and
                    // said so: MEASURED, Helidon's `stop()` did not cut a
                    // request already inside, so moving this first leaves
                    // every test here green.
                    door.close();
                }
            }

            @Override
            public void flushAndCommit() throws IOException {
                assembly.flush();
            }

            @Override
            public void releaseLeases() throws IOException {
                try {
                    assembly.close();
                } finally {
                    try {
                        transport.close();
                    } finally {
                        EndpointSliceWatch running = watch;
                        if (running != null) {
                            running.close();
                        }
                    }
                }
            }
        }, clock);
        lastShutdown = report;
        if (!report.failures().isEmpty()) {
            IOException first = asIoException(report.failures().get(0));
            report.failures().stream().skip(1).forEach(first::addSuppressed);
            throw first;
        }
    }

    /** The gate the door admits through, for a test that must know a request is inside. */
    DrainGate gate() {
        return door.gate();
    }

    /**
     * What the drain's switches, the flushes, the listener and the graph said
     * as they happened, in order (M8.7).
     *
     * <p>⚠️ **THIS, NOT {@link #lastShutdown()}, IS THE OBSERVED ORDER.** The
     * report lists the sequence's steps as the sequence ran them; this is
     * written by the things the steps act on, so a step that did the wrong
     * thing, or did it at the wrong time, shows here.
     */
    public java.util.List<String> shutdownJournal() {
        return java.util.List.copyOf(journal);
    }

    /** What the last shutdown measured, or {@code null} if it has not run. */
    public ShutdownSequence.Report lastShutdown() {
        return lastShutdown;
    }

    private static IOException asIoException(Exception failed) {
        return failed instanceof IOException io ? io : new IOException(failed);
    }

    private static void closeQuietly(AutoCloseable resource, Throwable primary) {
        try {
            resource.close();
        } catch (Exception suppressed) {
            primary.addSuppressed(suppressed);
        }
    }
}
