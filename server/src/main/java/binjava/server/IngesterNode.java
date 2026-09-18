// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.http.HttpSequencerTransport;
import binjava.sequencer.SequencerTransport;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

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
     * ⚠️ **HOW LONG A FORWARDED COMMIT WAITS BEFORE IT IS AMBIGUOUS.** 10 s,
     * and it is a constant rather than a key because the number that should
     * drive it is the lease TTL, which measurement M1 (M8.27) has not sized
     * yet — a settings key written against an unmeasured constant is a value an
     * operator will tune away from the one relationship that matters. ⚠️ A
     * timeout here yields an {@code IOException} and must never become a 409:
     * that conversion turns an outcome nobody knows into a licence to resend
     * elsewhere, which is how one batch gets two ranges of offsets.
     */
    static final Duration PEER_COMMIT_TIMEOUT = Duration.ofSeconds(10);

    private final Assembly assembly;
    private final FrontDoor door;
    private final SequencerTransport transport;

    private IngesterNode(Assembly assembly, FrontDoor door, SequencerTransport transport) {
        this.assembly = assembly;
        this.door = door;
        this.transport = transport;
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
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(clock, "clock");
        SequencerTransport transport = new HttpSequencerTransport(PEER_COMMIT_TIMEOUT);
        Assembly assembly = Assembly.open(config, transport, clock);
        try {
            return new IngesterNode(assembly, FrontDoor.start(assembly, clock), transport);
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
     * Stops the node.
     *
     * <p>⚠️ **THE DOOR FIRST, THEN THE GRAPH, THEN THE TRANSPORT'S POOL.**
     * Closing the graph first would leave a listener answering out of an
     * ingester that is closing under it. ⚠️ **AND THE GRAPH IS CLOSED EVEN IF
     * THE DOOR THROWS**: the graph's close is what RELEASES THE LEASE, and a
     * term left held turns every deploy into a TTL-long visibility stall
     * (research 08 §7 step 5).
     */
    @Override
    public void close() throws IOException {
        IOException first = null;
        try {
            door.close();
        } catch (RuntimeException doorFailed) {
            first = new IOException("the front door did not stop cleanly", doorFailed);
        }
        try {
            assembly.close();
        } catch (IOException graphFailed) {
            first = first != null ? first : graphFailed;
        }
        try {
            transport.close();
        } catch (RuntimeException | IOException poolFailed) {
            first = first != null ? first : asIoException(poolFailed);
        }
        if (first != null) {
            throw first;
        }
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
