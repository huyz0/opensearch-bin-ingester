// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import java.io.IOException;

/**
 * The port a {@link NodeProcess} starts a node on, and the retry when another
 * process takes it between the probe and the bind (M13.2, M12.28).
 */
final class NodePorts {

    private NodePorts() {
    }

    /**
     * How many ports a node is offered before its start fails (M13.2): the
     * race is a window of milliseconds, so a second loss in a row is already
     * rare and a third says something else is wrong.
     */
    static final int PORT_ATTEMPTS = 3;

    /**
     * The child refused to start because the port it was handed was taken
     * between the probe and its bind (M12.28).
     */
    static final class PortLost extends RuntimeException {
        PortLost(int port, String log) {
            super("port " + port + " was taken before the node bound it:\n" + log);
        }
    }

    /** One start of a node on {@code port}. */
    @FunctionalInterface
    interface PortAttempt<T> {
        T start(int port) throws Exception;
    }

    /**
     * Starts on a fresh port from {@code freePort}, and again on another ONLY
     * when the start lost its port (M13.2, M12.28), at most {@code attempts}
     * times.
     *
     * <p>⚠️ NOT PORT 0 IN THE CHILD, which M12.28 proposed first: the lease
     * advertises the endpoint, port included, when the graph is assembled --
     * before the front door binds -- so a child on port 0 would advertise a
     * port nobody knows. The race is recovered, not removed.
     *
     * <p>⚠️ ANY OTHER FAILURE IS NOT RETRIED: a node that dies for its own
     * reason must fail the test that started it, not be restarted until it
     * happens to come up.
     */
    static <T> T onAFreePort(int attempts, java.util.function.IntSupplier freePort,
            PortAttempt<T> attempt) throws Exception {
        for (int tried = 1; ; tried++) {
            try {
                return attempt.start(freePort.getAsInt());
            } catch (PortLost lost) {
                if (tried >= attempts) {
                    throw lost;
                }
            }
        }
    }

    /**
     * Whether {@code log} is the front door's refusal of exactly {@code port}
     * (FrontDoor: "the front door did not bind port N (http.port) or its peer
     * port P (peer.port) -- one is already in use"), which `Main` prints
     * before exiting. ⚠️ EITHER PORT LOST READS AS THIS ONE (M13.52c): the
     * retry probes both afresh.
     */
    static boolean lostItsPort(String log, int port) {
        return log.contains("the front door did not bind port " + port + " (http.port)");
    }

    /**
     * ⚠️ BOUND AND RELEASED, so another process can take it in the window
     * before the child binds -- the race {@link #onAFreePort} recovers.
     */
    static int probeFreePort() {
        try (var probe = new java.net.ServerSocket(0)) {
            return probe.getLocalPort();
        } catch (IOException noPort) {
            throw new java.io.UncheckedIOException(noPort);
        }
    }

    /**
     * The {@link PortLost} in {@code failed}'s cause chain, or {@code null}.
     *
     * <p>⚠️ DEFENSIVE, AND SAID SO (M13.2 review R2-P2): Awaitility 4 rethrows
     * a condition's RuntimeException bare today, so on the path a real node
     * takes the top-level exception IS the loss. The walk is kept so a
     * wrapping change in Awaitility cannot silently turn the retry off.
     */
    static PortLost portLostIn(Throwable failed) {
        for (Throwable cause = failed; cause != null; cause = cause.getCause()) {
            if (cause instanceof PortLost lost) {
                return lost;
            }
        }
        return null;
    }
}
