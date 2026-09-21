// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP forwarder the test can cut: the network between one process and one
 * thing it talks to (M8.23).
 *
 * <p>⚠️ **A CUT IS A BLACK HOLE, NOT A REFUSAL, AND A HEAL LOSES NOTHING.** A
 * real partition drops packets and TCP retransmits them: the sender's buffers
 * fill and its writes block, the receiver hears nothing, and after the heal
 * the stream either resumes INTACT or a timeout has already ended the
 * connection. So a cut stops READING, both ways, on every connection: nothing
 * is discarded, the kernel buffers fill, and the heal resumes the stream where
 * it stopped. A new connection made while cut is accepted and held, unread,
 * and joined to the target only at the heal. ⚠️ An earlier draft read and
 * DISCARDED bytes while cut, so a heal handed the receiver a stream with a
 * hole in it, a torn HTTP message no real network produces (review, M8.23).
 * ⚠️ WHAT IT CANNOT MODEL: a connect to a partitioned host would not complete
 * at all, where this one completes and then hears nothing, so a client's
 * connect timeout never fires here and its read timeout does instead.
 *
 * <p>⚠️ **WHAT IT PARTITIONS IS DECIDED BY WHO DIALS IT.** In front of the
 * store, it cuts one node from the bucket and leaves every other node's path
 * alone. In front of a node's advertised endpoint, every peer that dials that
 * node goes through it, so a cut isolates the node from its peers, inbound,
 * and leaves its own outbound calls working.
 */
public final class ChaosProxy implements AutoCloseable {

    private final ServerSocket listener;
    private final String targetHost;
    private final int targetPort;
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    private volatile boolean cut;
    private volatile boolean closed;

    public ChaosProxy(String targetHost, int targetPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.listener = new ServerSocket();
        listener.bind(new InetSocketAddress("localhost", 0));
        Thread.ofVirtual().name("chaos-proxy-accept").start(this::accept);
    }

    /** The port a process dials instead of the target. */
    public int port() {
        return listener.getLocalPort();
    }

    /** Stops every byte, both ways, on every connection, until {@link #heal()}: held, never lost. */
    public void cut() {
        cut = true;
    }

    /** Lets bytes through again, and joins connections made while cut. */
    public void heal() {
        cut = false;
    }

    public boolean isCut() {
        return cut;
    }

    private void accept() {
        while (!closed) {
            Socket inbound;
            try {
                inbound = listener.accept();
            } catch (IOException stopped) {
                return;
            }
            open.add(inbound);
            Thread.ofVirtual().start(() -> {
                // ⚠️ HELD, UNREAD, UNTIL THE HEAL: a black hole, not a refusal.
                if (awaitHeal()) {
                    connect(inbound);
                }
            });
        }
    }

    private void connect(Socket inbound) {
        Socket outbound;
        try {
            outbound = new Socket(targetHost, targetPort);
        } catch (IOException unreachable) {
            closeQuietly(inbound);
            return;
        }
        open.add(outbound);
        Thread.ofVirtual().start(() -> pump(inbound, outbound));
        Thread.ofVirtual().start(() -> pump(outbound, inbound));
    }

    /**
     * Copies bytes one way, pausing at a cut and resuming at the heal with
     * nothing lost: a chunk read just before the cut is delivered after it.
     */
    private void pump(Socket from, Socket to) {
        byte[] buffer = new byte[8192];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            while (awaitHeal()) {
                int n = in.read(buffer);
                if (n < 0 || !awaitHeal()) {
                    return;
                }
                out.write(buffer, 0, n);
                out.flush();
            }
        } catch (IOException ended) {
            // either side went away
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    /** Waits out a cut. Returns false if the proxy closed while waiting. */
    private boolean awaitHeal() {
        while (cut && !closed) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !closed;
    }

    private void closeQuietly(Socket socket) {
        open.remove(socket);
        try {
            socket.close();
        } catch (IOException alreadyClosed) {
            // nothing to do
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            listener.close();
        } catch (IOException alreadyClosed) {
            // nothing to do
        }
        open.forEach(this::closeQuietly);
    }
}
