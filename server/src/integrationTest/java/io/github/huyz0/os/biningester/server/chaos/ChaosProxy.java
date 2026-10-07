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
 * connection. So a cut stops READING, both ways, on every ESTABLISHED
 * connection: nothing is discarded, the kernel buffers fill, and the heal
 * resumes the stream where it stopped. A new connection made while cut is
 * accepted and held, unread, and joined to the target only at the heal --
 * by default even when its client has since given up, which is a request
 * that reaches its server after the client's timeout: the late forward
 * {@code AzPartitionIT} exists for. ⚠️ An earlier draft read and
 * DISCARDED bytes while cut, so a heal handed the receiver a stream with a
 * hole in it, a torn HTTP message no real network produces (review, M8.23).
 * ⚠️ WHAT IT CANNOT MODEL: a connect to a partitioned host would not complete
 * at all, where this one completes and then hears nothing, so a client's
 * connect timeout never fires here and its read timeout does instead. Where
 * that difference matters, {@link #dropAbandonedAtHeal()} comes closer: a
 * connection made during the cut whose client closed it before the heal is
 * dropped, as a connect into a real partition would never have completed
 * (M13.45).
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
    private volatile boolean dropAbandoned;
    private final java.util.concurrent.atomic.AtomicInteger screened =
            new java.util.concurrent.atomic.AtomicInteger();

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

    /**
     * Lets bytes through again, and joins connections made while cut -- all of
     * them, unless {@link #dropAbandonedAtHeal()} was asked for.
     */
    public void heal() {
        cut = false;
    }

    /**
     * From now on, a connection made while cut whose client closed it before
     * the heal is dropped at the heal rather than delivered.
     *
     * <p>⚠️ **OPT-IN, BECAUSE BOTH ARE REAL (M13.45).** A connect into a
     * partition never completes, so nothing a client abandoned during the cut
     * reaches its server; a request a slow server accepted and answered after
     * the client gave up does. Replaying held connections made every drain and
     * forward timed out during {@code PartitionVisibilityIT}'s cut reach the
     * leader at the heal, queued ahead of the first real one; dropping them in
     * {@code AzPartitionIT} would lose the late forward its dedupe case needs.
     *
     * <p>⚠️ Whether a connection was made while cut is read when the proxy
     * ACCEPTS it, so one whose accept returns after the heal is delivered
     * unscreened. {@link #screened()} tells a test when it was not.
     */
    public void dropAbandonedAtHeal() {
        dropAbandoned = true;
    }

    /** How many connections were accepted while cut and held for screening. */
    int screened() {
        return screened.get();
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
            boolean screenAtHeal = cut && dropAbandoned;
            if (screenAtHeal) {
                screened.incrementAndGet();
            }
            Thread.ofVirtual().start(() -> {
                // ⚠️ HELD, UNREAD, UNTIL THE HEAL: a black hole, not a refusal.
                if (!awaitHeal()) {
                    return;
                }
                if (!screenAtHeal) {
                    connect(inbound, new byte[0]);
                    return;
                }
                // ⚠️ A CLIENT THAT GAVE UP DURING THE CUT IS NOT REPLAYED, when
                // asked (M13.45): its connect would never have completed.
                byte[] held = heldUnlessAbandoned(inbound);
                if (held == null) {
                    closeQuietly(inbound);
                } else {
                    connect(inbound, held);
                }
            });
        }
    }

    /**
     * What a connection made during the cut sent, or {@code null} once its
     * client closed it: a request is read to the end the client wrote, and an
     * end of stream behind it is a client that gave up.
     */
    private byte[] heldUnlessAbandoned(Socket inbound) {
        java.io.ByteArrayOutputStream held = new java.io.ByteArrayOutputStream();
        try {
            inbound.setSoTimeout(50);
            InputStream in = inbound.getInputStream();
            byte[] buffer = new byte[8192];
            while (true) {
                int n;
                try {
                    n = in.read(buffer);
                } catch (java.net.SocketTimeoutException stillOpen) {
                    break;
                }
                if (n < 0) {
                    return null;
                }
                held.write(buffer, 0, n);
            }
            inbound.setSoTimeout(0);
            return held.toByteArray();
        } catch (IOException gone) {
            return null;
        }
    }

    private void connect(Socket inbound, byte[] held) {
        Socket outbound;
        try {
            outbound = new Socket(targetHost, targetPort);
        } catch (IOException unreachable) {
            closeQuietly(inbound);
            return;
        }
        open.add(outbound);
        try {
            outbound.getOutputStream().write(held);
        } catch (IOException gone) {
            closeQuietly(outbound);
            closeQuietly(inbound);
            return;
        }
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
