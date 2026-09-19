// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What a Kubernetes Service does for the ingester, and no more (M8.16).
 *
 * <p>⚠️ **NEW CONNECTIONS GO ONLY TO READY BACKENDS**, round-robin; readiness
 * is each backend's own {@code /ready}, polled every {@link #PROBE_PERIOD} as
 * a readiness probe polls it. A connection already open stays where it is,
 * because a Service balances connections, not requests. So a drained pod's
 * consumers reach another pod only by reconnecting, which is the behaviour a
 * rolling restart's herd is made of.
 */
public final class ServiceLb implements AutoCloseable {

    /** How often each backend's readiness is probed. */
    public static final Duration PROBE_PERIOD = Duration.ofMillis(200);

    private final ServerSocket listener;
    private final List<Integer> backends = new CopyOnWriteArrayList<>();
    private final Map<Integer, Boolean> ready = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger();
    private volatile boolean closed;

    public ServiceLb() throws IOException {
        listener = new ServerSocket();
        listener.bind(new InetSocketAddress("localhost", 0));
        Thread.ofVirtual().name("service-lb-accept").start(this::accept);
        Thread.ofVirtual().name("service-lb-probe").start(this::probe);
    }

    /** The address clients dial: the load balancer's. */
    public String endpoint() {
        return "http://localhost:" + listener.getLocalPort();
    }

    /** Adds a backend; it takes traffic once its probe passes. */
    public void add(int port) {
        ready.put(port, false);
        backends.add(port);
    }

    /** Removes a backend, as the endpoints controller does for a deleted pod. */
    public void remove(int port) {
        backends.remove(Integer.valueOf(port));
        ready.remove(port);
    }

    private void probe() {
        while (!closed) {
            for (int port : backends) {
                boolean up;
                try (var response = WebClient.builder().baseUri("http://localhost:" + port)
                        .connectTimeout(Duration.ofMillis(500)).readTimeout(Duration.ofMillis(500))
                        .build().get("/ready").request()) {
                    up = response.status().code() == 200;
                } catch (RuntimeException down) {
                    up = false;
                }
                if (backends.contains(port)) {
                    ready.put(port, up);
                }
            }
            try {
                Thread.sleep(PROBE_PERIOD);
            } catch (InterruptedException interrupted) {
                return;
            }
        }
    }

    private void accept() {
        while (!closed) {
            Socket inbound;
            try {
                inbound = listener.accept();
            } catch (IOException stopped) {
                return;
            }
            Thread.ofVirtual().start(() -> route(inbound));
        }
    }

    private void route(Socket inbound) {
        List<Integer> up = backends.stream().filter(p -> ready.getOrDefault(p, false)).toList();
        if (up.isEmpty()) {
            closeQuietly(inbound);
            return;
        }
        int port = up.get(Math.floorMod(next.getAndIncrement(), up.size()));
        Socket outbound;
        try {
            outbound = new Socket("localhost", port);
        } catch (IOException unreachable) {
            closeQuietly(inbound);
            return;
        }
        Thread.ofVirtual().start(() -> pump(inbound, outbound));
        Thread.ofVirtual().start(() -> pump(outbound, inbound));
    }

    private static void pump(Socket from, Socket to) {
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            in.transferTo(out);
        } catch (IOException ended) {
            // either side went away
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private static void closeQuietly(Socket socket) {
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
    }
}
