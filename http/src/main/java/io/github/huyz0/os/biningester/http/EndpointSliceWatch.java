// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Watches the ingester Service's {@code EndpointSlice}s and feeds every event
 * to an {@link EndpointSliceView} (M8.13, NFR-9, ADR-0012).
 *
 * <p>⚠️ **THE ONLY I/O ON THE EARLY-CHALLENGE PATH**, and it decides nothing:
 * the view does. This owns the socket, the reconnect, and the backoff.
 *
 * <p>⚠️ **ONE WATCH PER POD, NOT PER SHARD OR RECORD**, so its request rate
 * against the API server is one long-lived GET per pod plus one reconnect per
 * server-side timeout. It makes no object-store request at all.
 *
 * <p>⚠️ **A WATCH THAT FAILS IS RETRIED, AND WHILE IT IS DOWN THE VIEW
 * KEEPS ITS LAST STATE.** Evidence it already had stands -- a pod it last saw
 * gone is still gone -- and nothing new arrives, so a leader that dies during
 * the outage is replaced at its TTL. A watch that cleared the view on every
 * error would forget which pods it had seen ready and pay for every
 * API-server restart with TTL-long failovers.
 */
public final class EndpointSliceWatch implements AutoCloseable {

    private static final Duration MIN_BACKOFF = Duration.ofMillis(200);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(5);

    /**
     * ⚠️ **LONG, BECAUSE A WATCH IS QUIET WHEN NOTHING CHANGES.** A read timeout
     * shorter than the API server's own watch timeout would cut a healthy
     * stream in a stable cluster, and every cut is a reconnect.
     */
    private static final Duration READ_TIMEOUT = Duration.ofMinutes(10);

    private final WebClient client;
    private final String path;
    private final String service;
    private final Supplier<Optional<String>> token;
    private final EndpointSliceView view;
    private volatile boolean closed;
    private volatile Thread loop;
    private volatile int connections;

    /**
     * @param apiBase the Kubernetes API server, e.g. {@code https://kubernetes.default.svc}
     * @param token the bearer token, read afresh for every connection because a
     *     projected service-account token rotates; empty sends none
     */
    public EndpointSliceWatch(String apiBase, String namespace, String service,
            Supplier<Optional<String>> token, EndpointSliceView view) {
        this(apiBase, namespace, service, token, view, java.util.List.of());
    }

    /**
     * The same, trusting {@code trust} for the API server's certificate
     * (M8.51).
     *
     * @param trust the cluster CA's certificates -- a service account's
     *     {@code ca.crt} -- which an in-cluster API server's certificate is
     *     signed by and the JVM's default trust store does not hold. ⚠️ GIVEN,
     *     THEY ARE THE ONLY TRUST: the watch talks to one server, and that
     *     server's CA is known. Empty keeps the JVM's default.
     */
    public EndpointSliceWatch(String apiBase, String namespace, String service,
            Supplier<Optional<String>> token, EndpointSliceView view,
            java.util.List<java.security.cert.X509Certificate> trust) {
        Objects.requireNonNull(apiBase, "apiBase");
        Objects.requireNonNull(trust, "trust");
        this.path = "/apis/discovery.k8s.io/v1/namespaces/"
                + Objects.requireNonNull(namespace, "namespace") + "/endpointslices";
        this.service = Objects.requireNonNull(service, "service");
        this.token = Objects.requireNonNull(token, "token");
        this.view = Objects.requireNonNull(view, "view");
        var builder = WebClient.builder().baseUri(apiBase)
                .connectTimeout(Duration.ofSeconds(5))
                .readTimeout(READ_TIMEOUT);
        if (!trust.isEmpty()) {
            builder.tls(io.helidon.common.tls.Tls.builder().trust(trust).build());
        }
        this.client = builder.build();
    }

    /** Starts watching on a thread of its own. */
    public EndpointSliceWatch start() {
        loop = Thread.ofVirtual().name("endpointslice-watch").start(this::run);
        return this;
    }

    private volatile int failures;

    /**
     * How many connects or streams have failed, a refused TLS handshake among
     * them: the one sign an operator has that the watch produces no evidence.
     */
    public int failures() {
        return failures;
    }

    /** How many watch connections have been opened, for a reconnect assertion. */
    public int connections() {
        return connections;
    }

    private void run() {
        Duration backoff = MIN_BACKOFF;
        while (!closed) {
            try {
                if (watchOnce()) {
                    backoff = MIN_BACKOFF;
                }
            } catch (RuntimeException | java.io.IOException failed) {
                // ⚠️ COUNTED AND RETRIED: the view keeps its last state, and
                // the challenge falls back to the TTL until the next event.
                failures++;
            }
            if (closed) {
                return;
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException interrupted) {
                return;
            }
            backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0
                    ? MAX_BACKOFF : backoff.multipliedBy(2);
        }
    }

    /** One watch connection, read to its end. Returns whether any event arrived. */
    private boolean watchOnce() throws java.io.IOException {
        var request = client.get(path).queryParam("watch", "true")
                .queryParam("labelSelector", "kubernetes.io/service-name=" + service);
        Optional<String> bearer = token.get();
        if (bearer.isPresent()) {
            request = request.header(io.helidon.http.HeaderNames.AUTHORIZATION,
                    "Bearer " + bearer.get());
        }
        boolean any = false;
        try (HttpClientResponse response = request.request()) {
            if (response.status().code() != 200) {
                return false;
            }
            connections++;
            try (InputStream in = response.inputStream()) {
                ByteArrayOutputStream line = new ByteArrayOutputStream();
                for (int b; !closed && (b = in.read()) >= 0; ) {
                    if (b != '\n') {
                        line.write(b);
                        continue;
                    }
                    String text = line.toString(StandardCharsets.UTF_8).strip();
                    line.reset();
                    if (!text.isEmpty() && view.apply(text)) {
                        any = true;
                    }
                }
            }
        }
        return any;
    }

    /**
     * Stops watching.
     *
     * <p>⚠️ **BY INTERRUPTING THE LOOP, NEVER BY CLOSING ITS STREAM.** MEASURED
     * (M8.13): Helidon's entity stream DRAINS to the end when closed, and a
     * watch never ends by itself, so a close from here blocked for the whole
     * read timeout, ten minutes, and would have held a node's shutdown for
     * as long. The loop runs on a virtual thread, and interrupting a virtual
     * thread blocked in a socket read closes the socket under it.
     */
    @Override
    public void close() {
        closed = true;
        Thread running = loop;
        if (running != null) {
            running.interrupt();
            try {
                // ⚠️ BOUNDED: the loop ends at the interrupt, and a close that
                // could wait for ever is how a shutdown hangs.
                running.join(Duration.ofSeconds(2));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
