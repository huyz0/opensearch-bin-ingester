// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Lease;
import io.helidon.http.HeaderNames;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The watch reads a real stream, sends the token, and reconnects (M8.13).
 *
 * <p>⚠️ **A FAKE API SERVER, NOT A CLUSTER**: the stream format is the
 * Kubernetes watch format, one JSON event per line, and the fake speaks it
 * over a real socket, which is the part the watch owns.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class EndpointSliceWatchTest {

    private static final Lease POD0 = new Lease(3, "pod0", "uid-pod0",
            "http://10.0.0.1:8080", Long.MAX_VALUE);
    private static final String END = "__end_of_stream__";

    private final LinkedBlockingQueue<String> events = new LinkedBlockingQueue<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final List<String> selectors = new CopyOnWriteArrayList<>();
    private WebServer server;
    private EndpointSliceWatch watch;

    private volatile boolean stopped;

    @AfterEach
    void stop() {
        stopped = true;
        if (watch != null) {
            watch.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private String fakeApi() {
        server = WebServer.builder().port(0).routing(HttpRouting.builder().get(
                "/apis/discovery.k8s.io/v1/namespaces/ingest/endpointslices", (req, res) -> {
                    authorizations.add(req.headers().first(HeaderNames.AUTHORIZATION)
                            .orElse("none"));
                    selectors.add(req.query().first("labelSelector").orElse("none"));
                    try (OutputStream out = res.outputStream()) {
                        // ⚠️ POLLED, NOT TAKEN, so a handler whose watch has
                        // gone does not hold the server's stop for ever.
                        while (!stopped) {
                            String event = events.poll(100, TimeUnit.MILLISECONDS);
                            if (event == null) {
                                continue;
                            }
                            if (event.equals(END)) {
                                break;
                            }
                            out.write((event + "\n").getBytes(StandardCharsets.UTF_8));
                            out.flush();
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } catch (java.io.IOException gone) {
                        // the watch closed its end
                    }
                })).build().start();
        return "http://localhost:" + server.port();
    }

    private static void await(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("never: " + what);
            }
            Thread.sleep(20);
        }
    }

    @Test
    void theWATCHFeedsTheViewSENDSTheTokenAndSELECTSTheService() throws Exception {
        EndpointSliceView view = new EndpointSliceView();
        watch = new EndpointSliceWatch(fakeApi(), "ingest", "ingester",
                () -> Optional.of("t0ken"), view).start();

        events.add(EndpointSliceViewTest.event("ADDED", "pod0", "10.0.0.1", true, false));
        await(() -> watch.connections() == 1, "a connection");
        events.add(EndpointSliceViewTest.empty("MODIFIED"));

        await(() -> view.holderGone(POD0), "the removal reached the view");
        assertThat(authorizations).containsExactly("Bearer t0ken");
        assertThat(selectors).containsExactly("kubernetes.io/service-name=ingester");
    }

    @Test
    void aStreamTheSERVERENDSIsREOPENEDAndTheViewKEEPSWhatItKnew() throws Exception {
        EndpointSliceView view = new EndpointSliceView();
        watch = new EndpointSliceWatch(fakeApi(), "ingest", "ingester", Optional::empty, view)
                .start();
        events.add(EndpointSliceViewTest.event("ADDED", "pod0", "10.0.0.1", true, false));
        await(() -> watch.connections() == 1, "the first connection");

        // ⚠️ QUEUED TOGETHER: a watch response sends no headers until its
        // first event, so the second connection only counts once the removal
        // reaches it. The queue is FIFO, so the first stream takes the end and
        // the second takes the removal.
        events.add(END);
        events.add(EndpointSliceViewTest.empty("MODIFIED"));

        await(() -> view.holderGone(POD0),
                "⚠️ A REMOVAL ON THE NEW STREAM, JUDGED AGAINST WHAT THE OLD ONE SAW");
        assertThat(watch.connections()).as("⚠️ A SECOND CONNECTION AFTER THE FIRST ENDED")
                .isEqualTo(2);
        assertThat(authorizations).as("no token configured, none sent")
                .containsOnly("none");
    }

    @Test
    void closeRETURNSWhileAStreamIsLIVE() throws Exception {
        // ⚠️ MEASURED: closing Helidon's entity stream DRAINS it to the end,
        // and a watch never ends by itself -- an earlier close blocked for the
        // ten-minute read timeout, which a node's shutdown would have waited
        // out too.
        EndpointSliceView view = new EndpointSliceView();
        watch = new EndpointSliceWatch(fakeApi(), "ingest", "ingester", Optional::empty, view)
                .start();
        events.add(EndpointSliceViewTest.event("ADDED", "pod0", "10.0.0.1", true, false));
        await(() -> watch.connections() == 1, "a live stream");

        var closing = java.util.concurrent.CompletableFuture.runAsync(watch::close);

        closing.get(5, TimeUnit.SECONDS);
    }

    @Test
    void connectionFailuresAreReportedToTheInMemoryMetricSink() throws Exception {
        AtomicLong recordedFailures = new AtomicLong();
        WebServer closedEndpoint = WebServer.builder().port(0).build().start();
        String apiBase = "http://127.0.0.1:" + closedEndpoint.port();
        closedEndpoint.stop();
        watch = new EndpointSliceWatch(apiBase, "ingest", "ingester",
                Optional::empty, new EndpointSliceView(), List.of(), recordedFailures::addAndGet)
                .start();

        await(() -> recordedFailures.get() > 0, "the failed connection reaches the metric sink");

        assertThat(recordedFailures.get()).isEqualTo(watch.failures());
    }
}
