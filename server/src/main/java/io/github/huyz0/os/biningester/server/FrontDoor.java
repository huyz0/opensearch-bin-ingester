// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.http.AdminCostService;
import io.github.huyz0.os.biningester.ingest.IndexCostReport;
import io.github.huyz0.os.biningester.http.BulkService;
import io.github.huyz0.os.biningester.http.CommitService;
import io.github.huyz0.os.biningester.http.DrainGate;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.http.HealthService;
import io.github.huyz0.os.biningester.http.SubscriptionService;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalService;
import io.github.huyz0.os.biningester.http.CatchUpService;
import io.github.huyz0.os.biningester.http.SegmentFetchService;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
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
 * which is the point of keeping the three server composition-root files that must reach past a seam
 * ({@link Main} and {@link ConfigFile}) as small as they are.
 */
public final class FrontDoor implements AutoCloseable {

    static final String LISTENER_STOPPED = "listener stopped";

    private final WebServer server;
    private final DrainGate gate;
    private final java.util.function.Consumer<String> journal;
    private final LaneAdmission admission;
    private final io.github.huyz0.os.biningester.ingest.IndexQuotas quotas;

    private FrontDoor(WebServer server, DrainGate gate,
            java.util.function.Consumer<String> journal, LaneAdmission admission,
            io.github.huyz0.os.biningester.ingest.IndexQuotas quotas) {
        this.server = server;
        this.gate = gate;
        this.journal = journal;
        this.admission = admission;
        this.quotas = quotas;
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
        // ⚠️ ONE PER POD (M10.8, ADR-0074 decision 6): the budget is the
        // pod's, and a node starts one front door, so this is where it lives.
        LaneAdmission admission = new LaneAdmission(config.ingest().maxInFlightBulk(),
                config.ingest().lanes());
        io.github.huyz0.os.biningester.ingest.IndexQuotas quotas = new io.github.huyz0.os.biningester.ingest.IndexQuotas(config.quotas(), clock,
                // ⚠️ ONLY A REGISTERED INDEX GETS A BUCKET (M12.4)
                name -> assembly.catalog().resolve(name).isPresent());
        WebServer server = build(config, assembly, clock, gate, crossAz, admission, quotas);
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
        return new FrontDoor(server, gate, journal, admission, quotas);
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
            DrainGate gate, io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz,
            LaneAdmission admission, io.github.huyz0.os.biningester.ingest.IndexQuotas quotas) {
        HttpRouting.Builder routes = HttpRouting.builder()
                // ⚠️ HELIDON'S OWN SHUTDOWN HOOK IS OFF. Left on, a `SIGTERM`
                // runs it alongside `Main`'s, and it stops the listener while
                // the drain is still at step 2. MEASURED (M8.7): a poll during
                // the drain was then refused a connection instead of being
                // answered 503, and `/ready` cannot answer at all. The node's
                // own sequence stops the listener, after the drain. ⚠️ NOT
                // PINNED: the two hooks race, and a test that saw it once
                // passed the next run.
                // ⚠️ READY ONLY WHILE NOT DRAINING AND THE STORE ANSWERS
                        // (M8.15): a node partitioned from the store can only
                        // make a producer wait, so it asks not to be sent any.
                        .register(new HealthService(
                                () -> gate.ready() && assembly.storeHealthy()))
                        .register(new BulkService(assembly.ingest(), config.principal(), gate,
                                admission, quotas,
                                new RefusalMetrics(assembly.refusedIndices())))
                        .register(new CommitService(assembly::heldTerm,
                                // ⚠️ THE NODE's STORE, NOT THE RAW BACKEND (M10.26):
                                // the drain's LIST is declared recovery, so it is
                                // counted and never refused, as at a takeover.
                                (term, pod) -> io.github.huyz0.os.biningester.sequencer.InboxDrain.drain(
                                        assembly.nodeStore(), config.prefix(), term, pod,
                                        assembly.metrics()::failedIntentBatch)))
                .register(new SubscriptionService(assembly.hub(), assembly.catalog(),
                        assembly.watermarks(), clock, assembly.floors(), gate, crossAz))
                .register(new CatchUpService(assembly::respondCatchUp))
                // ⚠️ M10.1, ADR-0073: the payload half of `proxy`. Read through
                // the SAME proxy and node-wide cache the subscription path and
                // the prefetcher fill -- so on the AZ's ring owner a fetch after
                // a prefetch costs no store request -- and counted into the SAME
                // per-pod NFR-5 counter. ⚠️ AND ITS ABSENT-KEY `stat` THROUGH
                // THE NODE's STORE (M10.26), so it is counted like the GETs.
                .register(SegmentFetchService.over(
                        config.prefix(), assembly.segmentProxy(), assembly.nodeStore(), crossAz))
                .register(new DurableSegmentSignalService(assembly.peerView(),
                        assembly::prefetchDurableSegment));
        // ⚠️ M11.4, ADR-0077: each index's share of THIS pod's requests, from the
        // ledger its stores charge. No request of its own. ⚠️ ONLY WHERE TURNED ON
        // (M12.6, M11 review F7): it names indices on the producer port, which is
        // unauthenticated until M1.7c -- off, the path is simply not a route.
        if (config.adminCost()) {
            routes.register(new AdminCostService(top -> costReport(assembly, top)));
        }
        String macroPath = System.getProperty("binstore.macro.path");
        if (macroPath != null && !macroPath.isBlank()) {
            routes.register(new MacroCountsService(assembly, macroPath, crossAz));
        }
        return WebServer.builder().shutdownHook(false).port(config.httpPort())
                .routing(routes).build();
    }

    private static final class MacroCountsService implements io.helidon.webserver.http.HttpService {
        private final Assembly assembly;
        private final String path;
        private final io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz;

        private MacroCountsService(Assembly assembly, String path,
                io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
            this.assembly = assembly;
            this.path = path;
            this.crossAz = crossAz;
        }

        @Override
        public void routing(HttpRules rules) {
            rules.get(path, this::counts);
        }

        private void counts(ServerRequest request, ServerResponse response) {
            response.send(macroCountsJson(assembly.config().podId(), assembly.storeCounts(),
                    assembly.putPurposeCounts(), crossAz));
        }
    }

    /** The pod's {@code /admin/cost} answer for its {@code top} indices (M11.4). */
    static String costReport(Assembly assembly, int top) {
        return IndexCostReport.json(assembly.config().podId(), assembly.costLedger().snapshot(),
                new IndexCostReport.PodTotals(assembly.storeCounts(),
                        assembly.putPurposeCounts(), assembly.dataSegmentGets()),
                assembly.store().capabilities().costs(), assembly.catalog().namesById()::get,
                top, assembly.catalog().undecodableUuids());
    }

    static String macroCountsJson(String podId, StoreCounts counts) {
        return "{\"podId\":\"" + escapeJson(podId)
                + "\",\"puts\":" + counts.puts()
                + ",\"gets\":" + counts.gets()
                + ",\"lists\":" + counts.lists()
                + ",\"stats\":" + counts.stats()
                + ",\"deletes\":" + counts.deletes() + "}\n";
    }

    static String macroCountsJson(String podId, StoreCounts counts,
            io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
        return macroCountsJson(podId, counts, null, crossAz);
    }

    static String macroCountsJson(String podId, StoreCounts counts,
            PutPurposeCounts purposePuts,
            io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
        // The three-argument FrontDoor.start overload predates M9.2 and can
        // deliberately carry an untracked counter. Preserve its existing
        // store-only snapshot contract rather than turning a compatibility
        // caller's GET into an IllegalStateException. IngesterNode always
        // supplies the real tracked counter used by the M9 macro harness.
        try {
            return trackedMacroCountsJson(podId, counts, purposePuts, crossAz);
        } catch (IllegalStateException untracked) {
            return purposePuts == null ? macroCountsJson(podId, counts)
                    : purposeMacroCountsJson(podId, counts, purposePuts);
        }
    }

    private static String trackedMacroCountsJson(String podId, StoreCounts counts,
            PutPurposeCounts purposePuts,
            io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz) {
        String base = purposePuts == null ? macroCountsJson(podId, counts).stripTrailing()
                : purposeMacroCountsJson(podId, counts, purposePuts).stripTrailing();
        return base.substring(0, base.length() - 1)
                + ",\"crossAzBytes\":" + crossAz.crossAzBytes()
                + ",\"unknownPeerBytes\":" + crossAz.unknownPeerBytes()
                + ",\"proxyRead\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.PROXY_READ)
                // ⚠️ THE ONE SAME-AZ READING, because NFR-5's full proof (M10.4)
                // must show proxy payloads were served IN the consumer's zone,
                // not merely that few crossed: a route that counted nothing at
                // all would pass a cross-AZ budget too.
                + ",\"sameAzProxyRead\":" + crossAz.sameAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.PROXY_READ)
                + ",\"inlinePush\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.INLINE_PUSH)
                + ",\"consumerPoll\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.CONSUMER_POLL)
                + ",\"commitForward\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.COMMIT_FORWARD)
                + ",\"inboxDrain\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.INBOX_DRAIN)
                + ",\"durableSegmentSignal\":" + crossAz.crossAzBytes(
                        io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL)
                + "}\n";
    }

    private static String purposeMacroCountsJson(String podId, StoreCounts counts,
            PutPurposeCounts purposePuts) {
        return "{\"podId\":\"" + escapeJson(podId)
                + "\",\"puts\":" + purposePuts.total()
                + ",\"gets\":" + counts.gets()
                + ",\"lists\":" + counts.lists()
                + ",\"stats\":" + counts.stats()
                + ",\"deletes\":" + counts.deletes()
                + ",\"dataPuts\":" + purposePuts.dataPuts()
                + ",\"commitPuts\":" + purposePuts.commitPuts()
                + ",\"checkpointPuts\":" + purposePuts.checkpointPuts()
                + ",\"leasePuts\":" + purposePuts.leasePuts()
                + ",\"otherPuts\":" + purposePuts.otherPuts() + "}\n";
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '\\' -> escaped.append("\\\\");
                case '\"' -> escaped.append("\\\"");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.toString();
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
     * The pod's ONE lane admission, over its configured in-flight budget and
     * active lanes, that {@code _bulk} is admitted through (M10.8).
     */
    public LaneAdmission laneAdmission() {
        return admission;
    }

    /** The pod's per-index quotas, so a test can see which indices hold a bucket (M12.4). */
    io.github.huyz0.os.biningester.ingest.IndexQuotas quotas() {
        return quotas;
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
