// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.CommitRequestFrame;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.http.Status;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The pod-to-pod forwarding hop, over HTTP (M8.20, M5.6e, FR-12).
 *
 * <p>⚠️ **M5 SHIPPED THIS SEAM WITH ONLY A FAKE BEHIND IT.** M5.3 scoped "the
 * seam and its fake" and no row ever owned the real one, so
 * {@code new FleetSequencer(store, config, ???, election)} could not be written
 * by a service and every append to a non-leaseholder pod failed. Until this
 * class there was no fleet — only a single pod talking to itself.
 *
 * <p>⚠️ **THE ENDPOINT IS AN ARGUMENT AND NOTHING IS CACHED**, which the
 * interface says and this implementation must not quietly improve on: the lease
 * object is the truth about who holds it (ADR-0012, "a peer hint may
 * accelerate, never decide"), so a forwarding pod re-reads it every time. What
 * IS cached here is one {@link WebClient} per endpoint — a connection pool, not
 * a routing decision — because building one per commit would open a socket per
 * flush per pod.
 *
 * <p>⚠️ **THE THREE OUTCOMES ARE DISTINGUISHED AT THE STATUS LINE, AND THE
 * DISTINCTION IS THE WHOLE POINT OF THIS CLASS**:
 * <ul>
 *   <li><b>200</b> — the delta, applied. The body is a {@link CommitDelta}.</li>
 *   <li><b>409</b> — {@link SequencerTransport.NotTheLeaseholderException}: the
 *       peer answered that it does not hold the lease. ⚠️ **A REFUSAL, NOT A
 *       FAILURE**: nothing was applied, so re-reading the lease and resending
 *       is safe, and that is what the caller does.</li>
 *   <li><b>400 or 413</b> — an {@link IOException} saying the peer refused the
 *       request: it was invalid or too large. ⚠️ **KNOWN, not ambiguous.**
 *       The peer did not apply it, and retrying the unchanged request will be
 *       refused again, so neither status is a leaseholder refusal.</li>
 *   <li><b>anything else, or no answer at all</b> — {@link IOException}, which
 *       is ⚠️ **AMBIGUOUS**: the request may have been applied and only the
 *       reply lost. A caller may resend a REFUSED request, never one whose
 *       outcome it does not know.</li>
 * </ul>
 *
 * <p>⚠️ **A TIMEOUT IS AN {@code IOException} AND MUST NEVER BECOME A 409.**
 * That is the one conversion that turns an ambiguous outcome into a licence to
 * resend elsewhere, which is how the same records get two ranges of offsets.
 */
public final class HttpSequencerTransport implements SequencerTransport {

    /** {@code POST} here to forward a commit. */
    public static final String PATH = "/ctl/commit";

    /** Where a pod that deferred asks the leaseholder to drain the inbox (M8.14a). */
    public static final String DRAIN_PATH = "/ctl/drain";

    /**
     * ⚠️ 409 CONFLICT for "not the leaseholder", chosen because it is the one
     * 4xx that says the request was well-formed and the SERVER's state refused
     * it. A 404 would be indistinguishable from a peer running an older build
     * with no route, and a 403 from a credential problem — both of which a
     * caller must treat as ambiguous rather than as a clean refusal.
     */
    public static final Status NOT_THE_LEASEHOLDER = Status.CONFLICT_409;

    private final Map<String, WebClient> clients = new ConcurrentHashMap<>();
    private final Duration timeout;
    private final CrossAzBytes crossAz;
    private volatile boolean closed;

    /**
     * @param timeout how long to wait for a peer. ⚠️ **A TIMEOUT THAT EXPIRES
     *     IS AN AMBIGUOUS OUTCOME, not a refusal**, so this is a liveness dial
     *     and never a safety one: shortening it makes a slow peer look dead
     *     sooner, and the caller still may not resend elsewhere.
     */
    public HttpSequencerTransport(Duration timeout) {
        this(timeout, CrossAzBytes.untracked());
    }

    /**
     * The same, counting the bytes this hop sends to a peer in another zone
     * (M9.2, NFR-5).
     *
     * <p>⚠️ **EVERY OTHER CONSTRUCTOR COUNTS NOTHING**, which is the behaviour
     * before M9.2: only the composition root, which is the one place that
     * knows this pod's {@code pod.az}, passes a real counter.
     *
     * <p>⚠️ **WHAT IS COUNTED IS WHAT THIS POD SENDS**, not what it receives:
     * NFR-5 bounds the bytes a pod puts on a cross-zone wire, and counting the
     * reply as well would count the same segment's commit twice across the
     * fleet -- once as the forwarder's egress and once as the leaseholder's.
     */
    public HttpSequencerTransport(Duration timeout, CrossAzBytes crossAz) {
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
    }

    /**
     * Asks the leaseholder at {@code endpoint} to drain the inbox (M8.14a,
     * ADR-0058).
     *
     * <p>⚠️ **ANYTHING BUT 200 LEAVES THE CALLER DEFERRING**, which is the safe
     * side: it keeps writing intents rather than forwarding past its own.
     */
    @Override
    public void drain(String endpoint, String requester) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        if (closed) {
            throw new IOException("transport is closed");
        }
        // ⚠️ COUNTED BEFORE THE ANSWER, because the bytes left this pod
        // whatever the peer says. A drain asked of a leaseholder in another
        // zone is a cross-AZ request per deferral, and ADR-0058's inbox is
        // exactly the path a partition puts traffic on.
        crossAz.sentTo(CrossAzBytes.Transport.INBOX_DRAIN, endpoint, drainAskBytes(requester));
        try (HttpClientResponse response = clientFor(endpoint).post(DRAIN_PATH)
                .queryParam("pod", requester).request()) {
            if (response.status().code() == NOT_THE_LEASEHOLDER.code()) {
                throw new NotTheLeaseholderException(endpoint + " does not hold the lease");
            }
            if (response.status().code() != Status.OK_200.code()) {
                throw new IOException("the drain at " + endpoint + " answered "
                        + response.status() + ": " + read(response));
            }
        } catch (RuntimeException unreachable) {
            throw new IOException("the drain at " + endpoint + " was not reachable", unreachable);
        }
    }

    @Override
    public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(request, "request");
        if (closed) {
            // ⚠️ AN IOException AND NOT AN IllegalStateException: a caller that
            // races a shutdown must treat the outcome as unknown like any other
            // failure here, and an unchecked throw out of `send` would escape
            // the append path past its own `throws IOException`.
            throw new IOException("transport is closed");
        }
        byte[] body = frameOf(request).encode();
        // ⚠️ COUNTED BEFORE THE SEND, AND NOT RETRACTED ON FAILURE. A commit
        // whose answer was lost still spent the bytes, and an unsent one is the
        // rarer case; a counter that only counted successes would under-report
        // exactly when a zone is misbehaving.
        crossAz.sentTo(CrossAzBytes.Transport.COMMIT_FORWARD, endpoint, body.length);
        try (HttpClientResponse response = clientFor(endpoint).post(PATH).submit(body)) {
            // ⚠️ COMPARED BY CODE, NEVER BY `Status.equals`. Helidon's
            // `Status.equals` compares the REASON PHRASE as well as the code --
            // measured: `Status.create(409, "").equals(Status.CONFLICT_409)` is
            // FALSE, and RFC 9112 permits an empty reason. With `.equals`, a
            // peer answering 409 with a non-canonical reason falls through and
            // a CLEAN REFUSAL becomes an ambiguous failure, so the append never
            // follows the lease and writes stall; and a 200 with an odd reason
            // reports a commit that WAS applied as UNKNOWN. Invisible to any
            // test whose peer is this repo's own `CommitService`.
            if (response.status().code() == NOT_THE_LEASEHOLDER.code()) {
                throw new NotTheLeaseholderException(endpoint + " does not hold the lease");
            }
            if (response.status().code() == Status.BAD_REQUEST_400.code()
                    || response.status().code() == Status.REQUEST_ENTITY_TOO_LARGE_413.code()) {
                // ⚠️ KNOWN, NOT UNKNOWN: 400 rejects an invalid frame and 413
                // rejects an oversized one before commit. A reply body is also
                // bounded by `read`; its overflow is rendered as a neutral
                // sentinel, never as though this node's request body was too
                // large. Retrying an unchanged request will be refused again.
                throw new IOException("commit to " + endpoint + " was REFUSED with HTTP "
                        + response.status().code() + ": " + read(response)
                        + " -- nothing was applied, and resending it unchanged will be refused again");
            }
            if (response.status().code() != Status.OK_200.code()) {
                // ⚠️ THE PEER's OWN MESSAGE TRAVELS WITH THE STATUS. Without it
                // the only operator who can see why the fleet stopped
                // committing is the one on the node that failed, rather than
                // the one watching writes stall.
                throw ambiguous(endpoint, new IOException("peer answered "
                        + response.status() + ": " + read(response)));
            }
            try {
                return CommitDelta.decode(bounded(response, MAX_REPLY_BYTES));
            } catch (IOException torn) {
                // ⚠️ EXPLICIT, because `CommitDelta.decode` throws a CHECKED
                // IOException that would propagate past the arm below carrying
                // none of the "outcome is UNKNOWN" wording -- and a torn reply
                // is exactly the ambiguous case: the peer answered 200, so the
                // commit very probably HAPPENED and only the answer is
                // unreadable.
                throw ambiguous(endpoint, torn);
            }
        } catch (RuntimeException failed) {
            // ⚠️ ONE ARM, NOT TWO. The client throws unchecked for a connect
            // failure, a timeout and a torn reply, and all of them are the same
            // ambiguous case -- so ONE place decides what they become. Two arms
            // meant two places to get it wrong, and review measured a mutation
            // of one surviving because the other still caught the case under
            // test. `UncheckedIOException` is a `RuntimeException`, so nothing
            // is lost.
            //
            // ⚠️ AND WHAT THEY BECOME IS AN `IOException`, NEVER A REFUSAL.
            // Letting them escape unchecked would take down the append path;
            // converting them to a refusal would licence a resend elsewhere,
            // which is how one flush gets two ranges of offsets.
            throw ambiguous(endpoint, failed);
        }
    }

    /**
     * A FLOOR on the bytes a drain ask puts on the wire, and ⚠️ **it is a
     * floor rather than the wire cost, which matters when it is read**: it is
     * the request target's own characters -- the path, {@code ?pod=} and the
     * requester's id, about 25 of them -- counted as one byte each. It does
     * NOT include the request line's method and version, ANY header (a
     * {@code Host} alone is larger than everything counted here), the TLS
     * record or TCP framing, or percent-encoding, which would lengthen a
     * requester id containing anything outside the unreserved set. A real
     * drain ask on the wire is a few hundred bytes; this reports tens.
     *
     * <p>⚠️ **COUNTED ANYWAY, AND THE UNDERCOUNT IS SAFE IN THE DIRECTION
     * NFR-5 CARES ABOUT ONLY BECAUSE IT IS TINY.** A drain carries no body, so
     * a counter that ignored the transport entirely would report zero for the
     * one path ADR-0058 adds -- and "this transport spends nothing" is a claim
     * someone would have to make rather than an omission nobody notices. If a
     * measurement ever turns on this term, it is headers that have to be
     * counted, not this arithmetic that has to be tuned.
     */
    static long drainAskBytes(String requester) {
        return DRAIN_PATH.length() + POD_PARAM.length()
                + (requester == null ? 0 : requester.length());
    }

    /**
     * The counted form of the drain ask's one query parameter. ⚠️ **THE SEND
     * SITE DOES NOT USE IT**: Helidon takes the key and the value separately
     * ({@code .queryParam("pod", requester)}), so this constant is a second
     * spelling of the same key and renaming one does not rename the other --
     * the counter would then keep counting the old name. The undercount is
     * bounded either way; see {@link #drainAskBytes}.
     */
    static final String POD_PARAM = "?pod=";

    /**
     * The one outcome a caller must NOT act on: the commit may have been
     * applied and only the answer lost.
     *
     * <p>⚠️ **ONE METHOD, SO THERE IS ONE PLACE TO GET THIS WRONG**, and so
     * that a mutation of it is caught rather than absorbed by a sibling arm.
     */
    private static IOException ambiguous(String endpoint, Throwable cause) {
        return new IOException("commit to " + endpoint + " failed: " + cause
                + " -- the outcome is UNKNOWN, so this request must not be resent to "
                + "another sequencer", cause);
    }

    /** The wire shape of {@code request} (ADR-0053). */
    static CommitRequestFrame frameOf(CommitRequest request) {
        return new CommitRequestFrame(request.podId(), request.incarnationId(),
                request.flushSeq(), request.segmentKey(), request.recordCounts());
    }

    /** The in-process shape of {@code frame} (ADR-0053). */
    static CommitRequest requestOf(CommitRequestFrame frame) {
        return new CommitRequest(frame.podId(), frame.incarnationId(), frame.flushSeq(),
                frame.segmentKey(), frame.recordCounts());
    }

    /**
     * ⚠️ **THE REPLY IS BOUNDED TOO, AND THE ARGUMENT IS THE ROUTE'S IN
     * REVERSE.** `CommitService` refuses an unbounded request body because it
     * would OOM the leaseholder every other node forwards to; a peer's reply is
     * the same weapon pointed the other way, and review MEASURED it: a lazily
     * generated 2 GiB 200 response killed the CLIENT with an
     * `OutOfMemoryError`, which escapes `send` past its own
     * `throws IOException` and takes the forwarding node's writer with it.
     *
     * <p>⚠️ 8 MiB, not 1: a delta batches many nodes' commits (M4.7), so a
     * reply is legitimately larger than a request. Still far below a heap.
     */
    static final long MAX_REPLY_BYTES = 8L << 20;

    /** ⚠️ 4 KiB of a peer's error text is plenty to diagnose with. */
    private static final long MAX_MESSAGE_BYTES = 4L << 10;

    private static byte[] bounded(HttpClientResponse response, long limit) throws IOException {
        try (var in = new BoundedStream(response.inputStream(), limit)) {
            return in.readAllBytes();
        }
    }

    /** What one peer's reply says, for an operator on THIS node. */
    private static String read(HttpClientResponse response) {
        try {
            String text = new String(bounded(response, MAX_MESSAGE_BYTES),
                    java.nio.charset.StandardCharsets.UTF_8);
            return text.length() <= 200 ? text : text.substring(0, 200) + "...";
        } catch (IOException | RuntimeException unreadable) {
            // ⚠️ INCLUDING `BodyTooLargeException`: a peer whose error text is
            // megabytes long is one more thing not to believe, and an operator
            // reading THIS node's log needs the status either way.
            return "<no readable body>";
        }
    }

    /**
     * ⚠️ **BOUNDED, BECAUSE THE ENDPOINT COMES FROM THE LEASE AND PODS MOVE.**
     * A rescheduled pod returns at a NEW address, so an unbounded map grows
     * with peer INCARNATIONS for the process's life — a cluster rolling its
     * ingesters daily accumulates an entry per dead address, for ever.
     *
     * <p>⚠️ **WHAT THE BOUND SAVES IS THE MAP, NOT SOCKETS**, and an earlier
     * version of this comment claimed file descriptors: Helidon keeps
     * connections in a process-wide cache keyed by host and port, so neither
     * this cap nor {@link #close()} releases one. 64 is far more than the peers
     * a real fleet has at once and far less than an unbounded map.
     */
    static final int MAX_POOLED_CLIENTS = 64;

    private WebClient clientFor(String endpoint) {
        WebClient existing = clients.get(endpoint);
        if (existing != null) {
            return existing;
        }
        if (clients.size() >= MAX_POOLED_CLIENTS) {
            // ⚠️ CLEARED RATHER THAN EVICTED ONE BY ONE, and the crude answer
            // is the right one here: the map is a CACHE, every entry is
            // rebuildable on the next commit, and an LRU would need an access
            // order this class has no reason to track. What matters is that it
            // cannot grow without bound.
            clients.clear();
        }
        // ⚠️ NO KEEP-ALIVE (M10.36): this client is used from more than one
        // thread, and Helidon 4.3.0 re-queues a kept-alive connection BEFORE
        // it starts the idle monitor that reads one byte -- a second thread
        // taking it in that window loses its answer's first byte ("Protocol
        // is not HTTP: TTP", M10.35). A connection never re-queued cannot be
        // taken there; the cost is one TCP connect per request, and every
        // request here is per segment or per flush, never per record.
        return clients.computeIfAbsent(endpoint, uri -> WebClient.builder()
                .baseUri(URI.create(uri))
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .keepAlive(false)
                .build());
    }

    /**
     * How many peers this transport currently holds a client for.
     *
     * <p>⚠️ **PACKAGE-PRIVATE AND FOR ONE TEST**, which is a cost worth naming:
     * without it the reuse case can only assert that five commits arrived,
     * which is equally true of five clients — and a client per commit is a
     * socket per flush per node, which is what this pool exists to prevent.
     */
    int pooledClients() {
        return clients.size();
    }

    @Override
    public void close() {
        closed = true;
        clients.values().forEach(WebClient::closeResource);
        clients.clear();
    }
}
