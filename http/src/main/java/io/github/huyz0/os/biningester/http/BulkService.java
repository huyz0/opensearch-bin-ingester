// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
import io.github.huyz0.os.biningester.ingest.PlacementRefusedException;
import io.github.huyz0.os.biningester.ingest.RegistrationTimeoutException;
import io.github.huyz0.os.biningester.ingest.RegistrationWaitFullException;
import io.github.huyz0.os.biningester.security.Principal;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@code POST /{index}/_bulk?partition=N} — the front door.
 *
 * <p>⚠️ THIS OWNS NO DECISION (ADR-0019, architecture.md rule 4). It parses,
 * delegates to {@link Ingest}, and maps an exception to a status code. It does
 * not choose a partition, rewrite an {@code _id}, batch across requests, or
 * acknowledge before the records are durable. Anything it decided here would be
 * a decision the in-process API could not make, and the in-process API is the
 * primary one.
 *
 * <p>⚠️ The partition is an EXPLICIT query parameter with NO default. M1 has no
 * aliases and no {@code os_routing} (ADR-0015, M6), so there is nothing to
 * derive it from — and defaulting to 0 would funnel every producer that forgot
 * the parameter into one partition while returning 202.
 */
public final class BulkService implements HttpService {

    /**
     * ⚠️ 256 MiB, headroom over criterion 8's 200 MB rather than a bare
     * minimum -- M1.7b let the ingest seam accept a stream, so a request no
     * longer retains its whole body, and {@code MemoryFlatUnderTenXBodySizeTest}
     * (T12) proves 200 MB is safe under a 256 MB HEAP with this cap in place.
     * Raising this further than criterion 8 itself needs would outrun what has
     * actually been proven.
     */
    static final long MAX_BODY_BYTES = 256L << 20;

    /**
     * ⚠️ 2,000,000 -- headroom over what a 256 MiB body of realistic
     * documents needs, sized the same way {@link #MAX_BODY_BYTES} is. Chunking
     * (M1.7b) already bounds retained heap independent of the TOTAL record
     * count, so unlike before M1.7b this ceiling is mostly a REQUEST-DURATION
     * guard against a pathological number of minimal actions, not the primary
     * memory-safety mechanism the byte cap alone used to be.
     */
    static final int MAX_RECORDS = 2_000_000;

    /**
     * ⚠️ BOUNDS BOTH THE RETAINED MEMORY AND THE ACCUMULATOR LOCK's HOLD TIME
     * (M1.7b review finding, round 1). {@code Ingest.append} holds
     * {@code DefaultIngest}'s single per-pod lock for as long as its
     * {@code RecordSource} takes to run -- correct for a caller whose source
     * is a fast in-memory iteration, but this handler's source is
     * {@code BulkParser} reading off the REQUEST'S OWN SOCKET. Handing the
     * whole body to ONE {@code append} call would hold that lock, shared by
     * EVERY producer on the pod, for as long as this one connection takes to
     * deliver its bytes -- exactly the "many producers share one segment,
     * concurrently" property {@link io.github.huyz0.os.biningester.ingest.DefaultIngest}'s own
     * javadoc describes, defeated by one slow client. Chunking bounds it to
     * "however long it takes to add {@value} already-parsed records to an
     * in-memory accumulator", independent of network speed or body size.
     */
    static final int APPEND_CHUNK_RECORDS = 1_000;

    private final Ingest ingest;
    private final Principal principal;

    /** A body IOException, distinguished from a STORE IOException (M1.7b). */
    private static final class BulkBodyReadException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        BulkBodyReadException(IOException cause) {
            super(cause);
        }
    }

    /** A STORE IOException from appending one chunk, escaping BulkParser's sink. */
    private static final class ChunkAppendException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ChunkAppendException(IOException cause) {
            super(cause);
        }
    }

    /**
     * ⚠️ M1 takes a FIXED principal and does not authenticate per request.
     * {@code CredentialSource} (M1.0) is the seam that keeps per-request
     * authentication a change to this class alone.
     *
     * <p>⚠️ Stated plainly because a `_bulk` endpoint that looks authenticated
     * and is not is worse than one that plainly is not.
     *
     * <p>⚠️ ONE answer to "when is this replaced": <b>M1.7c</b>, a todo row in
     * the M1 section of the backlog. It is NOT required by M1's completion
     * condition — SPEC § Scope puts authentication out of M1 — but the row is
     * where it lives, and no roadmap milestone owns it.
     *
     * <p>Two earlier versions of this note were wrong in different ways, which
     * is why the pointer is spelled out rather than paraphrased: the first said
     * "wiring it is M1.8's row" (M1.8 is the accumulator task, already done);
     * the second added a `SKELETON: replaced by M6` marker, but M6's roadmap row
     * enumerates its scope and authentication is not in it.
     */
    // SKELETON: replaced by M1.7c
    public BulkService(Ingest ingest, Principal principal) {
        this(ingest, principal, new DrainGate());
    }

    /**
     * The same, admitting requests through a gate the node drains on
     * shutdown (M8.7).
     *
     * <p>⚠️ **THE CONSTRUCTOR ABOVE GETS A GATE NOBODY DRAINS**, which is the
     * behaviour before M8.7. Only the composition root passes one it will
     * close.
     */
    public BulkService(Ingest ingest, Principal principal, DrainGate gate) {
        this(ingest, principal, gate, new LaneAdmission(
                LaneAdmission.DEFAULT_MAX_IN_FLIGHT_BULK, LaneSet.defaults()));
    }

    private final DrainGate gate;

    /**
     * The same, admitting each request through the lanes' fair share of the
     * pod's in-flight budget (M10.8, ADR-0074 decision 6).
     *
     * <p>⚠️ **THE CONSTRUCTORS ABOVE GET THE DEFAULT BUDGET OVER THE DEFAULT
     * LANES**, whatever set the ingest behind them schedules. Only the
     * composition root builds one from the pod's own configuration.
     */
    public BulkService(Ingest ingest, Principal principal, DrainGate gate,
            LaneAdmission admission) {
        this(ingest, principal, gate, admission, IndexQuotas.none());
    }

    /**
     * The same, admitting each request through its index's quota as well
     * (M11.8, ADR-0078).
     *
     * <p>⚠️ **THE CONSTRUCTORS ABOVE REFUSE NOTHING BY QUOTA**; only the
     * composition root builds quotas from the pod's configuration.
     */
    public BulkService(Ingest ingest, Principal principal, DrainGate gate,
            LaneAdmission admission, IndexQuotas quotas) {
        this(ingest, principal, gate, admission, quotas, RefusalListener.UNCOUNTED);
    }

    /** Told of each {@code 429} as it is sent (M12.5). */
    public interface RefusalListener {
        /** Counts nothing: what every constructor above uses. */
        RefusalListener UNCOUNTED = new RefusalListener() {
            @Override
            public void admissionRefused() {
            }

            @Override
            public void quotaRefused(String index) {
            }

            @Override
            public void registrationWaitRefused() {
            }
        };

        /** The pod's in-flight budget refused a request (lane admission). */
        void admissionRefused();

        /** {@code index}'s quota refused a request. */
        void quotaRefused(String index);

        /**
         * An explicit-partition write was refused its wait for an unregistered
         * index's registration: as many writes already wait as may, for that
         * index or across the ingester (M13.11, M12.10 review P1) -- not the
         * pod's in-flight budget.
         */
        void registrationWaitRefused();
    }

    /** The same, telling {@code refusals} of each {@code 429} it sends (M12.5). */
    public BulkService(Ingest ingest, Principal principal, DrainGate gate,
            LaneAdmission admission, IndexQuotas quotas, RefusalListener refusals) {
        this.refusals = Objects.requireNonNull(refusals, "refusals");
        this.ingest = Objects.requireNonNull(ingest, "ingest");
        this.principal = Objects.requireNonNull(principal, "principal");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.quotas = Objects.requireNonNull(quotas, "quotas");
    }

    private final LaneAdmission admission;
    private final IndexQuotas quotas;
    private final RefusalListener refusals;

    @Override
    public void routing(HttpRules rules) {
        rules.post("/{index}/_bulk", this::bulk);
    }

    private void bulk(ServerRequest request, ServerResponse response) {
        if (!gate.enterBulk()) {
            // ⚠️ 503, WHICH A PRODUCER ALREADY RETRIES. Nothing was appended,
            // so the retry through the cluster address, to a pod that is
            // still ready, is safe.
            response.status(Status.SERVICE_UNAVAILABLE_503)
                    .send("this ingester is draining; retry");
            return;
        }
        try {
            admitted(request, response);
        } finally {
            gate.exitBulk();
        }
    }

    private void admitted(ServerRequest request, ServerResponse response) {
        String index = request.path().pathParameters().get("index");
        Placement placement;
        try {
            placement = PlacementParser.placementOf(request);
        } catch (BulkParseException e) {
            response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            return;
        }

        // ⚠️ Authorization is checked HERE, from the Principal, and NOT by
        // catching IllegalArgumentException out of append(). IAE is also how the
        // append path reports its own invariant failures (AppendResult and
        // CommitLog both throw it), and mapping those to 403 tells the producer
        // its write was permanently refused -- so it drops the batch and the
        // data is lost, with an implementation bug reported as a permissions
        // problem.
        //
        // ⚠️ AND THIS IS CURRENTLY THE ONLY ENFORCEMENT POINT. An earlier
        // version of this comment said "`Ingest` still enforces this
        // independently" -- that was FALSE: no production `Ingest`
        // implementation exists yet, and `Principal.canWriteTo` has exactly one
        // production caller, the line below. So the class whose javadoc opens
        // "THIS OWNS NO DECISION" is, for now, the only thing deciding.
        // ⚠️ `Ingest.append`'s contract still names IllegalArgumentException for
        // an authorization denial, so the first implementer will reintroduce the
        // collision. Giving that seam its own exception type is M1.7f, and
        // Ingest's javadoc now points at it rather than leaving the trap set.
        //
        // ⚠️ 403 with NO body: naming the index tells an unauthorised caller
        // which indices exist.
        //
        // ⚠️ MOVED BEFORE THE BODY IS READ (M1.7b): parsing and appending are now
        // one streamed call rather than "parse fully, then append", so there is
        // no longer a natural point between them to check this. Checking first
        // is strictly better: an unauthorised caller's body -- however large --
        // is never even opened.
        if (!principal.canWriteTo(index)) {
            response.status(Status.FORBIDDEN_403).send();
            return;
        }

        // ⚠️ ADMITTED AFTER THE LANE IS KNOWN AND BEFORE THE BODY IS OPENED
        // (M10.8, ADR-0074 decision 6): a refused request costs the pod no
        // parse and no buffer. 429 and NEVER a 5xx -- nothing failed, the pod
        // is busy, and a producer already retries 429 (OpenSearch bulk
        // semantics). ⚠️ THE PERMIT IS RETURNED IN THE FINALLY, so a store
        // failure, a bad body and a defect escaping as a 500 all give it back:
        // a leaked permit is a pod that answers 429 for ever.
        // ⚠️ THE INGESTER IS ASKED FIRST WHETHER THE LANE IS ACTIVE (review
        // round 1): an inactive lane is the producer's permanent mistake, and
        // answering it 429 on a saturated pod would have it retried until the
        // pod was idle enough to say 400. The ingester still decides; this
        // only asks it before spending a permit.
        if (!ingest.acceptsLane(placement.lane())) {
            response.status(Status.BAD_REQUEST_400).send("lane " + placement.lane()
                    + " is not active on this ingester");
            return;
        }
        // ⚠️ THE REGISTRATION WAIT's CAP, BEFORE THE PERMIT AND THE BODY (M13.11,
        // M12.10 review P2), as every other 429: a write that would be refused
        // its wait costs no permit and no parse. Advice only -- the wait still
        // refuses a write that loses the race, caught below.
        if (placement.partition() != null && ingest.registrationWaitFull(index)) {
            refusals.registrationWaitRefused();
            tooManyRequests(response, 1, "index " + index + " is not registered and as many "
                    + "writes wait for registrations as may; retry");
            return;
        }
        var permit = admission.tryAcquire(placement.lane());
        if (permit.isEmpty()) {
            refusals.admissionRefused();
            // ⚠️ ONE SECOND: a saturated pod frees a permit as soon as any
            // request in flight completes, which is well under a second at
            // any rate this refusal is reached.
            tooManyRequests(response, ADMISSION_RETRY_AFTER_SECONDS, "this ingester is at its "
                    + "in-flight budget and lane " + placement.lane() + " holds its share; retry");
            return;
        }
        // ⚠️ THE INDEX's QUOTA, AFTER THE LANE AND BEFORE THE BODY (M11.8,
        // ADR-0078): a refused request costs no parse and no buffer, and hands
        // its lane permit straight back.
        // ⚠️ BY THE CONCRETE INDEX, never the alias the path names (review P2):
        // an alias and its index are one index, with one bucket and one cap.
        String concrete = ingest.concreteIndex(index);
        IndexQuotas.Admission quota = quotas.admit(concrete);
        if (quota.refusal().isPresent()) {
            permit.get().release();
            refusals.quotaRefused(concrete);
            tooManyRequests(response, quota.refusal().get().retryAfterSeconds(),
                    quota.refusal().get().reason());
            return;
        }
        Admitted held = new Admitted(admission, permit.get(), placement.lane(),
                quota.ticket().get());
        try {
            append(request, response, index, placement, held);
        } finally {
            held.release();
        }
    }

    /** What a lane-admission {@code 429} tells a producer to wait, in seconds (M11.6). */
    static final long ADMISSION_RETRY_AFTER_SECONDS = 1;

    /**
     * Answers {@code 429} with {@code Retry-After} (M11.6, research 14 §3,
     * ADR-0010): ⚠️ **THE ONE PLACE A 429 IS SENT**, so no refusal can leave
     * without the header a producer is told to honour -- a bare 429 is
     * retried at whatever rate the producer's own loop runs, which is the
     * load the refusal existed to shed.
     *
     * @param retryAfterSeconds rounded up to at least 1: {@code Retry-After: 0}
     *     is an instruction to retry immediately
     */
    static void tooManyRequests(ServerResponse response, long retryAfterSeconds, String why) {
        response.status(Status.TOO_MANY_REQUESTS_429)
                .header("Retry-After", Long.toString(Math.max(1, retryAfterSeconds)))
                .send(why);
    }

    private void append(ServerRequest request, ServerResponse response, String index,
            Placement placement, Admitted held) {
        try {
            // ⚠️ Each CHUNK's append blocks until ITS segment and commit delta
            // are durable (criterion 1); the 202 below means every chunk landed
            // durably, not that any one of them was merely accepted into a
            // buffer.
            appendBulkBody(request.content().inputStream(), index, placement, held);
        } catch (PlacementRefusedException e) {
            // ⚠️ 400, AND ONLY FOR THIS TYPE. The comment above says why
            // catching IllegalArgumentException wholesale is worse than a 500:
            // the append path throws IAE for its own invariant failures, and
            // telling a producer its batch is permanently bad makes it drop
            // records over an implementation bug. This subtype names the one
            // condition that IS the producer's and IS permanent -- a partition
            // the index does not have -- which is FR-13's defining clause, and
            // a 500 there is retried forever because 5xx reads as transient.
            response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            return;
        } catch (RegistrationWaitFullException e) {
            // ⚠️ A LOAD REFUSAL, THROUGH THE ONE EMITTER (M12.10): too many writes
            // already wait for registrations -- the race the check above loses.
            // ⚠️ ITS OWN COUNT (M13.11, M12.10 review P1): not the in-flight
            // budget's, which a producer retrying an unregistered index would
            // raise on an idle pod.
            refusals.registrationWaitRefused();
            tooManyRequests(response, e.retryAfterSeconds(), e.getMessage());
            return;
        } catch (RegistrationTimeoutException e) {
            // ⚠️ 503, NOT 400 AND NOT 500. The index's shape has not been
            // pushed yet, which ends on its own: a producer told 400 drops the
            // batch, and one told 503 retries into a registration that has by
            // then usually arrived (ADR-0015's Consequences).
            response.status(Status.SERVICE_UNAVAILABLE_503).send(e.getMessage());
            return;
        } catch (BodyTooLargeException e) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send(e.getMessage());
            return;
        } catch (BulkParseException e) {
            // ⚠️ Some prefix of the body may already be durable-bound (M1.7b):
            // this handler appends in CHUNKS as it parses, rather than parsing
            // the whole body before appending any of it. Safe for a record
            // that carries an external `_version` (ADR-0020) -- a producer's
            // retry of the same corrected body lands the already-applied
            // prefix as a rejected stale version, not a duplicate; a missing
            // version is not covered by that guarantee. See Ingest.append's
            // javadoc.
            response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            return;
        } catch (BulkBodyReadException e) {
            response.status(Status.BAD_REQUEST_400).send("could not read the request body");
            return;
        } catch (IOException e) {
            // ⚠️ 503, not 500. The store is unavailable; the producer should
            // retry the same batch, and an external version makes that safe.
            response.status(Status.SERVICE_UNAVAILABLE_503).send();
            return;
        }
        response.status(Status.ACCEPTED_202).send();
    }

    /**
     * Parses {@code body} and appends it in bounded chunks of
     * {@value #APPEND_CHUNK_RECORDS} records — never the whole body as one
     * {@code List}, and never the whole body as one {@code Ingest.append} call
     * either (see {@link #APPEND_CHUNK_RECORDS}'s javadoc for why the second
     * half matters as much as the first).
     *
     * <p>Package-private so a test can drive it directly, without the real
     * HTTP stack, to prove the first record reaches {@link Ingest#append} after
     * only a small prefix of a large body has been read — the same shape
     * {@code BulkParserTest#bulkBodyIsNeverFullyBuffered} already proves for
     * {@link BulkParser} alone.
     *
     * @return the LAST chunk's result; a caller wanting every chunk's own
     *     offsets does not exist yet — {@link Ingest#append}'s contiguous-range
     *     contract does not extend across chunks, only within one
     */
    AppendResult appendBulkBody(InputStream body, String index, Placement placement)
            throws IOException {
        return appendBulkBody(body, index, placement, null);
    }

    /** The same, giving {@code held}'s permit back as each chunk buffers (M11.7). */
    private AppendResult appendBulkBody(InputStream body, String index, Placement placement,
            Admitted held) throws IOException {
        List<SegmentRecord> chunk = new ArrayList<>(APPEND_CHUNK_RECORDS);
        int[] seen = {0};
        AppendResult[] last = {null};
        try {
            BulkParser.parse(new BoundedStream(body, MAX_BODY_BYTES), r -> {
                if (++seen[0] > MAX_RECORDS) {
                    throw new BodyTooLargeException(
                            "the request exceeds " + MAX_RECORDS + " records");
                }
                if (chunk.isEmpty() && held != null) {
                    // ⚠️ BEFORE THE CHUNK's FIRST RECORD IS HELD, so a request
                    // waiting for its permit holds no parsed records, and every
                    // chunk is parsed and buffered under one (M11.7 review P2).
                    holdForNextChunk(held);
                }
                chunk.add(r);
                if (chunk.size() >= APPEND_CHUNK_RECORDS) {
                    last[0] = appendChunk(index, placement, chunk, held);
                    chunk.clear();
                }
            });
            // ⚠️ INSIDE the same try: the trailing partial chunk (fewer than
            // APPEND_CHUNK_RECORDS records at end of body) is appended here,
            // and it can throw ChunkAppendException exactly like an in-loop
            // chunk does. An earlier draft appended it AFTER this try/catch,
            // where nothing caught that exception -- it escaped as a bare
            // RuntimeException past every catch in bulk() and surfaced as a
            // 500, found by BulkEndpointTest#theTwoOhTwoIsSentOnlyAfterAppendSucceeds
            // going 500 instead of 503.
            if (!chunk.isEmpty()) {
                last[0] = appendChunk(index, placement, chunk, held);
            }
        } catch (IOException e) {
            throw new BulkBodyReadException(e);
        } catch (ChunkAppendException e) {
            // ⚠️ Unwrapped back to a checked IOException here, OUTSIDE
            // BulkParser's sink (which cannot declare one) -- distinct from
            // BulkBodyReadException so bulk()'s own catches still tell "could
            // not read the body" (400) apart from "the store rejected an
            // already-parsed chunk" (503, via the plain IOException below).
            throw (IOException) e.getCause();
        }
        if (seen[0] == 0) {
            // ⚠️ Known only once the body has been read to its end -- there is
            // no chunk, let alone a whole body, whose size is known up front.
            // Thrown from HERE, not left to Ingest.append's own empty-check,
            // so the message stays this adapter's own rather than the
            // library's generic one.
            throw new BulkParseException("an empty bulk body has nothing to append");
        }
        return last[0];
    }

    /**
     * ⚠️ {@code chunk} is consumed SYNCHRONOUSLY and fully by the time this
     * returns -- {@code Ingest.append} is blocking (its own javadoc) and calls
     * {@code chunk::forEach} to completion before this method's caller reuses
     * {@code chunk} via {@code clear()} -- so no defensive copy is needed.
     */
    private AppendResult appendChunk(String index, Placement placement,
            List<SegmentRecord> chunk, Admitted held) {
        Runnable buffered = held == null ? () -> { } : held::buffered;
        try {
            if (held != null) {
                held.charge(chunk);
            }
            return placement.routing() == null
                    ? ingest.append(principal, index, placement.partition(), placement.lane(),
                            chunk::forEach, buffered)
                    : ingest.appendRouted(principal, index, placement.routing(),
                            placement.lane(), chunk::forEach, buffered);
        } catch (IOException e) {
            throw new ChunkAppendException(e);
        }
    }

    private static void holdForNextChunk(Admitted held) {
        try {
            held.holdForNextChunk();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ⚠️ A STORE-SHAPED FAILURE, answered 503 and retried: the prefix
            // already appended carries its external versions (ADR-0020).
            throw new ChunkAppendException(new IOException(
                    "interrupted while waiting to admit the next chunk"));
        }
    }

    /**
     * How this request says where its records go: an explicit partition, or a
     * routing value the INGESTER turns into one (M6.6, FR-13, FR-19).
     *
     * <p>⚠️ IT CARRIES THE CHOICE AND MAKES NEITHER. Which partition a routing
     * value means needs the registered shard count, which lives in the
     * ingester -- ADR-0019 and architecture.md rule 4 keep that decision out of
     * this adapter, and an adapter that resolved it here would be a second
     * implementation of the one thing M6 exists to get exactly right.
     */
    record Placement(Integer partition, String routing, byte lane) {

        Placement(Integer partition, String routing) {
            this(partition, routing, (byte) 0);
        }

        /**
         * ⚠️ EXACTLY ONE, ENFORCED BY THE TYPE. {@code placementOf} refuses
         * neither and both with a 400, and this makes the same rule true of
         * every construction: a {@code Placement(null, null)} reaching
         * {@link #appendChunk} unboxes into an NPE and a 500, which reads to a
         * producer as a server fault it should retry.
         */
        Placement {
            if ((partition == null) == (routing == null)) {
                throw new IllegalArgumentException("a placement is exactly one of a partition "
                        + "and a routing value, never neither and never both");
            }
        }
    }
}
