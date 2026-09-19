// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.format.SegmentRecord;
import binjava.ingest.AppendResult;
import binjava.ingest.Ingest;
import binjava.ingest.PlacementRefusedException;
import binjava.ingest.RegistrationTimeoutException;
import binjava.security.Principal;
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
     * concurrently" property {@link binjava.ingest.DefaultIngest}'s own
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
        this.ingest = Objects.requireNonNull(ingest, "ingest");
        this.principal = Objects.requireNonNull(principal, "principal");
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    private final DrainGate gate;

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
            placement = placementOf(request);
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

        try {
            // ⚠️ Each CHUNK's append blocks until ITS segment and commit delta
            // are durable (criterion 1); the 202 below means every chunk landed
            // durably, not that any one of them was merely accepted into a
            // buffer.
            appendBulkBody(request.content().inputStream(), index, placement);
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
        List<SegmentRecord> chunk = new ArrayList<>(APPEND_CHUNK_RECORDS);
        int[] seen = {0};
        AppendResult[] last = {null};
        try {
            BulkParser.parse(new BoundedStream(body, MAX_BODY_BYTES), r -> {
                if (++seen[0] > MAX_RECORDS) {
                    throw new BodyTooLargeException(
                            "the request exceeds " + MAX_RECORDS + " records");
                }
                chunk.add(r);
                if (chunk.size() >= APPEND_CHUNK_RECORDS) {
                    last[0] = appendChunk(index, placement, chunk);
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
                last[0] = appendChunk(index, placement, chunk);
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
            List<SegmentRecord> chunk) {
        try {
            return placement.routing() == null
                    ? ingest.append(principal, index, placement.partition(), chunk::forEach)
                    : ingest.appendRouted(principal, index, placement.routing(),
                            chunk::forEach);
        } catch (IOException e) {
            throw new ChunkAppendException(e);
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
    record Placement(Integer partition, String routing) {

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

    /**
     * ⚠️ EXACTLY ONE OF THE TWO, AND NEITHER HAS A DEFAULT. Defaulting the
     * partition to 0 funnels every producer that forgot the parameter into one
     * shard while returning 202; accepting both and preferring one silently
     * makes a producer's stated intent depend on which the implementation
     * happens to read first.
     */
    private static Placement placementOf(ServerRequest request) {
        var rawPartition = request.query().first("partition");
        var routing = request.query().first("routing");
        if (rawPartition.isPresent() && routing.isPresent()) {
            throw new BulkParseException("'partition' and 'routing' are alternatives: the "
                    + "first says where these records go, the second asks the ingester to "
                    + "work it out. Send one");
        }
        if (routing.isPresent()) {
            if (routing.get().isEmpty()) {
                // ⚠️ AN EMPTY ROUTING VALUE IS REFUSED HERE, not placed. It
                // hashes to a partition like any other string, so accepting it
                // would send every producer that built its query string wrong
                // to one shard -- indistinguishable from a working deployment
                // until the shard is hot.
                throw new BulkParseException("'routing' is not empty");
            }
            return new Placement(null, routing.get());
        }
        String raw = rawPartition.orElseThrow(() -> new BulkParseException(
                "one of the 'partition' or 'routing' query parameters is required: there is "
                        + "no default, because defaulting to partition 0 funnels every "
                        + "producer that forgot it into one shard while returning 202"));
        int partition;
        try {
            partition = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new BulkParseException("'partition' is an integer");
        }
        if (partition < 0) {
            throw new BulkParseException("'partition' is not negative");
        }
        return new Placement(partition, null);
    }
}
