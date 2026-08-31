// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.format.SegmentRecord;
import binjava.ingest.Ingest;
import binjava.security.Principal;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
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
     * ⚠️ 32 MiB, not the 200 MB of criterion 8. This handler accumulates records
     * before appending (see {@code bulk}), and a SegmentRecord copies its
     * payload, so the retained set exceeds the body.
     *
     * <p>⚠️ A byte cap ALONE does not bound the retained heap, and an earlier
     * version of this note claimed "2-3x the body", which is true only for
     * realistic documents. The shape this cap exists to resist is the smallest
     * legal record: {@code {"index":{"_id":"a"}}\n1\n} is 24 bytes, so 32 MiB
     * is ~1.4M records, and each retains a SegmentRecord (~32 B) + the id String
     * (~48 B) + a byte[1] (~24 B) + a list slot (4 B) ≈ 108 B — about 150 MB,
     * some 6x. Three such requests at once exceed the 512 MiB the conventions
     * plugin sets. Hence {@link #MAX_RECORDS} as well: the count is what
     * dominates, and the byte cap does not bound it.
     */
    static final long MAX_BODY_BYTES = 32L << 20;

    /**
     * ⚠️ The companion ceiling. 200,000 records × ~108 B of per-record overhead
     * is ~22 MB retained, which keeps several concurrent requests inside a
     * modest heap even at the adversarial shape above. Raising either constant
     * without first letting the ingest seam accept a stream (M1.7b) just moves
     * where the OutOfMemoryError happens.
     */
    static final int MAX_RECORDS = 200_000;

    private final Ingest ingest;
    private final Principal principal;

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
        this.ingest = Objects.requireNonNull(ingest, "ingest");
        this.principal = Objects.requireNonNull(principal, "principal");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/{index}/_bulk", this::bulk);
    }

    private void bulk(ServerRequest request, ServerResponse response) {
        String index = request.path().pathParameters().get("index");
        int partition;
        try {
            partition = partitionOf(request);
        } catch (BulkParseException e) {
            response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            return;
        }

        List<SegmentRecord> records = new ArrayList<>();
        try {
            // ⚠️ BulkParser streams; THIS HANDLER DOES NOT. `Ingest.append` takes
            // a List, so every record of the request is retained here until the
            // append -- one byte[] and one String per action, at full body size.
            // Do not read the line below as "the adapter streams end to end": it
            // does not, and SPEC criterion 8 (200 MB body under a 256 MB heap) is
            // NOT reachable through this path. Closing that means letting the
            // ingest seam accept a stream, which is an API change and M1.7b.
            //
            // ⚠️ Until then the body is CAPPED, because an unbounded accumulate
            // ends in an OutOfMemoryError -- an Error, which reaches neither the
            // 400 nor the 503 path and takes the connection with it. A refusal
            // the producer can see and retry is strictly better than a heap dump.
            BulkParser.parse(new BoundedStream(request.content().inputStream(), MAX_BODY_BYTES),
                    r -> {
                        if (records.size() >= MAX_RECORDS) {
                            throw new BodyTooLargeException(
                                    "the request exceeds " + MAX_RECORDS + " records");
                        }
                        records.add(r);
                    });
        } catch (BodyTooLargeException e) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send(e.getMessage());
            return;
        } catch (BulkParseException e) {
            // ⚠️ Nothing has been appended: parsing completes before the append.
            // A partial append behind a 400 would leave the producer's retry
            // duplicating exactly the records that did land.
            response.status(Status.BAD_REQUEST_400).send(e.getMessage());
            return;
        } catch (IOException e) {
            response.status(Status.BAD_REQUEST_400).send("could not read the request body");
            return;
        }

        if (records.isEmpty()) {
            response.status(Status.BAD_REQUEST_400).send("an empty bulk body has nothing to append");
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
        if (!principal.canWriteTo(index)) {
            response.status(Status.FORBIDDEN_403).send();
            return;
        }

        try {
            // ⚠️ BLOCKING, on a virtual thread. append() returns when the segment
            // and its commit delta are BOTH durable (criterion 1), so the 202
            // below means durable rather than accepted-into-a-buffer.
            ingest.append(principal, index, partition, records);
        } catch (IOException e) {
            // ⚠️ 503, not 500. The store is unavailable; the producer should
            // retry the same batch, and an external version makes that safe.
            response.status(Status.SERVICE_UNAVAILABLE_503).send();
            return;
        }
        response.status(Status.ACCEPTED_202).send();
    }

    private static int partitionOf(ServerRequest request) {
        String raw = request.query().first("partition").orElseThrow(
                () -> new BulkParseException("the 'partition' query parameter is required"));
        int partition;
        try {
            partition = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new BulkParseException("'partition' is an integer");
        }
        if (partition < 0) {
            throw new BulkParseException("'partition' is not negative");
        }
        return partition;
    }
}
