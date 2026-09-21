// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * The only durable dependency: an object store, reduced to what this system
 * needs (docs/research/30-design-space/07-pluggable-store-abstraction.md).
 *
 * <p>⚠️ BLOCKING, not {@code CompletableFuture}. With virtual threads a blocking
 * call IS the concurrent API and is far easier to reason about. AutoMQ's SPI is
 * future-based because it predates ubiquitous virtual threads; diverging here is
 * deliberate.
 *
 * <p>⚠️ Every request through this interface is counted and budgeted. Request
 * rates scale with segments, AZs and nodes — never with records, shards,
 * partitions or indices (AGENTS.md non-negotiable 6). A new method that a caller
 * could invoke per record is a design defect, not an optimisation problem.
 *
 * <p>⚠️ M1 SUBSET, since narrowed. {@code putIfMatch} arrived in M2.0
 * (ADR-0008): the sequencer lease and the ordinal registry both mutate rather
 * than append, and M1 never needed a CAS primitive expressing that. {@code
 * multipart} arrived in M2.2, for a segment too large for one {@link Body}.
 * Both are additions, not changes — no M1 signature moved to accommodate
 * either.
 */
public interface BinStore extends Closeable {
    // ⚠️ Every I/O method declares IOException. An object store that cannot
    // signal failure forces implementations into UncheckedIOException, and a
    // caller that cannot see the failure in the signature does not retry — the
    // commit-log retry loop (ADR-0002) is built on distinguishing "lost the
    // race" (empty Optional) from "the store is unreachable" (this).


    /** The whole object, streamed. Never materialised. */
    InputStream get(String key) throws IOException;

    /** Bytes {@code [start, endIncl]}, streamed. */
    InputStream getRange(String key, long start, long endIncl) throws IOException;

    /** Size and version without reading the body, or empty if absent. */
    Optional<ObjectStat> stat(String key) throws IOException;

    /** Unconditional write. */
    Version put(String key, Body body) throws IOException;

    /**
     * Write only if the key does not exist.
     *
     * <p>⚠️ Empty means ALREADY EXISTS — a normal outcome, not an error. The
     * commit log is a chain of these (ADR-0002); a loser re-reads and retries at
     * the next sequence number, so this must never throw for a lost race.
     */
    Optional<Version> putIfAbsent(String key, Body body) throws IOException;

    /**
     * Write only if the object's current version equals {@code expected}.
     *
     * <p>⚠️ Empty means VERSION MOVED — a normal outcome for the losing side of
     * a lease renewal or an ordinal-registry update (ADR-0008), never an
     * exception: the caller re-reads and decides whether to retry. An ABSENT
     * key is a different failure and is NOT folded into that empty case — there
     * is no version to have moved from, so this throws instead. Both primitives
     * this store offers are write-based: {@link #putIfAbsent} for a sequence
     * that is never rewritten, this for the small set of objects that mutate
     * (the sequencer lease, the ordinal registry) where read-modify-write is
     * safe because contention is not at rate (ADR-0008).
     */
    Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException;

    /**
     * Begins a multipart upload for {@code key}, for an object too large to
     * hand to {@link #put} as one {@link Body} (M2.2; research doc 01 §7).
     * The key is not occupied until {@link MultipartWriter#complete()}
     * succeeds — {@code stat(key)} sees nothing while parts are staged.
     */
    MultipartWriter multipart(String key) throws IOException;

    /**
     * ONE page of keys under {@code prefix}, in lexicographic order.
     *
     * <p>⚠️ LIST is the expensive operation and is confined to recovery and GC
     * (cost rule R2). It is on the interface because recovery genuinely needs
     * it, not because a hot path may call it.
     *
     * <p>⚠️ ONE CALL IS ONE REQUEST. An earlier signature returned a lazy
     * {@code Stream} and hid paging inside the backend, so the counting
     * decorator saw one invocation whether S3 issued one request or ten
     * thousand — R9 under-reported by four orders of magnitude and a
     * zero-LIST assertion could pass over thousands of billed requests.
     *
     * @param startAfter resume strictly after this key, or null to start at the
     *     beginning. A value sorting BEFORE {@code prefix} does not skip the
     *     prefix; the seek is from the later of the two.
     * @param maxKeys upper bound on this page; the backend may return fewer
     */
    ListPage list(String prefix, String startAfter, int maxKeys) throws IOException;

    /** Delete in one request where the backend supports it. Absent keys are not an error. */
    void delete(List<String> keys) throws IOException;

    /** What this backend can do; checked at startup, never per request. */
    Capabilities capabilities();

    /**
     * A short-lived URL reading ONE object without credentials (M5.10,
     * ADR-0041).
     *
     * <p>⚠️ ONE KEY, AND A TTL THE CALLER CHOOSES, per security.md rule 3.
     * There is deliberately no prefix or bucket form: a grant that covers more
     * than the object being served is a wider grant than the fetch needs, and
     * it ends up in the OpenSearch JVM -- as a String, since ADR-0023 keeps
     * this module out of {@code client} and {@code plugin}, so
     * {@link SignedUrl}'s redaction does not travel with it.
     *
     * <p>⚠️ SIGNING ISSUES NO REQUEST PER SIGNATURE -- to the object store or
     * to anything else. The qualifier is load-bearing in both directions, and
     * two earlier drafts each got one of them wrong: "no OBJECT-STORE request"
     * is satisfied literally by a GCS backend calling
     * {@code iam.serviceAccounts.signBlob}, since that RPC goes to
     * {@code iamcredentials.googleapis.com} while still costing a round trip
     * for every URL; and "no request of ANY kind" is violated literally by the
     * Azure carve-out below, since {@code getUserDelegationKey} is a request.
     * PER SIGNATURE is the line: a key fetched once and reused for days is
     * amortised, a call per URL is not.
     *
     * <p>⚠️ AND THE URL NAMES THE KEY IT WAS ASKED FOR, which the conformance
     * suite asserts and this contract must therefore state.
     *
     * <p>⚠️ THE OBJECTION IS NOT non-negotiable 6, and an earlier draft cited
     * it wrongly: that rule names records, shards, partitions and indices,
     * while cost.md's invariant PERMITS scaling with nodes and rule 10 says
     * read cost does. A consumer is one per node, so per-consumer scaling is
     * allowed. The real objection is simpler and worse: {@code direct} is
     * chosen per fetch, so a signing round trip DOUBLES the request count on
     * the one path {@code direct} exists to make cheap -- and it arrives under
     * exactly the load that made the ingester choose it.
     *
     * <p>⚠️ WHICH BACKENDS THIS EXCLUDES, corrected: S3 signs locally from the
     * credential. An AZURE user-delegation SAS ALSO signs locally -- a
     * {@code getUserDelegationKey} call yields a key valid for up to seven
     * days, held by the application, and the SAS itself is a local HMAC, so the
     * cost amortises the way an STS refresh does on S3. An earlier draft listed
     * Azure as unable, which would have had the first Azure backend disable
     * {@code direct} for a reason that is not true. What is excluded is a
     * signature needing a round trip PER URL -- GCS V4 from a keyless Workload
     * Identity is the live example. ⚠️ SUCH A BACKEND ADVERTISES
     * {@code presignedUrls=false} rather than signing remotely.
     *
     * <p>⚠️ THE TTL MUST BE POSITIVE, and a backend rejects one that is not.
     * The upper bound is NOT enforced here: {@code io.github.huyz0.os.biningester.ingest.GrantIssuer}
     * clamps to it, because a ceiling belongs with the thing that decides to
     * hand a grant out rather than with the thing that signs. ⚠️ The bound
     * itself is ADR-0010's, at ≤60 s -- an earlier note here said M5.13 would
     * "wire it from configuration", which read as though the NUMBER were open.
     * It is not; only shortening it is.
     *
     * <p>⚠️ AND A BACKEND'S OWN EXCEPTION MESSAGES ARE BOUND BY security.md
     * RULE 4. If signing fails, whatever this method throws may be chained,
     * printed and pasted into a ticket by a caller that cannot inspect it --
     * so the message must carry NEITHER a signed URL NOR a credential. A
     * signing failure is the likeliest place in the whole system for a
     * credential to surface in text. ⚠️ CHECKED PER BACKEND, not by the
     * shared suite: {@code S3PresignTest} checks it for S3 (M8.18), and a new
     * capable backend owes the same case.
     *
     * <p>⚠️ THE DEFAULT REFUSES, so a backend that has not implemented this
     * cannot silently return something unusable. It is paired with
     * {@link Capabilities#presignedUrls()}: a deployment wanting {@code direct}
     * calls {@link Capabilities#requirePresignedUrls()} at startup and never
     * reaches this.
     *
     * @param key the single object the grant covers
     * @param ttl how long the grant lives; must be positive
     * @throws UnsupportedOperationException if this backend cannot presign,
     *     which {@link Capabilities#presignedUrls()} reports in advance
     * @throws IllegalArgumentException if {@code ttl} is zero or negative --
     *     the conformance suite requires this, and an earlier draft of this
     *     block documented only the line above
     */
    default SignedUrl presign(String key, java.time.Duration ttl) throws IOException {
        throw new UnsupportedOperationException(
                "this backend cannot presign (ADR-0041); Capabilities.presignedUrls() "
                        + "reports that before startup");
    }
}
