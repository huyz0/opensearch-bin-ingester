// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

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
 * <p>⚠️ M1 SUBSET, since narrowed. {@code putIfMatch} arrives in M2 (ADR-0008,
 * M2.0): the sequencer lease and the ordinal registry both mutate rather than
 * append, and M1 never needed a CAS primitive expressing that. Multipart is
 * still not here — a segment large enough to need it arrives with M2's later
 * tasks. Both are additions, not changes — no M1 signature moved to
 * accommodate either.
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
}
