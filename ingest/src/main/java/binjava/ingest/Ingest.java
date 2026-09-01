// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.SegmentRecord;
import binjava.security.Principal;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * The library surface: append records, learn when they are durable.
 *
 * <p>⚠️ THIS IS THE PRIMARY API AND HTTP IS AN ADAPTER OVER IT (ADR-0019). The
 * four assumptions ADR-0018 calls unvalidated are all on the consumer side and
 * none of them needs a socket, so this is built and tested first;
 * {@code check-module.sh} then asserts that no module below {@code http}
 * resolves an HTTP dependency, which is what keeps the adapter thin rather than
 * a second implementation.
 *
 * <p>⚠️ BLOCKING, because virtual threads make a blocking call the concurrent
 * API. {@code append} returns when the records are durable, not when they are
 * accepted into a buffer.
 */
public interface Ingest extends AutoCloseable {

    /**
     * A source of records, handed to {@code sink} as they become available.
     *
     * <p>⚠️ PUSH, not pull — java-style.md rule 8: "never materialise a request
     * body or a whole object". A {@code List}-shaped seam would let a caller
     * build the whole batch before appending it, which is exactly what M1.7b
     * closes: {@code BulkParser} already parses one record at a time, and this
     * lets {@link binjava.ingest.Ingest#append} hand each one straight to the
     * accumulator rather than requiring a caller to collect them first. A
     * caller that already holds a {@code List} or other {@code Iterable} can
     * still satisfy this with a method reference — {@code records::forEach}.
     */
    @FunctionalInterface
    interface RecordSource {
        void forEachRecord(Consumer<SegmentRecord> sink) throws IOException;
    }

    /**
     * Appends records to one index and partition, returning once they are
     * durable.
     *
     * <p>⚠️ The {@link Principal} decides the trust domain, and a producer
     * cannot write outside the indices its credential names (ADR-0021). The
     * partition is EXPLICIT in M1: aliases and {@code os_routing} are ADR-0015
     * and land in M6.
     *
     * <p>⚠️ {@code records} is consumed AT MOST ONCE, and an implementation is
     * free to add each record to its accumulator as {@code sink} receives it —
     * so if {@code records} throws partway through (a producer's body turns out
     * malformed after some valid records), whatever was already accumulated
     * stays queued for the next flush rather than being rolled back. That is
     * SAFE for a record that carries an external {@code _version}
     * (ADR-0020): a producer's retry of the same batch lands the
     * already-applied prefix as a rejected stale version — the same mechanism
     * {@code DeleteAndVersionIT} proves — not a duplicate. ⚠️ {@code _version}
     * is OPTIONAL at the wire level (`BulkParser` accepts a missing one), and
     * ADR-0020 says plainly that a missing version DOWNGRADES this guarantee —
     * so for an unversioned record, a retry after a partial failure is not
     * proven safe by anything this seam does; that risk is the producer's, the
     * same as it already is for an unversioned record replayed after any other
     * kind of retry.
     *
     * <p>⚠️ IllegalArgumentException is the WRONG signal for an authorization
     * denial and is retained only until M1.7f gives this seam its own exception
     * type. IAE is also how the append path reports its own invariant failures
     * (see {@link AppendResult} and the commit log), so a caller cannot tell
     * "you may not write here" — permanent, do not retry — from "this
     * implementation has a bug" — retryable, and a lost batch if it is not.
     * The HTTP adapter therefore checks {@link Principal#canWriteTo} itself
     * rather than catching IAE, and an implementer of this interface should not
     * read the tag below as an instruction to keep the collision.
     *
     * @throws IllegalArgumentException if the principal may not write to the
     *     index, or if {@code records} yields nothing — ⚠️ ambiguous, see
     *     above; M1.7f replaces it
     */
    AppendResult append(Principal principal, String index, int partition,
            RecordSource records) throws IOException;

    /** Flushes anything buffered and releases resources. */
    @Override
    void close() throws IOException;
}
