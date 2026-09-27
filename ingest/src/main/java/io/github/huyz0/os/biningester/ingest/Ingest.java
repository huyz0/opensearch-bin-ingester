// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
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
     * lets {@link io.github.huyz0.os.biningester.ingest.Ingest#append} hand each one straight to the
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

    /**
     * Appends records whose partition the INGESTER computes, from a routing
     * value (M6.6, FR-19, ADR-0015 § 1).
     *
     * <p>⚠️ THE PRODUCER NEVER LEARNS THE SHARD COUNT, which is the whole point
     * of the mode: it would otherwise need an endpoint on the plugin or
     * OpenSearch credentials of its own, both of which ADR-0015 rejects. It
     * sends a routing value; placement happens where the registered shape is
     * known.
     *
     * <p>⚠️ {@code indexOrAlias} MAY BE AN ALIAS, resolved to the current
     * concrete index by whoever holds the catalog -- never by the HTTP adapter,
     * which owns no decision (ADR-0019).
     *
     * <p>⚠️ THE DEFAULT REFUSES, and says what is missing rather than placing
     * the records somewhere. ⚠️ AS A PLACEMENT REFUSAL, which the HTTP layer
     * answers with 400: anything else is a 500, which reads as transient and is
     * retried for ever by a deployment that can never place it (M6.19). A deployment with no catalog has no shard count
     * for any index, so every routed write is unplaceable; answering with
     * partition 0 would funnel an index into one shard while returning 202.
     *
     * @throws PlacementRefusedException if this implementation cannot place by
     *     routing value
     */
    default AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            RecordSource records) throws IOException {
        throw new PlacementRefusedException("this ingester has no index catalog, so it cannot "
                + "compute a partition from a routing value -- write with an explicit "
                + "partition, or deploy the plugin that registers index shapes (FR-16)");
    }

    /**
     * Appends records of priority lane {@code lane} (FR-18, ADR-0074).
     *
     * <p>⚠️ THE DEFAULT REFUSES ANY LANE BUT 0, as a placement refusal (a 400
     * at the HTTP layer), rather than dropping it: an implementation that
     * schedules no lanes and accepted a {@code +2} write as lane 0 would demote
     * it silently while returning 202.
     *
     * @throws PlacementRefusedException if {@code lane} is not 0 and this
     *     implementation schedules no lanes
     */
    default AppendResult append(Principal principal, String index, int partition, byte lane,
            RecordSource records) throws IOException {
        refuseLane(lane);
        return append(principal, index, partition, records);
    }

    /** The routed form of {@link #append(Principal, String, int, byte, RecordSource)}. */
    default AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            byte lane, RecordSource records) throws IOException {
        refuseLane(lane);
        return appendRouted(principal, indexOrAlias, routing, records);
    }

    /**
     * Whether this ingester schedules records of {@code lane} (ADR-0074).
     *
     * <p>⚠️ A QUERY, SO A WRAPPER CAN REFUSE BEFORE IT DOES ANY WORK: a routed
     * write to an unregistered index would otherwise be pooled and held for
     * the whole registration timeout, then answered 503 -- which a producer
     * retries for ever -- for a lane that was never going to be accepted.
     */
    default boolean acceptsLane(byte lane) {
        return lane == 0;
    }

    private static void refuseLane(byte lane) {
        if (lane != 0) {
            throw new PlacementRefusedException("lane " + lane + " was asked for, and this "
                    + "ingester schedules no priority lanes");
        }
    }

    /** Flushes anything buffered and releases resources. */
    @Override
    void close() throws IOException;
}
