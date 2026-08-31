// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.SegmentRecord;
import java.util.Objects;

/**
 * A record with the offset the commit log assigned it.
 *
 * <p>WARNING: {@code timestampMillis} is the SEGMENT's creation time -- when the
 * producer's oldest record in it arrived -- never this node's clock. OpenSearch
 * reads it through {@code Message.getTimestamp()}, and an ingestion-time stamp
 * would make a replay produce different documents than the original run.
 *
 * <p>WARNING: the offset comes from the COMMIT LOG, not from the record's
 * position in the segment (ADR-0001). Those coincide within one flush and
 * diverge across flushes, so deriving it from the position passes any
 * single-segment test and then silently restarts every consumer's dedup at zero
 * on the second flush -- which is exactly what test T11c is written to catch.
 */
public record ConsumerRecord(long offset, SegmentRecord record, long timestampMillis) {

    public ConsumerRecord {
        Objects.requireNonNull(record, "record");
        if (offset < 0) {
            throw new IllegalArgumentException("offsets are never negative: " + offset);
        }
    }

    /** The bytes {@code Message.getPayload()} returns. */
    public byte[] payload() {
        return DefaultEnvelope.assemble(record);
    }
}
