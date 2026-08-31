// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * What a producer learns once its records are durable.
 *
 * <p>⚠️ Returned only AFTER the segment and its commit delta are both in the
 * store (FR-4). A result that meant "buffered" would be a 202 that loses data on
 * a crash, and the M1 test plan's T4 exists to refuse exactly that.
 *
 * @param recordCount how many records were accepted
 * @param firstOffset the offset assigned to the first of them
 * @param lastOffset the offset assigned to the last; equal to firstOffset for a
 *     single record
 */
public record AppendResult(int recordCount, long firstOffset, long lastOffset) {

    public AppendResult {
        if (recordCount <= 0) {
            throw new IllegalArgumentException("an append of nothing has no result");
        }
        if (firstOffset < 0 || lastOffset < firstOffset) {
            throw new IllegalArgumentException(
                    "offsets [" + firstOffset + "," + lastOffset + "] are not a range");
        }
        if (lastOffset - firstOffset != recordCount - 1) {
            // ⚠️ Offsets are CONTIGUOUS within one append. A gap here would be a
            // gap in the log that stalls every consumer of the partition, which
            // is why offsets are assigned at commit and never pre-allocated.
            throw new IllegalArgumentException(
                    "offset range does not match recordCount " + recordCount);
        }
    }
}
