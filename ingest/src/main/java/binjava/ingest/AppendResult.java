// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * What a producer learns once its records are durable.
 *
 * <p>⚠️ Returned only AFTER the segment and its commit delta are both in the
 * store (FR-4) -- or, when no sequencer is reachable, its commit INTENT
 * ({@link #deferred(int)}, ADR-0058). A result that meant "buffered" would be a 202 that loses data on
 * a crash, and the M1 test plan's T4 exists to refuse exactly that.
 *
 * @param recordCount how many records were accepted
 * @param firstOffset the offset assigned to the first of them
 * @param lastOffset the offset assigned to the last; equal to firstOffset for a
 *     single record
 */
public record AppendResult(int recordCount, long firstOffset, long lastOffset,
        boolean deferred) {

    /** What {@link #firstOffset} and {@link #lastOffset} are before any is assigned. */
    public static final long UNASSIGNED = -1;

    /** A committed append: offsets assigned. */
    public AppendResult(int recordCount, long firstOffset, long lastOffset) {
        this(recordCount, firstOffset, lastOffset, false);
    }

    /**
     * An append whose segment AND commit INTENT are durable, and whose offsets
     * the leaseholder assigns when it drains the inbox (M8.14a, ADR-0058).
     *
     * <p>⚠️ **NO OFFSET IS INVENTED**: both are {@link #UNASSIGNED}, so a caller
     * that reads them as a range is refused by the type rather than handed a
     * number that means nothing.
     */
    public static AppendResult deferred(int recordCount) {
        return new AppendResult(recordCount, UNASSIGNED, UNASSIGNED, true);
    }

    public AppendResult {
        if (recordCount <= 0) {
            throw new IllegalArgumentException("an append of nothing has no result");
        }
        if (deferred && (firstOffset != UNASSIGNED || lastOffset != UNASSIGNED)) {
            throw new IllegalArgumentException("a deferred append has no offsets yet");
        }
        if (!deferred && (firstOffset < 0 || lastOffset < firstOffset)) {
            throw new IllegalArgumentException(
                    "offsets [" + firstOffset + "," + lastOffset + "] are not a range");
        }
        if (!deferred && lastOffset - firstOffset != recordCount - 1) {
            // ⚠️ Offsets are CONTIGUOUS within one append. A gap here would be a
            // gap in the log that stalls every consumer of the partition, which
            // is why offsets are assigned at commit and never pre-allocated.
            throw new IllegalArgumentException(
                    "offset range does not match recordCount " + recordCount);
        }
    }
}
