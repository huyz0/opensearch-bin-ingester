// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.util.Objects;

/**
 * One stream's share of a commit: where its records landed in the log.
 *
 * <p>⚠️ THE OFFSET LIVES HERE, NOT IN THE SEGMENT (ADR-0001). Segments carry no
 * absolute offsets, so any pod can write any partition's bytes at any time with
 * zero coordination and ordering is decided afterwards, at commit. Pre-assigning
 * an offset range before the PUT would create holes that stall every consumer of
 * the partition when a write is abandoned.
 *
 * @param key which stream
 * @param recordCount how many records this segment holds for it
 * @param firstOffset the offset assigned to the first of them
 */
public record RunCommit(RunKey key, int recordCount, long firstOffset) {

    public RunCommit {
        Objects.requireNonNull(key, "key");
        if (recordCount <= 0) {
            throw new IllegalArgumentException("a run with no records is not committed");
        }
        if (firstOffset < 0) {
            throw new IllegalArgumentException("offsets are never negative: " + firstOffset);
        }
    }

    /** The offset of the last record, inclusive. */
    public long lastOffset() {
        return firstOffset + recordCount - 1;
    }
}
