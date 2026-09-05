// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.util.List;
import java.util.Objects;

/**
 * One segment's contribution to a delta: the object that became part of the
 * log, and the offsets its streams gained (ADR-0032).
 *
 * <p>⚠️ THIS TYPE EXISTS SO THE PAIRING CANNOT BE LOST. A delta batches every
 * pod flush in a {@code commitBatchInterval} window, so it names many segments,
 * and a run's offsets are meaningless without knowing which object holds the
 * records — a consumer handed the wrong pairing fetches the wrong bytes, which
 * is a silent data error rather than a crash. Grouping the runs *under* their
 * segment makes that mistake unrepresentable; a flat list of runs plus a
 * separate list of keys would not.
 *
 * <p>⚠️ NOT a segment key on each {@link RunCommit}, which was the other way to
 * carry this and is rejected in ADR-0032: keys run to ~80 bytes, a saturated
 * window across six pods and a thousand streams would repeat one of six keys
 * across thousands of runs, and nothing in that shape would stop two runs of the
 * same segment naming different keys.
 */
public record SegmentCommit(String segmentKey, List<RunCommit> runs) {

    public SegmentCommit {
        Objects.requireNonNull(segmentKey, "segmentKey");
        if (segmentKey.isEmpty()) {
            throw new IllegalArgumentException("a segment commit names a segment");
        }
        runs = List.copyOf(Objects.requireNonNull(runs, "runs"));
        if (runs.isEmpty()) {
            // ⚠️ Same reasoning as an empty delta: a segment that commits no
            // runs would occupy bytes in the chain and say nothing, and a
            // replay meeting one could not tell it from a truncation.
            throw new IllegalArgumentException("a segment commit with no runs commits nothing");
        }
    }
}
