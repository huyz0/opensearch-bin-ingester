// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What each stream's OLDEST RETAINED offset is after a GC pass (M7.10, FR-9).
 *
 * <p>⚠️ THE ANSWER IS THE LOWEST RETAINED FIRST OFFSET, NEVER THE HIGHEST
 * DELETED END. The two differ exactly when a kept segment sits between two
 * deleted ones — which happens whenever one stream in a shared segment is
 * behind, so it is the normal case rather than an edge one — and the
 * highest-deleted reading claims records are gone while they are still in the
 * bucket, refusing a consumer for data it could have had.
 *
 * <p>⚠️ A STREAM WITH EVERY SEGMENT DELETED RETAINS FROM ITS {@code nextOffset},
 * because {@code oldestRetainedOffset == nextOffset} is exactly what an empty
 * stream means to {@code Checkpoint.StreamOffsets}.
 *
 * <p>⚠️ AND A STREAM THIS PASS CANNOT ANSWER FOR IS ABSENT RATHER THAN ZERO.
 * Zero says "everything ever written is still here", which is the one answer
 * that is never conservative: it lets a consumer seek into records that have
 * been deleted and meet a 404 instead of the refusal M7.16 owes it.
 */
public final class RetainedOffsets {

    private RetainedOffsets() {
    }

    /**
     * @param chain the deltas the caller replayed — ⚠️ IT MUST NAME EVERY LIVE
     *     SEGMENT OF EVERY STREAM IT REPORTS ON. A truncated chain hides
     *     retained segments, so the boundary comes out too HIGH, and
     *     {@code CheckpointWriter} merges boundaries with {@code max} — which
     *     makes a too-high one permanent, refusing consumers for records that
     *     are still in the bucket
     * @param deletedSegments the segment keys this pass actually deleted — ⚠️
     *     what was DELETED, not what was condemned: a batch that failed leaves
     *     its objects readable, and reporting them gone refuses a consumer that
     *     could still have read them
     * @param nextOffsets each stream's next offset, for the streams whose
     *     segments are all gone
     */
    public static Map<RunKey, Long> after(List<CommitDelta> chain, Set<String> deletedSegments,
            Map<RunKey, Long> nextOffsets) {
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(deletedSegments, "deletedSegments");
        Objects.requireNonNull(nextOffsets, "nextOffsets");
        Map<RunKey, Long> lowestRetained = new LinkedHashMap<>();
        java.util.Set<RunKey> seen = new java.util.LinkedHashSet<>();
        for (CommitDelta delta : chain) {
            for (SegmentCommit segment : delta.segments()) {
                boolean gone = deletedSegments.contains(segment.segmentKey());
                for (RunCommit run : segment.runs()) {
                    seen.add(run.key());
                    if (!gone) {
                        lowestRetained.merge(run.key(), run.firstOffset(), Math::min);
                    }
                }
            }
        }
        Map<RunKey, Long> out = new LinkedHashMap<>();
        for (RunKey stream : seen) {
            Long retained = lowestRetained.get(stream);
            if (retained != null) {
                out.put(stream, retained);
            } else {
                Long next = nextOffsets.get(stream);
                if (next != null) {
                    out.put(stream, next);
                }
            }
        }
        return out;
    }
}
