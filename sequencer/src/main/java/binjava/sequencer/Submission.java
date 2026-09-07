// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.util.Map;

/**
 * One segment's worth of a commit — what a single flush contributes.
 *
 * <p>⚠️ WHY THIS EXISTS AT ALL, stated correctly: {@link CommitLog#commit(String, Map)} has no {@code podId} and no {@code flushSeq} to offer, so it cannot
 * build a {@link CommitRequest} without inventing them — and an invented
 * pod id in the log's own primitive is exactly the kind of placeholder
 * M4.10's idempotency would later key on by accident.
 *
 * <p>⚠️ TWO EARLIER VERSIONS OF THIS COMMENT WERE FALSE: the log is not
 * ignorant of {@code CommitRequest} ({@code commitAll} names it), and it
 * does now READ {@code podId}, {@code incarnationId} and {@code flushSeq}
 * — to COPY them onto the delta, never to decide anything, which is the
 * boundary this record still makes visible. The attribution is null for
 * {@link CommitLog#commit(String, Map)}, which has no request behind it.
 */
record Submission(String segmentKey, Map<RunKey, Integer> recordCounts,
        SegmentCommit.Attribution attribution) {
    Submission(String segmentKey, Map<RunKey, Integer> recordCounts) {
        this(segmentKey, recordCounts, null);
    }
}
