// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Completes append promises only from the matching durable segment in the commit delta. */
final class DurableSegmentAcknowledgement {
    private static final System.Logger LOG = System.getLogger(
            DurableSegmentAcknowledgement.class.getName());

    private DurableSegmentAcknowledgement() {
    }

    static void complete(SegmentPublisher.Published published, CommitDelta delta,
            List<DefaultIngest.Pending> batch, DurableSegmentListener listener) {
        // Pair runs with their segment: a forwarded delta can contain another pod's
        // runs for the same stream, and accepting those offsets would acknowledge
        // records this pod did not commit.
        Map<RunKey, Long> firstOffsets = new HashMap<>();
        for (SegmentCommit segment : delta.segments()) {
            if (!segment.segmentKey().equals(published.key())) {
                continue;
            }
            for (RunCommit run : segment.runs()) {
                firstOffsets.put(run.key(), run.firstOffset());
            }
        }
        for (DefaultIngest.Pending pending : batch) {
            Long base = firstOffsets.get(pending.stream());
            if (base == null) {
                pending.done().completeExceptionally(new IOException(
                        "the commit carried no run for the stream this append wrote, "
                        + "under segment " + published.key() + " -- the delta names "
                        + delta.segments().size() + " segment(s)"));
                continue;
            }
            // Each caller receives its own slice of a batched stream run.
            long first = base + pending.offsetWithinRun();
            pending.done().complete(new AppendResult(pending.count(), first,
                    first + pending.count() - 1));
        }
        if (!firstOffsets.isEmpty()) {
            try {
                listener.onDurable(published.key());
            } catch (RuntimeException unavailable) {
                // A missed cache warm is still served by the authoritative object store.
                LOG.log(System.Logger.Level.DEBUG,
                        "durable segment hint failed for " + published.key(), unavailable);
            }
        }
    }
}
