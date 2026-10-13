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

    /** A redirected run's append, re-sent through the fast path (M13.34l). */
    @FunctionalInterface
    interface Redirect {
        void resend(DefaultIngest.Pending pending);
    }

    /**
     * ⚠️ EACH STREAM ACKED FROM WHICHEVER DELTA COMMITTED IT (M13.34l; ADR-0081
     * §8): a paused run lands in a later delta than the rest of its segment,
     * and the answer lists both; a redirected run's appends are re-sent.
     */
    static void complete(SegmentPublisher.Published published,
            io.github.huyz0.os.biningester.format.CommitAnswer answer,
            List<DefaultIngest.Pending> batch, DurableSegmentListener listener,
            java.util.function.Consumer<Runnable> settle, Redirect redirect) {
        // Pair runs with their segment: a forwarded delta can contain another pod's
        // runs for the same stream, and accepting those offsets would acknowledge
        // records this pod did not commit.
        Map<RunKey, Long> firstOffsets = new HashMap<>();
        for (CommitDelta delta : answer.deltas()) {
            for (SegmentCommit segment : delta.segments()) {
                if (!segment.segmentKey().equals(published.key())) {
                    continue;
                }
                for (RunCommit run : segment.runs()) {
                    firstOffsets.put(run.key(), run.firstOffset());
                }
            }
        }
        for (DefaultIngest.Pending pending : batch) {
            Long base = firstOffsets.get(pending.stream());
            if (base == null && answer.redirected().contains(pending.stream())) {
                redirect.resend(pending);
                continue;
            }
            if (base == null) {
                IOException missing = new IOException(
                        "the commit carried no run for the stream this append wrote, "
                        + "under segment " + published.key() + " -- the answer names "
                        + answer.deltas().size() + " delta(s)");
                settle.accept(() -> pending.done().completeExceptionally(missing));
                continue;
            }
            // Each caller receives its own slice of a batched stream run.
            long first = base + pending.offsetWithinRun();
            AppendResult result = new AppendResult(pending.count(), first,
                    first + pending.count() - 1);
            settle.accept(() -> pending.done().complete(result));
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
