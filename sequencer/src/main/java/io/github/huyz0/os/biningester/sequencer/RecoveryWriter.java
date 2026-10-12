// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a takeover's decided streams as ONE recovery entry (ADR-0081 §5 step
 * 6, invariant c; M13.33g): their committed entries uploaded as segments at
 * the offsets they were assigned, and every void beside them, in one chain
 * append -- a crash leaves all of it or none.
 *
 * <p>⚠️ A SEGMENT RUN HAS ONE FIRST OFFSET, so a stream's contiguous commits
 * merge into one run, and a stream committed around a void takes a further
 * segment per run: segment {@code i} carries each stream's {@code i}-th run.
 *
 * <p>⚠️ NOTHING BELOW A STREAM's NEXT OFFSET IS WRITTEN: a commit or void
 * there would void, or re-commit, an offset the chain already holds. Checked
 * before anything is uploaded.
 */
public final class RecoveryWriter {

    /** Uploads one segment of the given runs; returns its key. */
    @FunctionalInterface
    public interface SegmentUpload {
        String upload(Map<RunKey, List<SegmentRecord>> runs) throws IOException;
    }

    /** One contiguous run of a stream: its first offset and its records. */
    private record Run(RunKey stream, long first, List<SegmentRecord> records) {
    }

    private final CommitLog log;
    private final SegmentUpload upload;

    public RecoveryWriter(CommitLog log, SegmentUpload upload) {
        this.log = Objects.requireNonNull(log, "log");
        this.upload = Objects.requireNonNull(upload, "upload");
    }

    /**
     * Writes {@code decided}, each stream's decision.
     *
     * @throws IllegalStateException a commit or void below its stream's next
     *     offset, or the slot taken by another entry
     */
    public Recovery write(List<StreamDecision.Decided> decided) throws IOException {
        Objects.requireNonNull(decided, "decided");
        List<List<Run>> byStream = new ArrayList<>();
        List<Recovery.VoidRange> voids = new ArrayList<>();
        for (StreamDecision.Decided d : decided) {
            List<Run> runs = runs(d.commits());
            byStream.add(runs);
            for (Run run : runs) {
                requireFresh(run.stream(), run.first());
            }
            for (Recovery.VoidRange v : d.voids()) {
                requireFresh(v.key(), v.fromOffset());
                voids.add(v);
            }
        }
        List<SegmentCommit> segments = new ArrayList<>();
        for (int layer = 0; ; layer++) {
            Map<RunKey, List<SegmentRecord>> records = new LinkedHashMap<>();
            List<RunCommit> runs = new ArrayList<>();
            for (List<Run> stream : byStream) {
                if (layer < stream.size()) {
                    Run run = stream.get(layer);
                    records.put(run.stream(), run.records());
                    runs.add(new RunCommit(run.stream(), run.records().size(), run.first()));
                }
            }
            if (runs.isEmpty()) {
                break;
            }
            runs.sort(Comparator.comparing(RunCommit::key));
            segments.add(new SegmentCommit(upload.upload(records), runs));
        }
        voids.sort(Comparator.comparing(Recovery.VoidRange::key)
                .thenComparingLong(Recovery.VoidRange::fromOffset));
        return log.appendRecovery(new Recovery(log.nextSequence(), segments, voids));
    }

    /** One stream's commits, contiguous ones merged into one run. */
    private static List<Run> runs(List<FastJournalRecord.Entry> commits) {
        List<Run> runs = new ArrayList<>();
        for (FastJournalRecord.Entry e : commits) {
            Run last = runs.isEmpty() ? null : runs.get(runs.size() - 1);
            if (last != null && last.first() + last.records().size() == e.firstOffset()) {
                List<SegmentRecord> merged = new ArrayList<>(last.records());
                merged.addAll(e.records());
                runs.set(runs.size() - 1, new Run(last.stream(), last.first(), merged));
            } else {
                runs.add(new Run(e.key(), e.firstOffset(), e.records()));
            }
        }
        return runs;
    }

    private void requireFresh(RunKey stream, long from) {
        if (from < log.nextOffset(stream)) {
            throw new IllegalStateException("offset " + from + " of " + stream
                    + " is below the chain's next, " + log.nextOffset(stream));
        }
    }
}
