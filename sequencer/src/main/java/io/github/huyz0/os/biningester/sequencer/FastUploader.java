// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The leader's upload (M13.31a; ADR-0081 §7): each stream's EXPOSED entries
 * of this term, contiguous from the chain's next offset, written as ONE
 * segment and ONE delta at the offsets already assigned; then each stream
 * committed at the leader, the leader's own copies released, and every
 * holder told to release theirs.
 *
 * <p>⚠️ NEVER ABOVE THE QUORUM FRONTIER (M13.22b review round 3, P1): a
 * quorum-complete entry above an incomplete one is not uploaded, or the chain
 * would hold a hole below its next offset.
 *
 * <p>⚠️ WHAT THE CHAIN ALREADY HOLDS IS RELEASED, uploaded or not (its review
 * round 1, P2): a delta that landed with its answer lost, or the chain moving
 * on otherwise, would leave entries below its next offset held for ever. An
 * entry beyond a gap waits: committing it would leave the gap a hole.
 *
 * <p>⚠️ THE CHAIN SAYS WHAT IT COMMITTED (round 1, P1): a retry whose delta
 * folds into one that landed releases only what that one holds; the rest is
 * uploaded next time.
 *
 * <p>⚠️ THE CALLER PACES IT at {@code min_upload_interval} (§7): it makes
 * no request when nothing is exposed and uncommitted.
 */
public final class FastUploader {

    /** The chain the upload commits to. */
    public interface Chain {
        /** {@code stream}'s committed next offset. */
        long next(RunKey stream);

        /**
         * One delta of {@code segmentKey}'s runs, each at its first offset, as
         * assigned.
         *
         * @return each stream's committed next offset after it
         */
        Map<RunKey, Long> commitAt(String segmentKey, Map<RunKey, Long> firsts,
                Map<RunKey, Integer> counts) throws IOException;
    }

    /** A leader's last upload as it leaves, unpaced (M13.35c; ADR-0081 §9). */
    @FunctionalInterface
    public interface Last {
        void upload() throws IOException;
    }

    /** Tells every holder of this term to release below each stream's committed offset. */
    @FunctionalInterface
    public interface Release {
        void release(Map<RunKey, Long> below);
    }

    private final long epoch;
    private final FastJournal journal;
    private final FastWriteLeader leader;
    private final Chain chain;
    private final RecoveryWriter.SegmentUpload upload;
    private final Release release;

    public FastUploader(long epoch, FastJournal journal, FastWriteLeader leader, Chain chain,
            RecoveryWriter.SegmentUpload upload, Release release) {
        this.epoch = epoch;
        this.journal = Objects.requireNonNull(journal, "journal");
        this.leader = Objects.requireNonNull(leader, "leader");
        this.chain = Objects.requireNonNull(chain, "chain");
        this.upload = Objects.requireNonNull(upload, "upload");
        this.release = Objects.requireNonNull(release, "release");
    }

    /**
     * Uploads what is exposed and uncommitted, and releases what the chain
     * holds.
     *
     * @return each stream committed and its committed next offset now; empty
     *     when there was nothing to upload or release, and no request was made
     */
    public Optional<Map<RunKey, Long>> upload() throws IOException {
        Map<RunKey, List<FastJournalRecord.Entry>> byStream = new TreeMap<>();
        for (FastJournalRecord.Entry e : journal.held()) {
            if (e.epoch() == epoch) {
                byStream.computeIfAbsent(e.key(), k -> new ArrayList<>()).add(e);
            }
        }
        Map<RunKey, List<SegmentRecord>> runs = new TreeMap<>();
        Map<RunKey, Long> firsts = new TreeMap<>();
        Map<RunKey, Integer> counts = new TreeMap<>();
        Map<RunKey, Long> committed = new TreeMap<>();
        for (Map.Entry<RunKey, List<FastJournalRecord.Entry>> s : byStream.entrySet()) {
            RunKey stream = s.getKey();
            long next = chain.next(stream);
            long frontier = leader.frontier(stream);
            List<FastJournalRecord.Entry> entries = s.getValue();
            entries.sort(Comparator.comparingLong(FastJournalRecord.Entry::firstOffset));
            long at = next;
            List<SegmentRecord> run = new ArrayList<>();
            for (FastJournalRecord.Entry e : entries) {
                long end = e.firstOffset() + e.records().size();
                if (e.firstOffset() < at) {
                    if (end <= next) {
                        committed.put(stream, next);
                    }
                    continue;
                }
                if (e.firstOffset() != at || end > frontier) {
                    break;
                }
                run.addAll(e.records());
                at = end;
            }
            if (!run.isEmpty()) {
                runs.put(stream, run);
                firsts.put(stream, next);
                counts.put(stream, run.size());
            }
        }
        if (!runs.isEmpty()) {
            String key = upload.upload(runs);
            Map<RunKey, Long> after = chain.commitAt(key, firsts, counts);
            for (RunKey stream : runs.keySet()) {
                Long next = after.get(stream);
                if (next == null) {
                    throw new IOException("the chain committed no run of " + stream);
                }
                committed.put(stream, next);
            }
        }
        if (committed.isEmpty()) {
            return Optional.empty();
        }
        for (Map.Entry<RunKey, Long> c : committed.entrySet()) {
            leader.committed(c.getKey(), c.getValue());
            journal.release(c.getKey(), c.getValue());
        }
        release.release(committed);
        return Optional.of(committed);
    }
}
