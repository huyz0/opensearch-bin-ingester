// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import binjava.format.SegmentKey;
import java.io.IOException;
import java.lang.System.Logger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deletes expired segments, driven from the commit log (M7.6, FR-9, cost rules
 * R2 and R8; research 06 §4).
 *
 * <p>⚠️ EVERYTHING IT NEEDS IS ALREADY IN THE CHAIN, which is what makes GC
 * essentially free rather than proportional to what it collects:
 * {@code CommitDelta → SegmentCommit(segmentKey, runs) → RunCommit(key,
 * recordCount, firstOffset)} gives a segment's last offset per stream by
 * arithmetic, and {@link SegmentKey#timestampOf} gives its age from the key.
 * So this class issues <b>no LIST</b> (a LIST costs what a PUT costs), <b>no
 * GET</b> and <b>no stat</b> — only DELETEs, which are free and batch 1,000
 * keys per call.
 *
 * <p>⚠️ THE RUNS MERGE ACROSS DELTAS BEFORE ANYTHING IS JUDGED. One object can
 * be named by more than one delta, and judging each mention separately deletes
 * on the first one that says yes — with the other streams' unread records
 * inside it.
 *
 * <p>⚠️ NOTHING TO COLLECT COSTS NOTHING. A pass over a chain with no expired
 * segment issues zero requests of any kind, because an idle cluster runs this
 * on an interval forever (NFR-2 through the other door).
 *
 * <p>⚠️ IT DOES NOT DECIDE, IT ACTS. {@link RetentionRule} is the predicate and
 * owns every keep-or-delete argument; this class turns its verdicts into
 * batched DELETEs and counts what happened.
 */
public final class SegmentGc {

    private static final Logger LOG = java.lang.System.getLogger(SegmentGc.class.getName());

    /**
     * What one pass did.
     *
     * <p>⚠️ {@code ceilingDeleted} IS SEPARATE FROM {@code deleted}, AND THAT IS
     * NOT BOOKKEEPING. A ceiling delete is data a consumer had not read; folding
     * the two together reports a healthy-looking number for an incident.
     */
    public record Result(int deleted, int kept, int ceilingDeleted, int unreadable,
            List<String> deletedKeys) {

        public Result {
            deletedKeys = List.copyOf(Objects.requireNonNull(deletedKeys, "deletedKeys"));
        }
    }

    /**
     * ⚠️ **1,000 KEYS PER DELETE, NAMED IN THE TREE RATHER THAN IN EACH
     * CALLER'S HEAD** (M7.26). {@code DeleteObjects} takes 1,000 keys, and the
     * budget M7 asserted -- one DELETE per 1,000 keys -- was a TEST ARGUMENT:
     * every cost case passed {@code 1000} from its own body and no production
     * call site existed. ⚠️ A WIRING THAT PASSED 1 IS A 1,000x DELETE-COST
     * REGRESSION THAT COMPILES AND PASSES EVERY TEST, which is why the
     * retention loop takes this constant and a case asserts it did.
     */
    public static final int DEFAULT_DELETE_BATCH = 1000;

    private final BinStore store;
    private final RetentionRule rule;
    private final int deleteBatchSize;

    /**
     * @param deleteBatchSize keys per DELETE call — 1,000 is what
     *     {@code DeleteObjects} takes, and the budget this milestone asserts is
     *     one call per 1,000 keys
     */
    public SegmentGc(BinStore store, RetentionRule rule, int deleteBatchSize) {
        this.store = Objects.requireNonNull(store, "store");
        this.rule = Objects.requireNonNull(rule, "rule");
        if (deleteBatchSize <= 0) {
            throw new IllegalArgumentException(
                    "deleteBatchSize is never " + deleteBatchSize);
        }
        this.deleteBatchSize = deleteBatchSize;
    }

    /** One pass over the chain a caller has already read. */
    public Result collect(List<CommitDelta> chain) {
        Objects.requireNonNull(chain, "chain");
        Map<String, Map<RunKey, Long>> endOffsets = new LinkedHashMap<>();
        for (CommitDelta delta : chain) {
            for (SegmentCommit segment : delta.segments()) {
                Map<RunKey, Long> ends = endOffsets.computeIfAbsent(segment.segmentKey(),
                        k -> new LinkedHashMap<>());
                for (RunCommit run : segment.runs()) {
                    // ⚠️ INCLUSIVE, AND THE MINUS ONE IS LOAD-BEARING: the
                    // segment's LAST offset for this stream, against a
                    // consumer position that is EXCLUSIVE (ADR-0049). One
                    // record of silent loss per segment if it is wrong.
                    long end = run.firstOffset() + run.recordCount() - 1;
                    ends.merge(run.key(), end, Math::max);
                }
            }
        }

        List<String> doomed = new ArrayList<>();
        java.util.Set<String> atCeiling = new java.util.HashSet<>();
        int kept = 0;
        int unreadable = 0;
        for (Map.Entry<String, Map<RunKey, Long>> segment : endOffsets.entrySet()) {
            Instant writtenAt;
            try {
                writtenAt = Instant.ofEpochMilli(SegmentKey.timestampOf(segment.getKey()));
            } catch (IllegalArgumentException notAKey) {
                // ⚠️ KEPT, AND COUNTED. A key whose age cannot be read is a key
                // whose retention cannot be judged; guessing "now" makes it
                // immortal and guessing zero deletes it on this pass.
                unreadable++;
                LOG.log(Logger.Level.WARNING, () -> "a committed segment key cannot be dated, "
                        + "so its retention cannot be judged and it is kept: "
                        + segment.getKey());
                continue;
            }
            RetentionRule.Verdict verdict = rule.verdictFor(
                    new RetentionRule.SegmentFacts(segment.getKey(), writtenAt,
                            segment.getValue()));
            if (verdict.delete()) {
                doomed.add(segment.getKey());
                if (verdict.ceiling()) {
                    atCeiling.add(segment.getKey());
                }
            } else {
                kept++;
            }
        }

        int deleted = 0;
        int ceiling = 0;
        // ⚠️ THE KEYS, NOT JUST THE COUNT. What was DELETED is what moves
        // `oldestRetainedOffset` (M7.10), and a Result carrying only a number
        // leaves that boundary at 0 forever -- so the refusal a consumer below
        // it is owed has no boundary to be below.
        List<String> gone = new ArrayList<>();
        for (int from = 0; from < doomed.size(); from += deleteBatchSize) {
            List<String> batch = doomed.subList(from,
                    Math.min(from + deleteBatchSize, doomed.size()));
            try {
                store.delete(batch);
                deleted += batch.size();
                gone.addAll(batch);
                // ⚠️ COUNTED AFTER THE DELETE SUCCEEDS, NOT WHEN THE VERDICT IS
                // TAKEN. A ceiling count is an incident number -- data a
                // consumer had not read is now gone -- and a failed batch
                // leaves the object in the bucket, so counting the verdict
                // would report the loss of something still there.
                ceiling += (int) batch.stream().filter(atCeiling::contains).count();
            } catch (IOException failed) {
                // ⚠️ A FAILED BATCH IS NOT COUNTED AS DELETED, and the rest of
                // the pass continues: the objects are still there and the next
                // pass judges them again from the same chain, which is why GC
                // needs no memory of its own.
                LOG.log(Logger.Level.WARNING, () -> "a batch of " + batch.size()
                        + " expired segments was not deleted; the next pass judges them "
                        + "again: " + failed);
            }
        }
        return new Result(deleted, kept, ceiling, unreadable, gone);
    }
}
