// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One stream's takeover decision (ADR-0081 §5 step 5; M13.33f), from its
 * live entries at offsets {@code >= c0}, {@code c0} its committed next offset
 * when the takeover started.
 *
 * <p>⚠️ WITHOUT QUORUM LOSS: committed from {@code c0} up to the first hole,
 * resumed at the hole; the entries above it are discarded -- by exposure in
 * order none was exposed, and the decision supersedes them on every pod. An
 * unanswered member is waited for unless zone coverage allows deciding early.
 *
 * <p>⚠️ WITH QUORUM LOSS: every member must have answered or be gone -- a
 * void must not cover a copy on a pod that survives -- then every live entry
 * is committed at its offset and every hole up to {@code c0 + B} voided, so
 * no committed offset is ever voided; resumed at {@code c0 + B}. An entry
 * above a hole may be exposed and survive, so it is committed, never
 * discarded.
 *
 * <p>⚠️ COPIES OF ONE ENTRY COUNT ONCE; two different entries at one offset
 * cannot occur (a reassignment follows a decision superseding the first) and
 * are refused rather than one guessed.
 */
public final class StreamDecision {

    /** How a stream's decision stands. */
    public sealed interface Outcome permits Wait, Decided {
    }

    /** Not yet: a member it needs has not answered. */
    public record Wait() implements Outcome {
    }

    /** Decided: what to commit, what to void, and where assignment resumes. */
    public record Decided(List<FastJournalRecord.Entry> commits,
            List<Recovery.VoidRange> voids, long resumeAt) implements Outcome {
        public Decided {
            commits = List.copyOf(commits);
            voids = List.copyOf(voids);
        }
    }

    private StreamDecision() {
    }

    /**
     * @param live the stream's live entries, collected and the successor's own
     * @param lost whether its index has lost quorum ({@link QuorumLoss#lost})
     * @param allAnswered whether every member of every unclosed term answered or is gone
     * @param mayDecideEarly whether zone coverage lets it decide unanswered
     *     ({@link QuorumLoss#mayDecideEarly})
     */
    public static Outcome decide(RunKey stream, long c0, long bound,
            List<FastJournalRecord.Entry> live, boolean lost, boolean allAnswered,
            boolean mayDecideEarly) {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(live, "live");
        if (c0 < 0 || bound < 1) {
            throw new IllegalArgumentException("c0 is never negative and B at least one");
        }
        if (lost ? !allAnswered : !allAnswered && !mayDecideEarly) {
            return new Wait();
        }
        TreeMap<Long, FastJournalRecord.Entry> byOffset = new TreeMap<>();
        for (FastJournalRecord.Entry e : live) {
            if (!e.key().equals(stream)) {
                throw new IllegalArgumentException("an entry of " + e.key() + ", not " + stream);
            }
            if (e.firstOffset() < c0) {
                continue;
            }
            FastJournalRecord.Entry held = byOffset.putIfAbsent(e.firstOffset(), e);
            if (held != null && (held.epoch() != e.epoch()
                    || held.assignedAfter() != e.assignedAfter()
                    || !held.idempotencyKey().equals(e.idempotencyKey())
                    || held.records().size() != e.records().size())) {
                throw new IllegalStateException("two entries at offset " + e.firstOffset()
                        + " of " + stream + ": terms " + held.epoch() + " and " + e.epoch());
            }
        }
        // ⚠️ NO OVERLAP AND NOTHING PAST c0 + B (its review round 1, P1-P2):
        // a run is assigned after its predecessor ends and never past the
        // bound, so either is a state the protocol cannot produce -- and,
        // taken, it would void a committed offset or void past the bound.
        long limit = c0 + bound;
        long end = c0;
        for (FastJournalRecord.Entry e : byOffset.values()) {
            if (e.firstOffset() < end) {
                throw new IllegalStateException("an entry at " + e.firstOffset() + " of "
                        + stream + " overlaps the one ending at " + end);
            }
            end = e.firstOffset() + e.records().size();
            if (end > limit) {
                throw new IllegalStateException("an entry of " + stream + " ends at " + end
                        + ", past c0 + B = " + limit);
            }
        }
        List<FastJournalRecord.Entry> commits = new ArrayList<>();
        List<Recovery.VoidRange> voids = new ArrayList<>();
        long next = c0;
        if (!lost) {
            for (FastJournalRecord.Entry e : byOffset.values()) {
                if (e.firstOffset() != next) {
                    break;
                }
                commits.add(e);
                next = e.firstOffset() + e.records().size();
            }
            return new Decided(commits, voids, next);
        }
        for (FastJournalRecord.Entry e : byOffset.values()) {
            if (e.firstOffset() > next) {
                voids.add(new Recovery.VoidRange(stream, next, e.firstOffset()));
            }
            commits.add(e);
            next = e.firstOffset() + e.records().size();
        }
        if (next < limit) {
            voids.add(new Recovery.VoidRange(stream, next, limit));
            next = limit;
        }
        return new Decided(commits, voids, next);
    }
}
