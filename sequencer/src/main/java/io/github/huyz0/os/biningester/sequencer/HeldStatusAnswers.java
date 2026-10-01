// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The leader's answer to a HELD report (ADR-0081 §5.7, §9; ADR-0082 §2's
 * HELD_STATUS; M13.26h): per reported stream its committed next offset, and
 * per reported {@code (epoch, assignedAfter)} group the offset from which a
 * decision supersedes it and the status of the part below.
 *
 * <p>⚠️ SUPERSEDED BY ADR-0081 §5 INVARIANT (b): an entry of epoch {@code e},
 * assigned after {@code d} decisions of its roster, is superseded at offsets
 * at or above a decision for its stream recorded by a later term's roster, or
 * by its own term's numbered {@code d} or later. So a group a switch's
 * decision splits is answered for both parts.
 *
 * <p>The status, of the part below the superseding offset, first match: a
 * term at or below {@code closedThrough} is {@code CLOSED_TERM}; a group a
 * decision supersedes from its lowest offset {@code SUPERSEDED}; one whose
 * part below the superseding offset is below the committed next offset
 * {@code COMMITTED}; anything else {@code PENDING}.
 */
public final class HeldStatusAnswers {

    private HeldStatusAnswers() {
    }

    /**
     * @param committedNext each stream's committed next offset, from the chain
     * @param rosters every roster whose decisions can supersede a reported
     *     group: this term's and every unclosed earlier one's
     * @param closedThrough the highest closed epoch
     */
    public static FastFrame.HeldStatus answer(FastFrame.Held held,
            Map<RunKey, Long> committedNext, List<Roster> rosters, long closedThrough) {
        Objects.requireNonNull(held, "held");
        Objects.requireNonNull(committedNext, "committedNext");
        Objects.requireNonNull(rosters, "rosters");
        List<FastFrame.StreamStatus> streams = new ArrayList<>();
        for (FastFrame.HeldStream s : held.streams()) {
            long next = committedNext.getOrDefault(s.stream(), 0L);
            List<FastFrame.GroupStatus> groups = new ArrayList<>();
            for (FastFrame.HeldGroup g : s.groups()) {
                long from = supersededFrom(s.stream(), g.epoch(), g.assignedAfter(), rosters);
                FastFrame.Status status;
                // ⚠️ THE STATUS IS OF THE PART BELOW `from` (M13.26f review
                // round 1, P1): a group a decision splits, its lower part
                // committed, is COMMITTED -- answered PENDING, the departing
                // pod would wait for a release that never comes.
                if (g.epoch() <= closedThrough) {
                    status = FastFrame.Status.CLOSED_TERM;
                } else if (from <= g.lowest()) {
                    status = FastFrame.Status.SUPERSEDED;
                } else if (Math.min(g.highest(), from - 1) < next) {
                    status = FastFrame.Status.COMMITTED;
                } else {
                    status = FastFrame.Status.PENDING;
                }
                groups.add(new FastFrame.GroupStatus(g.epoch(), g.assignedAfter(), from, status));
            }
            streams.add(new FastFrame.StreamStatus(s.stream(), next, groups));
        }
        return new FastFrame.HeldStatus(streams);
    }

    /** The lowest resume offset of a decision superseding the group, or {@code Long.MAX_VALUE}. */
    static long supersededFrom(RunKey stream, long epoch, long assignedAfter,
            List<Roster> rosters) {
        long from = Long.MAX_VALUE;
        for (Roster r : rosters) {
            if (r.epoch() < epoch) {
                continue;
            }
            for (Roster.Decision d : r.decisions()) {
                if (d.stream().equals(stream)
                        && (r.epoch() > epoch || d.seq() >= assignedAfter)) {
                    from = Math.min(from, d.resumeAt());
                }
            }
        }
        return from;
    }
}
