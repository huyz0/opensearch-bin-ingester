// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToLongFunction;

/**
 * An elected term's fast-mode side at its leader (M13.27j): what the term
 * start learned, and the JOINs it answers. The write (M13.27m) and the
 * departure answers (M13.27k) land beside these.
 *
 * <p>⚠️ A JOIN IS ANSWERED ONLY ONCE THE ROSTER LISTS THE POD (ADR-0081 §1):
 * the incarnations that can hold a term's copies are exactly its members, so
 * an answer before the write would let a pod hold an entry no quorum counts.
 *
 * <p>⚠️ JOINED CARRIES {@code closedThrough} AND THE STATUS OF EVERY GROUP
 * THE POD REPORTED (ADR-0081 §5 step 7): a holder that missed a RELEASE learns
 * from it which entries to drop, judged against this term's roster and every
 * earlier one still open -- their decisions can supersede a group.
 */
public final class FastLeaderTerm {

    private final FastTermOpening.Opened opened;
    private final JoinDesk joins;
    private final ToLongFunction<RunKey> committedNext;

    /**
     * @param committedNext a stream's committed next offset, from the chain
     */
    public FastLeaderTerm(FastTermOpening.Opened opened, JoinDesk joins,
            ToLongFunction<RunKey> committedNext) {
        this.opened = Objects.requireNonNull(opened, "opened");
        this.joins = Objects.requireNonNull(joins, "joins");
        this.committedNext = Objects.requireNonNull(committedNext, "committedNext");
    }

    public long epoch() {
        return opened.started().own().epoch();
    }

    /**
     * The answer to {@code join}, sent under {@code header}.
     *
     * @throws IOException the roster write failed; the join stays pending and
     *     the pod retries
     */
    public FastFrame.Body answerJoin(FastFrame.Header header, FastFrame.Join join)
            throws IOException {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(join, "join");
        if (header.epoch() != epoch()) {
            return refused(FastFrame.Reason.NOT_ROSTERED, "this pod leads term " + epoch()
                    + ", not " + header.epoch());
        }
        return switch (joins.join(join.incarnation())) {
            case JoinDesk.Rostered rostered -> {
                List<Roster> rosters = new ArrayList<>();
                rosters.add(rostered.roster());
                rosters.addAll(opened.stillOpen());
                Map<RunKey, Long> next = new HashMap<>();
                for (FastFrame.HeldStream s : join.held().streams()) {
                    next.put(s.stream(), committedNext.applyAsLong(s.stream()));
                }
                yield new FastFrame.Joined(opened.closedThrough(), HeldStatusAnswers.answer(
                        join.held(), next, rosters, opened.closedThrough()));
            }
            case JoinDesk.Departed departed -> refused(FastFrame.Reason.DEPARTING,
                    join.incarnation().podUid() + " departed term " + epoch());
            case JoinDesk.Deposed deposed -> refused(FastFrame.Reason.LOWER_EPOCH,
                    "term " + epoch() + " was fenced by " + deposed.newer());
        };
    }

    private static FastFrame.Refused refused(FastFrame.Reason reason, String text) {
        return new FastFrame.Refused(reason, Optional.empty(), text);
    }
}
