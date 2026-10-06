// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToLongFunction;

/**
 * An elected term's fast-mode side at its leader (M13.27j, M13.27k): what the
 * term start learned, the JOINs it answers, and a departing pod's DEPART and
 * HELD. The write (M13.27m) lands beside these.
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
    private final BinStore store;
    private final String prefix;
    private final RosterDepartures departures;

    /**
     * @param committedNext a stream's committed next offset, from the chain
     */
    public FastLeaderTerm(FastTermOpening.Opened opened, JoinDesk joins,
            ToLongFunction<RunKey> committedNext, BinStore store, String prefix) {
        this.opened = Objects.requireNonNull(opened, "opened");
        this.joins = Objects.requireNonNull(joins, "joins");
        this.committedNext = Objects.requireNonNull(committedNext, "committedNext");
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.departures = new RosterDepartures(store, prefix, epoch());
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
            return notThisTerm(header);
        }
        return switch (joins.join(join.incarnation())) {
            case JoinDesk.Rostered rostered -> new FastFrame.Joined(opened.closedThrough(),
                    status(join.held(), rostered.roster()));
            case JoinDesk.Departed departed -> refused(FastFrame.Reason.DEPARTING,
                    join.incarnation().podUid() + " departed term " + epoch());
            case JoinDesk.Deposed deposed -> refused(FastFrame.Reason.LOWER_EPOCH,
                    "term " + epoch() + " was fenced by " + deposed.newer());
        };
    }

    /**
     * The answer to a departing pod's DEPART (M13.27k): phase 1, the status of
     * every group it reports; phase 2, the pod marked {@code DEPARTED} here and
     * in every earlier open roster listing it, answered by an empty
     * HELD_STATUS once every write landed (ADR-0081 §9).
     *
     * <p>⚠️ PHASE 1 ALSO ASKS FOR AN UPLOAD, which M13.31 brings; until then a
     * pending group stays pending, and the pod waits or stops undeparted.
     *
     * @throws IOException the store failed; the pod retries
     */
    public FastFrame.Body answerDepart(FastFrame.Header header, FastFrame.Depart depart)
            throws IOException {
        Objects.requireNonNull(depart, "depart");
        if (header.epoch() != epoch()) {
            return notThisTerm(header);
        }
        if (depart.phase() == 1) {
            return new FastFrame.HeldStatusReport(status(depart.held(), ownRoster()));
        }
        // ⚠️ ONLY A POD DEPARTS ITSELF (M13.27k review round 1, P2): marked
        // departed on another's word, a pod still holding copies would lose
        // them uncounted -- its loss is no longer quorum loss.
        if (!depart.incarnation().podUid().equals(header.senderUid())) {
            return refused(FastFrame.Reason.NOT_ROSTERED, header.senderUid()
                    + " cannot depart " + depart.incarnation().podUid());
        }
        List<Long> earlier = new ArrayList<>();
        for (Roster r : opened.stillOpen()) {
            earlier.add(r.epoch());
        }
        return switch (departures.depart(depart.incarnation().podUid(), earlier)) {
            case RosterDepartures.Departed departed ->
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
            case RosterDepartures.Deposed deposed -> refused(FastFrame.Reason.LOWER_EPOCH,
                    "term " + epoch() + " was fenced by " + deposed.newer());
        };
    }

    /**
     * The answer to a HELD: the status of every group reported (ADR-0081 §5
     * step 7, §9).
     *
     * @throws IOException the store failed; the pod retries
     */
    public FastFrame.Body answerHeld(FastFrame.Header header, FastFrame.HeldReport held)
            throws IOException {
        Objects.requireNonNull(held, "held");
        if (header.epoch() != epoch()) {
            return notThisTerm(header);
        }
        return new FastFrame.HeldStatusReport(status(held.held(), ownRoster()));
    }

    /**
     * ⚠️ JUDGED ON THIS TERM'S ROSTER AS IT IS NOW and every earlier one still
     * open: a decision written after the term started supersedes as much as
     * one found at its start.
     */
    private FastFrame.HeldStatus status(FastFrame.Held held, Roster own) {
        List<Roster> rosters = new ArrayList<>();
        rosters.add(own);
        rosters.addAll(opened.stillOpen());
        Map<RunKey, Long> next = new HashMap<>();
        for (FastFrame.HeldStream s : held.streams()) {
            next.put(s.stream(), committedNext.applyAsLong(s.stream()));
        }
        return HeldStatusAnswers.answer(held, next, rosters, opened.closedThrough());
    }

    private Roster ownRoster() throws IOException {
        try (InputStream in = store.get(Roster.key(prefix, epoch()))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    private FastFrame.Refused notThisTerm(FastFrame.Header header) {
        return refused(FastFrame.Reason.NOT_ROSTERED, "this pod leads term " + epoch()
                + ", not " + header.epoch());
    }

    private static FastFrame.Refused refused(FastFrame.Reason reason, String text) {
        return new FastFrame.Refused(reason, Optional.empty(), text);
    }
}
