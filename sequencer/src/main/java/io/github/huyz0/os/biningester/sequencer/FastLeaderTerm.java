// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
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
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * An elected term's fast-mode side at its leader (M13.27j, M13.27k): what the
 * term start learned, the JOINs it answers, and a departing pod's DEPART and
 * HELD; and the COMMITs it assigns (M13.27s), the rest of the write following
 * (M13.27m).
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

    /** ADR-0081's per-stream bound {@code B}: a cursor never more than this past its commit. */
    public static final long STREAM_BOUND = 65_536;

    /** How long a writer waits for the flush another COMMIT's caller runs. */
    static final java.time.Duration ANSWER_WAIT = java.time.Duration.ofSeconds(10);

    private final FastTermOpening.Opened opened;
    private final JoinDesk joins;
    private final ToLongFunction<RunKey> committedNext;
    private final BinStore store;
    private final String prefix;
    private final RosterDepartures departures;
    private final MonotonicClock mono;
    private final long refreshNanos;
    /** This term's roster as last read, and when; null before the first read. */
    private Roster cached;
    /**
     * The same roster, read without the monitor (M13.27s review round 1, P2):
     * the monitor is held across a roster GET, and the write's next decision
     * must never wait on one.
     */
    private volatile Roster latest;
    /** The desk this term's COMMITs reach; null until the first. */
    private CommitDesk desk;
    private long cachedAt;

    /**
     * @param committedNext a stream's committed next offset, from the chain
     * @param refresh how long a read of this term's roster answers HELDs:
     *     {@code min_upload_interval} (M13.27q)
     */
    public FastLeaderTerm(FastTermOpening.Opened opened, JoinDesk joins,
            ToLongFunction<RunKey> committedNext, BinStore store, String prefix,
            MonotonicClock mono, java.time.Duration refresh) {
        this.opened = Objects.requireNonNull(opened, "opened");
        this.joins = Objects.requireNonNull(joins, "joins");
        this.committedNext = Objects.requireNonNull(committedNext, "committedNext");
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.departures = new RosterDepartures(store, prefix, epoch());
        this.mono = Objects.requireNonNull(mono, "mono");
        this.refreshNanos = refresh.toNanos();
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
            return answerHeld(depart.held());
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
        return answerHeld(held.held());
    }

    /**
     * ⚠️ A DEPOSED TERM ANSWERS NO STATUS (M13.27k review round 2, P4): its
     * roster shows the newer fence, and the newer term's decisions are not in
     * what it reads -- a pod told PENDING by it would wait for a RELEASE no
     * deposed leader sends.
     */
    private FastFrame.Body answerHeld(FastFrame.Held held) throws IOException {
        Roster own = ownRoster();
        if (own.fencedBy() > epoch()) {
            return refused(FastFrame.Reason.LOWER_EPOCH, "term " + epoch()
                    + " was fenced by " + own.fencedBy());
        }
        return new FastFrame.HeldStatusReport(status(held, own));
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

    /**
     * ⚠️ READ AT MOST ONCE PER {@code min_upload_interval} (M13.27k review
     * round 2, P3): a holder over half its cap sends a HELD on every refused
     * REPLICA, and a read per HELD would follow the refused writes. A decision
     * or a fence written meanwhile is seen within one interval.
     */
    private synchronized Roster ownRoster() throws IOException {
        long now = mono.nanos();
        if (cached != null && now - cachedAt < refreshNanos) {
            return cached;
        }
        try (InputStream in = store.get(Roster.key(prefix, epoch()))) {
            cached = Roster.decode(in.readAllBytes());
        }
        latest = cached;
        cachedAt = now;
        return cached;
    }

    /**
     * The answer to a COMMIT from {@code header}'s sender, by {@code desk}
     * (M13.27s): only a writer this term's roster lists as ROSTERED is
     * assigned, at its own zone, against the pods the roster lists.
     *
     * @throws IOException the store or the journal failed; the writer retries
     */
    public FastFrame.Body answerCommit(FastFrame.Header header, FastWriteFrame.Commit commit,
            CommitDesk desk) throws IOException {
        Objects.requireNonNull(commit, "commit");
        Objects.requireNonNull(desk, "desk");
        if (header.epoch() != epoch()) {
            return notThisTerm(header);
        }
        Roster own = ownRoster();
        if (own.fencedBy() > epoch()) {
            return refused(FastFrame.Reason.LOWER_EPOCH, "term " + epoch()
                    + " was fenced by " + own.fencedBy());
        }
        Optional<Roster.Member> writer = own.member(header.senderUid());
        if (writer.isEmpty() || writer.get().state() != Roster.State.ROSTERED) {
            return refused(FastFrame.Reason.NOT_ROSTERED, header.senderUid()
                    + " is not rostered in term " + epoch());
        }
        // ⚠️ AVAILABLE = EVERY POD THE ROSTER LISTS, for now: a pod's loss is
        // judged by its UID's liveness, which M13.33 brings. A departed one
        // lends no zone -- FastAdmission counts only the ROSTERED.
        Set<String> available = own.members().stream()
                .map(m -> m.incarnation().podUid()).collect(Collectors.toSet());
        return desk.answer(new QuorumFrontier.Holder(header.senderUid(),
                writer.get().incarnation().az()), commit, own, available);
    }

    /**
     * This term's desk, over {@code journal}, built on the first call
     * (M13.27s).
     *
     * <p>⚠️ ITS TERM RECORD IS WHAT THE TERM STARTED WITH, empty until M13.27l
     * feeds it from the catalog: until then every COMMIT is held.
     */
    public synchronized CommitDesk commitDesk(FastJournal journal, Roster.Incarnation self) {
        if (desk == null) {
            Roster started = opened.started().own();
            desk = new CommitDesk(new FastWriteLeader(epoch(), self,
                    new FastCursor(STREAM_BOUND),
                    new TermRecordWriter(store, prefix, started, mono,
                            java.time.Duration.ofNanos(refreshNanos)),
                    new QuorumFrontier(self.podUid(), self.az()), journal,
                    this::nextDecision), committedNext, ANSWER_WAIT);
        }
        return desk;
    }

    /**
     * The {@code seq} this term's next decision takes: one past the highest
     * its roster as last read holds, never the list's size -- a pruned list
     * is shorter than its numbering (ADR-0081 §2).
     */
    long nextDecision() {
        Roster read = latest;
        Roster r = read != null ? read : opened.started().own();
        List<Roster.Decision> decisions = r.decisions();
        return decisions.isEmpty() ? 0 : decisions.get(decisions.size() - 1).seq() + 1L;
    }

    private FastFrame.Refused notThisTerm(FastFrame.Header header) {
        return refused(FastFrame.Reason.NOT_ROSTERED, "this pod leads term " + epoch()
                + ", not " + header.epoch());
    }

    private static FastFrame.Refused refused(FastFrame.Reason reason, String text) {
        return new FastFrame.Refused(reason, Optional.empty(), text);
    }
}
