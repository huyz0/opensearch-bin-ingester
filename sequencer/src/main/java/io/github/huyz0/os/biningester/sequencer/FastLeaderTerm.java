// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
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
 * HELD; the COMMITs it assigns (M13.27s) and the CONFIRMs that count their
 * writers' copies (M13.27v), and the EXPOSEDs it sends (M13.27w), the rest
 * of the write following (M13.27m).
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

    /**
     * ADR-0081's replica timeout (M13.27y; its seventh constant, measured by
     * M13.37): how long a holder that has not answered, or refused with
     * backpressure, is unavailable, and how often a waiting REPLICA is
     * re-sent to it -- far below the lease TTL, since it bounds how long one
     * slow holder stalls a stream's acks.
     */
    public static final java.time.Duration REPLICA_TIMEOUT = java.time.Duration.ofSeconds(1);

    /** How long a writer waits for the flush another COMMIT's caller runs. */
    static final java.time.Duration ANSWER_WAIT = java.time.Duration.ofSeconds(10);

    private final FastTermOpening.Opened opened;
    private final java.util.concurrent.atomic.AtomicBoolean takeoverClaimed =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final JoinDesk joins;
    private final ToLongFunction<RunKey> committedNext;
    private final BinStore store;
    private final String prefix;
    private final RosterDepartures departures;
    private final MonotonicClock mono;
    private final long refreshNanos;
    /** This term's record, fed from the catalog and read by its desk (M13.27l). */
    private final TermRecordWriter termRecord;
    /** When this term last uploaded, paced at {@code min_upload_interval} (M13.31a). */
    private long lastUploadNanos;
    private boolean uploaded;
    private long uploadSeq;
    /** The first offsets of the upload whose delta is not known committed; null when none. */
    private Map<RunKey, Long> pendingUpload;
    private final java.util.concurrent.locks.ReentrantLock uploading =
            new java.util.concurrent.locks.ReentrantLock();
    /** This term's roster as last read, and when; null before the first read. */
    private Roster cached;
    /**
     * The same roster, read without the monitor (M13.27s review round 1, P2):
     * the monitor is held across a roster GET, and the write's next decision
     * must never wait on one.
     */
    private volatile Roster latest;
    /** When each unavailable holder last failed to answer, or refused: M13.27y. */
    private final Map<String, Long> unavailableAt =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** The desk this term's COMMITs reach; null until the first. */
    private CommitDesk desk;
    /** Shutting down: a desk built after is stopped from its birth (M13.35a). */
    private boolean leaving;
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
        this.termRecord = new TermRecordWriter(store, prefix, opened.started().own(), mono,
                refresh);
    }

    /**
     * The catalog's fast indices and their {@code wal_quorum} (M13.27l;
     * ADR-0081 §4): each a value this term's record must hold before a batch
     * of its index is assigned under it. A value already recorded wants
     * nothing.
     */
    public void catalogSays(Map<java.util.UUID, Integer> fast) {
        fast.forEach(termRecord::want);
    }

    /**
     * Writes what the catalog changed into this term's record, at most one
     * roster write per {@code min_upload_interval} (cost.md rule 6), then
     * re-reads the roster it wrote.
     */
    public TermRecordWriter.Flush flushTermRecord() throws IOException {
        TermRecordWriter.Flush flushed = termRecord.flush();
        if (flushed instanceof TermRecordWriter.Written) {
            rosterWritten();
        }
        return flushed;
    }

    /**
     * The opened term, once, when it has unclosed terms to take over
     * (M13.33p): the one caller that gets it runs the takeover.
     */
    public Optional<FastTermOpening.Opened> claimTakeover() {
        if (opened.stillOpen().isEmpty() || !takeoverClaimed.compareAndSet(false, true)) {
            return Optional.empty();
        }
        return Optional.of(opened);
    }

    public long epoch() {
        return opened.started().own().epoch();
    }

    /**
     * Uploads what this term's desk has exposed and the chain has not
     * committed (M13.31a; ADR-0081 §7): one segment and one delta at the
     * offsets assigned, then every holder of the term told to release.
     *
     * <p>⚠️ AT MOST ONE UPLOAD PER {@code min_upload_interval}, however often
     * it is asked: four segment PUTs and four deltas a second, the rate
     * scaling with time and leaders, never records (NFR-1, NFR-4).
     *
     * @param chain this term's chain, committing at the offsets assigned
     * @return each stream uploaded and its committed next offset; empty when
     *     none was due or nothing was exposed
     */
    public Optional<Map<RunKey, Long>> upload(CommitDesk desk, FastJournal journal,
            FastUploader.Chain chain, RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport) throws IOException {
        return upload(desk, journal, chain, upload, transport, true);
    }

    /** The same, unpaced for a leader's last (M13.35c); ⚠️ one at a time. */
    public Optional<Map<RunKey, Long>> upload(CommitDesk desk, FastJournal journal,
            FastUploader.Chain chain, RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport, boolean paced) throws IOException {
        boolean idle = desk.leader().heldBatches() == 0;
        synchronized (this) {
            // ⚠️ NOTHING HELD, NOTHING TO UPLOAD (M13.87), unless an upload's
            // commit is not yet known: no roster read on an idle tick.
            if (idle && pendingUpload == null) {
                return Optional.empty();
            }
            long now = mono.nanos();
            if (paced && uploaded && now - lastUploadNanos < refreshNanos) {
                return Optional.empty();
            }
            uploaded = true;
            lastUploadNanos = now;
        }
        uploading.lock();
        try {
            Roster own = ownRoster();
            ReleaseSender release = new ReleaseSender(own.leader().podUid(), epoch(), transport);
            return new FastUploader(epoch(), journal, desk.leader(), chain, upload,
                    below -> release.send(List.of(own), below)).upload();
        } finally {
            uploading.unlock();
        }
    }

    /**
     * Whether {@code podUid} is ROSTERED in this term, as its roster was last
     * read (M13.83a review round 1, P1): who may tell the leader of an index.
     */
    public boolean rostered(String podUid) throws IOException {
        return ownRoster().member(podUid)
                .filter(m -> m.state() == Roster.State.ROSTERED).isPresent();
    }

    /**
     * The sequence of the upload delta at {@code firsts}, for its idempotency
     * key (M13.31a review round 1, P1): ⚠️ THE SAME UNTIL ONE IS COMMITTED, so
     * a retry of a delta that landed with its answer lost is folded into it
     * rather than committing its records twice.
     */
    public synchronized long uploadSeq(Map<RunKey, Long> firsts) {
        if (!firsts.equals(pendingUpload)) {
            pendingUpload = Map.copyOf(firsts);
            uploadSeq++;
        }
        return uploadSeq;
    }

    /** The upload delta at {@code firsts} is committed: the next takes a new sequence. */
    public synchronized void uploadCommitted(Map<RunKey, Long> firsts) {
        if (firsts.equals(pendingUpload)) {
            pendingUpload = null;
        }
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
        JoinDesk.Outcome joined;
        try {
            joined = joins.join(join.incarnation());
        } finally {
            // ⚠️ THE TERM's OWN WRITE IS SEEN BY THE NEXT ANSWER (M13.33m): the
            // cache is dropped -- a failed join's write may have landed too --
            // so a COMMIT from the pod just joined is read against the roster
            // that lists it.
            forget();
        }
        return switch (joined) {
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
        RosterDepartures.Outcome outcome = departures.depart(depart.incarnation().podUid(),
                earlier);
        rosterWritten();
        return switch (outcome) {
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
     * This term wrote its roster -- a departure, a takeover's decisions
     * (M13.33m; M13.27q review round 1, P1): read it again at once, so no
     * answer and no next decision number comes from before the write.
     *
     * @throws IOException the roster could not be read again: ⚠️ the next
     *     decision number is not yet known past the write (its review round
     *     1, P1), so the caller writes again -- a decisions write is
     *     idempotent -- before it numbers anything
     */
    public void rosterWritten() throws IOException {
        forget();
        ownRoster();
    }

    private synchronized void forget() {
        cached = null;
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
        Writer writer = writer(header, commit.key());
        if (writer.refused() != null) {
            return writer.refused();
        }
        // ⚠️ AVAILABLE = EVERY POD THE ROSTER LISTS, for now: a pod's loss is
        // judged by its UID's liveness, which M13.33 brings. A departed one
        // lends no zone -- FastAdmission counts only the ROSTERED.
        Set<String> available = writer.roster().members().stream()
                .map(m -> m.incarnation().podUid()).collect(Collectors.toSet());
        return desk.answer(writer.holder(), commit, writer.roster(), available);
    }

    /**
     * The answer to a writer's CONFIRM, by {@code desk} (M13.27v; ADR-0081
     * §2.3): from the ROSTERED writer under its own key, its copy counts --
     * or, not journaled, is replaced -- answered by an empty HELD_STATUS, the
     * EXPOSED following once the quorum frontier passes the batch.
     *
     * <p>⚠️ CHECKED AS ITS COMMIT WAS: a copy counted on another pod's word
     * would expose a batch on a copy no zone holds.
     *
     * @throws IOException the store failed; the writer retries
     */
    public FastFrame.Body answerConfirm(FastFrame.Header header, FastWriteFrame.Confirm confirm,
            CommitDesk desk) throws IOException {
        Objects.requireNonNull(confirm, "confirm");
        Objects.requireNonNull(desk, "desk");
        Writer writer = writer(header, confirm.key());
        if (writer.refused() != null) {
            return writer.refused();
        }
        desk.confirm(writer.holder(), confirm);
        return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
    }

    /** A write frame's sender: its holder and the roster judging it, or why refused. */
    private record Writer(QuorumFrontier.Holder holder, Roster roster,
            FastFrame.Refused refused) {
    }

    /**
     * ⚠️ IN THIS ORDER: the term, its fence, the sender ROSTERED in it, and
     * the key's pod the sender's own (ADR-0084 decision 8; M13.52g) -- a pod
     * writing under another's key would collide with its dedupe.
     */
    private Writer writer(FastFrame.Header header, FastJournalRecord.IdempotencyKey key)
            throws IOException {
        if (header.epoch() != epoch()) {
            return new Writer(null, null, notThisTerm(header));
        }
        Roster own = ownRoster();
        if (own.fencedBy() > epoch()) {
            return new Writer(null, null, refused(FastFrame.Reason.LOWER_EPOCH, "term "
                    + epoch() + " was fenced by " + own.fencedBy()));
        }
        Optional<Roster.Member> member = own.member(header.senderUid());
        if (member.isEmpty() || member.get().state() != Roster.State.ROSTERED) {
            return new Writer(null, null, refused(FastFrame.Reason.NOT_ROSTERED,
                    header.senderUid() + " is not rostered in term " + epoch()));
        }
        Roster.Incarnation incarnation = member.get().incarnation();
        if (!key.podId().equals(incarnation.podId())) {
            return new Writer(null, null, refused(FastFrame.Reason.NOT_ROSTERED,
                    "the key names pod " + key.podId() + ", not the sender's, "
                            + incarnation.podId()));
        }
        return new Writer(new QuorumFrontier.Holder(header.senderUid(), incarnation.az()),
                own, null);
    }

    /**
     * Sends each writer the EXPOSED {@code desk} owes it (M13.27w; ADR-0081
     * §2.5), addressed to its incarnation at the endpoint this term's roster
     * lists: how many were delivered.
     *
     * <p>⚠️ A FAILED SEND IS DROPPED, NEVER OWED AGAIN, and a writer the
     * roster no longer lists is not sent to: the writer, unanswered, retries
     * its COMMIT, and the desk exposes a retried exposed batch again (M13.27s
     * review round 1, P1) -- so a lost EXPOSED costs one retry, never a queue
     * that grows while a writer is unreachable.
     *
     * @throws IOException the roster could not be read; nothing was drained
     */
    public int sendExposed(CommitDesk desk, TermJoiner.Transport transport) throws IOException {
        Objects.requireNonNull(desk, "desk");
        Objects.requireNonNull(transport, "transport");
        // ⚠️ M13.87: an EXPOSED is owed only for a batch held; an idle tick
        // reads no roster, or a fleet with no fast index pays a GET a second.
        if (desk.leader().heldBatches() == 0) {
            return 0;
        }
        Roster own = ownRoster();
        String self = opened.started().own().leader().podUid();
        int sent = 0;
        for (FastWriteLeader.Exposure owed : desk.exposures()) {
            Optional<Roster.Member> writer = own.member(owed.writerUid());
            if (writer.isEmpty()) {
                continue;
            }
            try {
                transport.exchange(writer.get().incarnation().endpoint(),
                        FastFrame.encode(epoch(), self, owed.writerUid(), owed.exposed()));
                sent++;
            } catch (IOException unreachable) {
                // dropped: the writer's retry is exposed again
            }
        }
        return sent;
    }

    /**
     * Sends each batch's missing copies as REPLICAs to this term's ROSTERED
     * pods, the leader excepted, and counts each holder's REPLICA_ACK --
     * the REPLICA's answer, sent once its group is fsynced (M13.27x; ADR-0081
     * §2.3-2.4): how many were acked.
     *
     * <p>⚠️ AN ANSWER COUNTS ONLY FROM THE INCARNATION ADDRESSED, IN THIS TERM
     * (ADR-0082 §2): a reused name or address never makes a copy in one zone
     * count for another.
     *
     * <p>⚠️ A REFUSAL DECLINES THE COPY, replaced in another pod; an ack marks
     * the pod returned, chosen again for what it declined (M13.27e's review
     * round 2, P11). An unreachable holder stays asked: the re-send after
     * the replica timeout is M13.27y's.
     *
     * @throws IOException the roster could not be read; nothing was sent
     */
    public int sendReplicas(CommitDesk desk, TermJoiner.Transport transport) throws IOException {
        Objects.requireNonNull(desk, "desk");
        Objects.requireNonNull(transport, "transport");
        // ⚠️ M13.87: every REPLICA and re-send is of a held batch.
        if (desk.leader().heldBatches() == 0) {
            return 0;
        }
        Roster own = ownRoster();
        String self = opened.started().own().leader().podUid();
        List<QuorumFrontier.Holder> ready = own.members().stream()
                .filter(m -> m.state() == Roster.State.ROSTERED)
                .filter(m -> !m.incarnation().podUid().equals(self))
                .map(m -> new QuorumFrontier.Holder(m.incarnation().podUid(),
                        m.incarnation().az()))
                .toList();
        // ⚠️ ONE TIMEOUT AFTER A FAILURE OR A BACKPRESSURE REFUSAL, A HOLDER IS
        // AVAILABLE AGAIN and what still waits on it is re-sent (M13.27y;
        // ADR-0081 §2.3); before it, its ask covers no zone, so the copy goes
        // to another.
        long now = mono.nanos();
        List<FastWriteLeader.ReplicaSend> sends = new ArrayList<>();
        for (QuorumFrontier.Holder h : ready) {
            Long at = unavailableAt.get(h.podUid());
            if (at != null && now - at >= REPLICA_TIMEOUT.toNanos()) {
                unavailableAt.remove(h.podUid());
                sends.addAll(desk.resends(h));
            }
        }
        List<QuorumFrontier.Holder> available = ready.stream()
                .filter(h -> !unavailableAt.containsKey(h.podUid())).toList();
        sends.addAll(desk.replicas(available));
        int acked = 0;
        for (FastWriteLeader.ReplicaSend send : sends) {
            String uid = send.holder().podUid();
            Optional<Roster.Member> holder = own.member(uid);
            if (holder.isEmpty()) {
                continue;
            }
            FastFrame.Frame answer;
            try {
                answer = FastFrame.decode(transport.exchange(holder.get().incarnation().endpoint(),
                        FastFrame.encode(epoch(), self, uid, send.replica())));
            } catch (IOException unanswered) {
                unavailableAt.put(uid, now);
                continue;
            }
            if (answer.header().epoch() != epoch() || !answer.header().senderUid().equals(uid)) {
                continue;
            }
            switch (answer.body()) {
                case FastWriteFrame.ReplicaAck ack -> {
                    desk.replicaAck(send.holder(), ack);
                    desk.returned(uid);
                    unavailableAt.remove(uid);
                    acked++;
                }
                case FastFrame.Refused refused when refused.reason()
                        != FastFrame.Reason.LOWER_EPOCH -> {
                    desk.declined(send.holder(), send.replica());
                    if (refused.reason() == FastFrame.Reason.BACKPRESSURE
                            || refused.reason() == FastFrame.Reason.DEPARTING) {
                        unavailableAt.put(uid, now);
                    }
                }
                default -> {
                    // a deposed term's answer, or no answer at all: still asked
                }
            }
        }
        return acked;
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
            FastWriteLeader leader = new FastWriteLeader(epoch(), self,
                    new FastCursor(STREAM_BOUND), termRecord,
                    new QuorumFrontier(self.podUid(), self.az()), journal,
                    this::nextDecision);
            // ⚠️ HELD FROM ITS BIRTH WHILE TERMS ARE UNCLOSED (M13.33o): the
            // desk is built by the first frame that needs it, so a hold put
            // on later could come after a COMMIT it should have held.
            if (!opened.stillOpen().isEmpty()) {
                leader.takingOver();
            }
            if (leaving) {
                leader.stop();
            }
            desk = new CommitDesk(leader, committedNext, ANSWER_WAIT);
        }
        return desk;
    }

    /** How a leader's {@link #leave} ended. */
    public enum Left { DEPARTED_AND_CLOSED, DEPARTED, STAYED, DEPOSED }

    /**
     * This leader's shutdown (ADR-0081 §9; M13.35a): it FIRST stops assigning
     * and exposing, then marks itself departed in its roster -- and closes it
     * in the same write only if no earlier term was open when it started, a
     * takeover it may not have finished being its successor's -- so its
     * successor is handed over with no fence and no collection.
     *
     * <p>⚠️ NEVER WITH AN EXPOSED ENTRY UNCOMMITTED: the leader then stays
     * undeparted, its stop counted as a crash would be. Uploading those
     * first, and departing earlier rosters listing it, is M13.35b's.
     *
     * @throws IOException the store failed; the leader stops undeparted
     */
    public Left leave() throws IOException {
        return leave(() -> { });
    }

    /**
     * The same, uploading and committing what it exposed FIRST through
     * {@code last} (ADR-0081 §9; M13.35c) -- staying undeparted only if some
     * is still uncommitted after it, or it failed.
     */
    public Left leave(FastUploader.Last last) throws IOException {
        CommitDesk built;
        synchronized (this) {
            leaving = true;
            built = desk;
        }
        if (built != null && built.leader().stop()) {
            try {
                last.upload();
            } catch (IOException failed) {
                return Left.STAYED;
            }
            if (built.leader().stop()) {
                return Left.STAYED;
            }
        }
        boolean close = opened.stillOpen().isEmpty();
        RosterDepartures.Outcome outcome =
                departures.leave(opened.started().own().leader().podUid(), close);
        rosterWritten();
        return switch (outcome) {
            case RosterDepartures.Departed departed ->
                    close ? Left.DEPARTED_AND_CLOSED : Left.DEPARTED;
            case RosterDepartures.Deposed deposed -> Left.DEPOSED;
        };
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
