// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The leader's side of a fast write (ADR-0081 §2; M13.27e): a COMMIT assigned
 * under the term record, admission and the per-stream bound, journaled with
 * its offsets, answered ASSIGNED only once the journal group holding it is
 * fsynced, its copies counted from CONFIRM and REPLICA_ACK, sent to holders by
 * REPLICA, and answered EXPOSED once the quorum frontier passes every run.
 *
 * <p>⚠️ A RUN OF MORE THAN {@code B} RECORDS IS REFUSED, the writer's to
 * split (M13.29): a run gets one first offset, and it could never fit.
 *
 * <p>⚠️ A BATCH IS ASSIGNED WHOLE OR WAITS WHOLE: every run must have a
 * recorded {@code wal_quorum}, admission, room under {@code B} and room in
 * the journal before any offset is taken -- backpressure, never a refusal
 * (cost.md rule 14).
 *
 * <p>⚠️ ONE FSYNC PER GROUP, NEVER UNDER THE STATE MONITOR: {@link #commit}
 * appends; {@link #flushGroup} fsyncs everything appended and only then counts
 * the leader's copies and answers. The journal is reached under one I/O lock,
 * always taken before the monitor; the monitor -- which {@link #expose},
 * {@link #confirm} and {@link #replicaAck} take -- is never held across I/O.
 *
 * <p>⚠️ A RETRY IS ANSWERED FROM ITS OFFSETS ONLY IF EXPOSED IN THIS TERM
 * (ADR-0081 §2): the chain carries no idempotency keys, so anything else --
 * unexposed, discarded, committed -- is assigned anew, a duplicate removed by
 * {@code _id} as the default path's retries are.
 */
public final class FastWriteLeader implements TakeoverRun.Hold {

    /** How a COMMIT ended for now. */
    public sealed interface Outcome permits Pending, Answered, Wait {
    }

    /** Assigned and appended: answered by the next {@link #flushGroup}. */
    public record Pending(long assignedAfter) implements Outcome {
    }

    /**
     * A retry of an exposed batch of this term, answered from its offsets --
     * with its EXPOSED, so a writer whose EXPOSED was lost can still ack
     * (M13.27c review round 1, P4).
     */
    public record Answered(FastWriteFrame.Assigned assigned, FastWriteFrame.Exposed exposed)
            implements Outcome {
    }

    /** Held, unassigned: backpressure, retried later. */
    public record Wait(String why) implements Outcome {
    }

    /**
     * An ASSIGNED answer to send, once fsynced, with the batch's key: every
     * batch between two decisions shares {@code assignedAfter}, so only the
     * key tells a writer's answers apart (M13.27s).
     */
    public record Answer(String writerUid, FastJournalRecord.IdempotencyKey key,
            FastWriteFrame.Assigned assigned) {
    }

    /** An EXPOSED answer to send. */
    public record Exposure(String writerUid, FastWriteFrame.Exposed exposed) {
    }

    /** A REPLICA to send to a holder. */
    public record ReplicaSend(Holder holder, FastWriteFrame.Replica replica) {
    }

    private static final class Batch {
        final Holder writer;
        final FastJournalRecord.IdempotencyKey key;
        final long assignedAfter;
        final List<FastJournalRecord.Entry> entries;
        final FastWriteFrame.Assigned assigned;
        boolean answered;
        FastWriteFrame.Exposed exposed;
        /** When it was assigned, for its quorum wait (M13.36); MIN_VALUE when unobserved. */
        long assignedNanos = Long.MIN_VALUE;

        Batch(Holder writer, FastJournalRecord.IdempotencyKey key, long assignedAfter,
                List<FastJournalRecord.Entry> entries, FastWriteFrame.Assigned assigned) {
            this.writer = writer;
            this.key = key;
            this.assignedAfter = assignedAfter;
            this.entries = entries;
            this.assigned = assigned;
        }
    }

    private final long epoch;
    private final Roster.Incarnation self;
    private final FastCursor cursor;
    private final TermRecordWriter termRecord;
    private final QuorumFrontier frontier;
    private final FastJournal journal;
    private final LongSupplier nextDecision;
    private final ReentrantLock io = new ReentrantLock();
    private final List<Batch> batches = new ArrayList<>();
    /** Streams a takeover has not decided yet: assigned nothing (M13.33l). */
    private final Set<RunKey> undecided = new HashSet<>();
    /** Taking over, not every member heard: only {@link #released} streams assign (M13.33k). */
    private boolean holdingAll;
    private final Set<RunKey> released = new HashSet<>();
    /** Shutting down: nothing more assigned or exposed (M13.35a). */
    private boolean stopping;
    /** The clock and the sink each batch's quorum wait is told to (M13.36); null while none. */
    private MonotonicClock clock;
    private java.util.function.LongConsumer quorumWaits;
    /** Told each exposure step's entries, for publication (M13.30d); null while none. */
    private java.util.function.Consumer<List<FastJournalRecord.Entry>> published;
    private final Map<FastJournalRecord.IdempotencyKey, Batch> exposedByKey = new HashMap<>();
    private final Map<RunKey, Long> committedNext = new HashMap<>();

    /**
     * @param nextDecision the {@code seq} this roster's next decision will
     *     take: every entry's {@code assignedAfter} (ADR-0081 §2)
     */
    public FastWriteLeader(long epoch, Roster.Incarnation self, FastCursor cursor,
            TermRecordWriter termRecord, QuorumFrontier frontier, FastJournal journal,
            LongSupplier nextDecision) {
        this.epoch = epoch;
        this.self = Objects.requireNonNull(self, "self");
        this.cursor = Objects.requireNonNull(cursor, "cursor");
        this.termRecord = Objects.requireNonNull(termRecord, "termRecord");
        this.frontier = Objects.requireNonNull(frontier, "frontier");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.nextDecision = Objects.requireNonNull(nextDecision, "nextDecision");
    }

    /** {@code stream}'s quorum frontier: every offset below it exposable (M13.31a). */
    public synchronized long frontier(RunKey stream) {
        return frontier.frontier(stream);
    }

    /** Starts {@code stream} at its committed next offset, in the cursor and the frontier. */
    public synchronized void open(RunKey stream, long committedNext) {
        cursor.open(stream, committedNext);
        frontier.open(stream, committedNext);
    }

    /**
     * The chain committed {@code stream} up to {@code committedNext}: the bound
     * moves, and a batch every run of which is committed is no longer held --
     * its retry is assigned anew.
     */
    public synchronized void committed(RunKey stream, long next) {
        cursor.committed(stream, next);
        frontier.committed(stream, next);
        committedNext.merge(stream, next, Math::max);
        batches.removeIf(b -> {
            boolean done = b.entries.stream().allMatch(e ->
                    e.endOffset() <= committedNext.getOrDefault(e.key(), 0L));
            if (done) {
                exposedByKey.remove(b.key, b);
            }
            return done;
        });
    }

    /**
     * Assigns a COMMIT from {@code writer}, or holds it.
     *
     * @param availableUids the pods available to the leader, its own among them
     */
    public Outcome commit(Holder writer, FastWriteFrame.Commit commit, Roster roster,
            Set<String> availableUids) throws IOException {
        io.lock();
        try {
            List<FastJournalRecord.Entry> toAppend;
            synchronized (this) {
                Batch retried = exposedByKey.get(commit.key());
                if (retried != null) {
                    return new Answered(retried.assigned, retried.exposed);
                }
                if (stopping) {
                    return new Wait("this leader is stopping");
                }
                Set<RunKey> seen = new HashSet<>();
                List<Integer> quorums = new ArrayList<>();
                for (FastWriteFrame.CommitRun run : commit.runs()) {
                    if (!seen.add(run.stream())) {
                        throw new IllegalArgumentException("a COMMIT names a stream twice: "
                                + run.stream());
                    }
                    // ⚠️ A RUN OF MORE THAN B NEVER FITS: held, it would wait for
                    // ever. ASSIGNED gives a run one first offset, so only the
                    // writer can split it (M13.29) -- a longer run is its error.
                    if (run.records().size() > cursor.bound()) {
                        throw new IllegalArgumentException("a run of " + run.records().size()
                                + " records exceeds B = " + cursor.bound() + ": the writer splits it");
                    }
                    // ⚠️ NOTHING ON A STREAM BEFORE ITS DECISION IS DURABLE
                    // (M13.33l; ADR-0081 §5 invariant d): an offset assigned
                    // now could be one the decision frees.
                    if (awaitsDecision(run.stream())) {
                        return new Wait("stream " + run.stream()
                                + " awaits its takeover decision");
                    }
                    OptionalInt q = termRecord.assignable(run.stream().indexId());
                    if (q.isEmpty()) {
                        return new Wait("wal_quorum not yet recorded for " + run.stream());
                    }
                    if (!FastAdmission.admits(roster, availableUids, q.getAsInt())) {
                        return new Wait("fewer than " + q.getAsInt() + " zones available");
                    }
                    if (!cursor.fits(run.stream(), run.records().size())) {
                        return new Wait("stream " + run.stream() + " at its bound");
                    }
                    quorums.add(q.getAsInt());
                }
                long assignedAfter = nextDecision.getAsLong();
                List<FastJournalRecord.Entry> entries = new ArrayList<>();
                List<FastWriteFrame.AssignedRun> runs = new ArrayList<>();
                long bytes = 0;
                for (int i = 0; i < commit.runs().size(); i++) {
                    FastWriteFrame.CommitRun run = commit.runs().get(i);
                    long first = cursor.cursor(run.stream());
                    FastJournalRecord.Entry e = new FastJournalRecord.Entry(epoch, run.stream(),
                            first, quorums.get(i), assignedAfter, commit.key(), run.records());
                    entries.add(e);
                    bytes += e.encode().length;
                    runs.add(new FastWriteFrame.AssignedRun(run.stream(), first, quorums.get(i),
                            !writer.az().equals(self.az()) && quorums.get(i) >= 2));
                }
                if (!journal.fitsLeaderShare(bytes)) {
                    return new Wait("the journal is at the leader's half of its cap");
                }
                for (FastJournalRecord.Entry e : entries) {
                    cursor.assign(e.key(), e.records().size());
                }
                Batch batch = new Batch(writer, commit.key(), assignedAfter, entries,
                        new FastWriteFrame.Assigned(assignedAfter, runs));
                if (clock != null) {
                    batch.assignedNanos = clock.nanos();
                }
                batches.add(batch);
                toAppend = entries;
            }
            // APPENDED OUTSIDE THE MONITOR (M13.27c review round 1, P3), under
            // the I/O lock every journal call takes: the batch is not answered
            // -- so neither exposed nor replicated -- until flushGroup.
            for (int i = 0; i < toAppend.size(); i++) {
                // A REFUSED APPEND IS NEVER ANSWERED (M13.27c review rounds 2
                // and 3, P6, P7): fits was checked under this lock, but a
                // journal another writer shares could fill meanwhile. The batch
                // is withdrawn whole -- its appended entries dropped, its
                // offsets returned to the cursor -- so no flush ever fsyncs,
                // counts and answers ASSIGNED for an entry not on disk.
                if (!journal.append(toAppend.get(i))) {
                    for (FastJournalRecord.Entry appended : toAppend.subList(0, i)) {
                        journal.drop(appended.key(), appended.epoch(), appended.assignedAfter(),
                                appended.firstOffset());
                    }
                    synchronized (this) {
                        batches.removeIf(b -> b.entries == toAppend);
                        for (FastJournalRecord.Entry e : toAppend) {
                            cursor.resume(e.key(), e.firstOffset());
                        }
                    }
                    throw new IllegalStateException("the journal refused an entry it fit "
                            + "a moment ago: it is shared, or its cap moved");
                }
            }
            return new Pending(toAppend.get(0).assignedAfter());
        } finally {
            io.unlock();
        }
    }

    /**
     * Fsyncs every appended entry, then counts the leader's copies and returns
     * the ASSIGNED answers now owed.
     */
    public List<Answer> flushGroup() throws IOException {
        io.lock();
        try {
            List<Batch> owed;
            synchronized (this) {
                owed = batches.stream().filter(b -> !b.answered).toList();
            }
            if (owed.isEmpty()) {
                return List.of();
            }
            journal.sync();
            List<Answer> answers = new ArrayList<>();
            synchronized (this) {
                for (Batch b : owed) {
                    for (int i = 0; i < b.entries.size(); i++) {
                        FastJournalRecord.Entry e = b.entries.get(i);
                        frontier.assigned(id(e), e.records().size(), e.walQuorum());
                        // THE WRITER'S REQUIRED COPY IS ASKED (M13.27c review
                        // round 1, P1): it covers the writer's zone, so no
                        // REPLICA goes there -- max(q - 1, [W not in AZ(L)])
                        // cross-zone copies, never one more (section 2.3).
                        if (b.assigned.runs().get(i).copyRequired()) {
                            frontier.asked(id(e), b.writer);
                        }
                    }
                    b.answered = true;
                    answers.add(new Answer(b.writer.podUid(), b.key, b.assigned));
                }
            }
            return answers;
        } finally {
            io.unlock();
        }
    }

    /**
     * The writer's CONFIRM: its copy counts if journaled with the offsets.
     *
     * <p>⚠️ ONLY FOR A RUN OF THE SENDER's OWN BATCH under the CONFIRM's key
     * (M13.27v review round 1, P1): {@code assignedAfter} is shared by every
     * batch between two decisions, so a CONFIRM naming another writer's entry
     * would count a copy no pod in its zone holds.
     */
    public synchronized void confirm(Holder writer, FastWriteFrame.Confirm confirm) {
        for (FastWriteFrame.RunAt run : confirm.runs()) {
            if (!ownRun(writer, confirm, run)) {
                continue;
            }
            EntryId id = new EntryId(epoch, confirm.assignedAfter(), run.stream(),
                    run.firstOffset());
            if (confirm.copyJournaled()) {
                frontier.copied(id, writer);
            } else {
                // NOT JOURNALED: the writer's ask is withdrawn, so its zone is
                // uncovered and the copy replaced -- held as an ask, the entry
                // never completed and its stream stalled (M13.27c review round
                // 2, P5; ADR-0081 section 2.3).
                frontier.withdraw(id, writer.podUid());
            }
        }
    }

    private boolean ownRun(Holder writer, FastWriteFrame.Confirm confirm,
            FastWriteFrame.RunAt run) {
        for (Batch b : batches) {
            if (b.key.equals(confirm.key()) && b.writer.podUid().equals(writer.podUid())
                    && b.assignedAfter == confirm.assignedAfter()) {
                for (FastJournalRecord.Entry e : b.entries) {
                    if (e.key().equals(run.stream()) && e.firstOffset() == run.firstOffset()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * A takeover has begun (M13.33k): every stream is held until it is
     * {@link #decided} or {@link #everyMemberHeard} -- a member not yet heard
     * may hold entries of a stream no one else named. The caller calls it
     * before the term serves.
     */
    public synchronized void takingOver() {
        holdingAll = true;
        released.clear();
    }

    /**
     * Holds {@code streams} until each is {@link #decided} (M13.33l): a
     * takeover's streams with collected entries.
     */
    @Override
    public synchronized void undecided(java.util.Collection<RunKey> streams) {
        undecided.addAll(streams);
    }

    /**
     * Whether {@code stream} waits for its takeover decision: not yet
     * decided, or every member not yet heard and it not yet released -- what
     * holds a fast COMMIT, and a default-path run of a walked index too
     * (ADR-0081 §5 step 3's tail; M13.33n).
     */
    public synchronized boolean awaitsDecision(RunKey stream) {
        return undecided.contains(stream) || holdingAll && !released.contains(stream);
    }

    /** Every member answered or is gone: only the {@link #undecided} streams stay held. */
    @Override
    public synchronized void everyMemberHeard() {
        holdingAll = false;
    }

    /**
     * {@code stream}'s chain entry and decision are durable: it is assigned
     * again from {@code resumeAt}, the chain's next offset its recovery moved
     * (M13.33l). ⚠️ THE CURSOR MOVES BEFORE THE STREAM IS RELEASED (its review
     * round 1, P1), under this desk's lock: a COMMIT never sees it released at
     * the pre-recovery cursor. The chain's next offset IS {@code resumeAt} --
     * the recovery committed or voided up to it -- so it opens there.
     */
    @Override
    public synchronized void decided(RunKey stream, long resumeAt) {
        open(stream, resumeAt);
        undecided.remove(stream);
        if (holdingAll) {
            released.add(stream);
        }
    }

    /** A holder's REPLICA_ACK, sent once its group is fsynced. */
    public synchronized void replicaAck(Holder holder, FastWriteFrame.ReplicaAck ack) {
        for (FastWriteFrame.RunAt run : ack.runs()) {
            frontier.copied(new EntryId(epoch, ack.assignedAfter(), run.stream(),
                    run.firstOffset()), holder);
        }
    }

    /**
     * The REPLICAs still asked of {@code holder} and unanswered, one per
     * batch (M13.27y): what is re-sent to it every replica timeout.
     */
    public synchronized List<ReplicaSend> resends(Holder holder) {
        List<ReplicaSend> sends = new ArrayList<>();
        for (Batch b : batches) {
            if (!b.answered || b.exposed != null) {
                continue;
            }
            List<FastJournalRecord.Entry> waiting = new ArrayList<>();
            for (FastJournalRecord.Entry e : b.entries) {
                if (frontier.isAsked(id(e), holder.podUid())) {
                    waiting.add(e);
                }
            }
            if (!waiting.isEmpty()) {
                sends.add(new ReplicaSend(holder, new FastWriteFrame.Replica(waiting)));
            }
        }
        return sends;
    }

    /**
     * {@code holder} refused {@code replica} (M13.27x; ADR-0081 §2.3): its asks
     * are withdrawn and it is not chosen for those entries again until it
     * returns, so each copy is replaced in another pod.
     */
    public synchronized void declined(Holder holder, FastWriteFrame.Replica replica) {
        for (FastJournalRecord.Entry e : replica.entries()) {
            frontier.withdraw(id(e), holder.podUid());
        }
    }

    /**
     * {@code podUid} answered without refusing (M13.27x; M13.27e's review
     * round 2, P11): it may be chosen again for every copy it declined.
     */
    public synchronized void returned(String podUid) {
        frontier.returned(podUid);
    }

    /**
     * The REPLICAs to send now: each answered, unexposed batch to the holders
     * it lacks -- ONE BATCH PER REPLICA PER HOLDER (ADR-0082 section 2; M13.27c
     * review round 1, P2), since its REPLICA_ACK names one assignedAfter.
     */
    public synchronized List<ReplicaSend> replicas(List<Holder> available) {
        List<ReplicaSend> sends = new ArrayList<>();
        for (Batch b : batches) {
            if (!b.answered || b.exposed != null) {
                continue;
            }
            Map<String, Holder> holders = new LinkedHashMap<>();
            Map<String, List<FastJournalRecord.Entry>> toSend = new LinkedHashMap<>();
            for (FastJournalRecord.Entry e : b.entries) {
                for (Holder h : frontier.holdersFor(id(e), available)) {
                    frontier.asked(id(e), h);
                    holders.put(h.podUid(), h);
                    toSend.computeIfAbsent(h.podUid(), k -> new ArrayList<>()).add(e);
                }
            }
            toSend.forEach((uid, entries) -> sends.add(new ReplicaSend(holders.get(uid),
                    new FastWriteFrame.Replica(entries))));
        }
        return sends;
    }

    /**
     * Tells {@code waits} each batch's quorum wait from now: the nanoseconds
     * from its assignment to its exposure (M13.36; SPEC criterion 17).
     */
    public synchronized void observe(MonotonicClock clock,
            java.util.function.LongConsumer waits) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.quorumWaits = Objects.requireNonNull(waits, "waits");
    }

    /** The EXPOSED answers now owed: each batch whose every run the frontier passed. */
    public synchronized List<Exposure> expose() {
        if (stopping) {
            return List.of();
        }
        frontier.expose();
        List<Exposure> out = new ArrayList<>();
        List<FastJournalRecord.Entry> step = new ArrayList<>();
        for (Batch b : batches) {
            if (!b.answered || b.exposed != null) {
                continue;
            }
            boolean all = b.entries.stream()
                    .allMatch(e -> e.endOffset() <= frontier.frontier(e.key()));
            if (all) {
                List<FastWriteFrame.RunAt> runs = b.entries.stream()
                        .map(e -> new FastWriteFrame.RunAt(e.key(), e.firstOffset())).toList();
                b.exposed = new FastWriteFrame.Exposed(b.assignedAfter, runs);
                exposedByKey.put(b.key, b);
                out.add(new Exposure(b.writer.podUid(), b.exposed));
                step.addAll(b.entries);
                if (quorumWaits != null && b.assignedNanos != Long.MIN_VALUE) {
                    quorumWaits.accept(clock.nanos() - b.assignedNanos);
                }
            }
        }
        if (published != null && !step.isEmpty()) {
            published.accept(step);
        }
        return out;
    }

    /** Each exposure step's entries, told to {@code to} from now (ADR-0081 §6; M13.30d). */
    public synchronized void publishTo(
            java.util.function.Consumer<List<FastJournalRecord.Entry>> to) {
        this.published = Objects.requireNonNull(to, "to");
    }

    /**
     * Stops assigning and exposing, FIRST at a shutdown (ADR-0081 §9; M13.35a):
     * a CONFIRM arriving after must not be answered as complete.
     *
     * @return whether an entry this leader exposed is not yet committed -- it
     *     then stays undeparted, its stop counted as a crash would be; an
     *     entry never exposed was never acked, and its writer retries
     */
    public synchronized boolean stop() {
        stopping = true;
        return batches.stream().anyMatch(b -> b.exposed != null);
    }

    /**
     * Whether an upload is pressed for now (ADR-0081 §7; M13.31c): a stream at
     * {@code B / 2} uncommitted offsets, or the journal past half the leader's share.
     */
    public boolean uploadPressed() {
        return cursor.anyUploadDue() || journal.pastHalfLeaderShare();
    }

    /** How many batches this leader still holds: committed ones are dropped. */
    synchronized int heldBatches() {
        return batches.size();
    }

    private EntryId id(FastJournalRecord.Entry e) {
        return new EntryId(e.epoch(), e.assignedAfter(), e.key(), e.firstOffset());
    }
}
