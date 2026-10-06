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
public final class FastWriteLeader {

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
                if (!journal.fits(bytes)) {
                    return new Wait("the journal is at its cap");
                }
                for (FastJournalRecord.Entry e : entries) {
                    cursor.assign(e.key(), e.records().size());
                }
                batches.add(new Batch(writer, commit.key(), assignedAfter, entries,
                        new FastWriteFrame.Assigned(assignedAfter, runs)));
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

    /** The writer's CONFIRM: its copy counts if journaled with the offsets. */
    public synchronized void confirm(Holder writer, FastWriteFrame.Confirm confirm) {
        for (FastWriteFrame.RunAt run : confirm.runs()) {
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

    /** A holder's REPLICA_ACK, sent once its group is fsynced. */
    public synchronized void replicaAck(Holder holder, FastWriteFrame.ReplicaAck ack) {
        for (FastWriteFrame.RunAt run : ack.runs()) {
            frontier.copied(new EntryId(epoch, ack.assignedAfter(), run.stream(),
                    run.firstOffset()), holder);
        }
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

    /** The EXPOSED answers now owed: each batch whose every run the frontier passed. */
    public synchronized List<Exposure> expose() {
        frontier.expose();
        List<Exposure> out = new ArrayList<>();
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
            }
        }
        return out;
    }

    /** How many batches this leader still holds: committed ones are dropped. */
    synchronized int heldBatches() {
        return batches.size();
    }

    private EntryId id(FastJournalRecord.Entry e) {
        return new EntryId(e.epoch(), e.assignedAfter(), e.key(), e.firstOffset());
    }
}
