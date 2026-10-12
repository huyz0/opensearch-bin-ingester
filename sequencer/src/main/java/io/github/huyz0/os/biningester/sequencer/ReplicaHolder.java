// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A holder's answer to a REPLICA (M13.28a; ADR-0081 §2.4): from the leader of
 * a term this pod joined, one batch of that term's entries, journaled and
 * fsynced before it is answered REPLICA_ACK.
 *
 * <p>⚠️ REFUSED NOT_ROSTERED from anyone but the term's leader, for a term
 * not joined, or for an entry of another term: the incarnations that hold a
 * term's copies are its members, and only its leader assigns them (§1). The
 * router has already fenced the frame's epoch.
 *
 * <p>⚠️ PAST THE CAP, REFUSED BACKPRESSURE KEEPING NOTHING OF THE BATCH: the
 * leader replaces the copy, and half a batch held would count for no one.
 * Departing, refused DEPARTING under the {@link HolderGate} a departure's
 * empty check takes.
 *
 * <p>⚠️ A RE-SENT REPLICA (M13.27y) IS ACKED AGAIN, NEVER JOURNALED TWICE:
 * an entry already held is skipped, so the ack the leader lost is repeated.
 */
public final class ReplicaHolder {

    private final FastJournal journal;
    private final JoinedTerms joined;
    private final HolderGate gate;

    public ReplicaHolder(FastJournal journal, JoinedTerms joined, HolderGate gate) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.joined = Objects.requireNonNull(joined, "joined");
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    /**
     * The answer to {@code replica}, sent under {@code header}.
     *
     * @throws IOException the journal failed; nothing is answered and the
     *     leader re-sends after the replica timeout
     */
    public FastFrame.Body answer(FastFrame.Header header, FastWriteFrame.Replica replica)
            throws IOException {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(replica, "replica");
        long epoch = header.epoch();
        // ⚠️ ONE CHECK FOR BOTH: a term this pod has not joined, or has seen
        // closed, has no leader here -- `leaderOf` is empty wherever
        // `mayHold` is false.
        Optional<String> leader = joined.leaderOf(epoch);
        if (leader.isEmpty() || !leader.get().equals(header.senderUid())) {
            return refused(FastFrame.Reason.NOT_ROSTERED, header.senderUid()
                    + " is not the leader of a term this pod joined: " + epoch);
        }
        long assignedAfter = replica.entries().get(0).assignedAfter();
        for (FastJournalRecord.Entry e : replica.entries()) {
            if (e.epoch() != epoch || e.assignedAfter() != assignedAfter) {
                return refused(FastFrame.Reason.NOT_ROSTERED, "a REPLICA carries one batch "
                        + "of its own term");
            }
        }
        Optional<Boolean> kept = gate.whileAccepting(() -> keep(replica.entries()));
        if (kept.isEmpty()) {
            return refused(FastFrame.Reason.DEPARTING, "this pod is departing");
        }
        if (!kept.get()) {
            return refused(FastFrame.Reason.BACKPRESSURE, "this pod's journal is at its cap");
        }
        List<FastWriteFrame.RunAt> runs = new ArrayList<>();
        for (FastJournalRecord.Entry e : replica.entries()) {
            runs.add(new FastWriteFrame.RunAt(e.key(), e.firstOffset()));
        }
        return new FastWriteFrame.ReplicaAck(assignedAfter, runs);
    }

    /** Journals and fsyncs every entry not yet held: false, keeping none, past the cap. */
    private boolean keep(List<FastJournalRecord.Entry> entries) throws IOException {
        List<FastJournalRecord.Entry> fresh = new ArrayList<>();
        long bytes = 0;
        for (FastJournalRecord.Entry e : entries) {
            if (!journal.holds(e.epoch(), e.key(), e.assignedAfter(), e.firstOffset())) {
                fresh.add(e);
                bytes += e.encode().length;
            }
        }
        if (!journal.fits(bytes)) {
            return false;
        }
        List<FastJournalRecord.Entry> appended = new ArrayList<>();
        for (FastJournalRecord.Entry e : fresh) {
            if (!journal.append(e)) {
                // another caller filled the journal between the check and here
                for (FastJournalRecord.Entry gone : appended) {
                    journal.drop(gone.key(), gone.epoch(), gone.assignedAfter(),
                            gone.firstOffset());
                }
                journal.sync();
                return false;
            }
            appended.add(e);
        }
        journal.sync();
        return true;
    }

    private static FastFrame.Refused refused(FastFrame.Reason reason, String text) {
        return new FastFrame.Refused(reason, Optional.empty(), text);
    }
}
