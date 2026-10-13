// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToLongFunction;

/**
 * A term's COMMITs answered at its leader (ADR-0081 §2; M13.27s): each
 * assigned by {@link FastWriteLeader}, then answered ASSIGNED once the group
 * holding it is fsynced -- the caller whose flush answers a group hands every
 * other writer in it its own answer, by the batch's idempotency key.
 *
 * <p>⚠️ A HELD BATCH IS REFUSED {@code BACKPRESSURE}, never queued here: the
 * writer retries, and its retry is assigned once the bound, the journal or
 * the recorded {@code wal_quorum} lets it (cost.md rule 14).
 *
 * <p>⚠️ A STREAM OPENS AT ITS COMMITTED NEXT OFFSET on its first COMMIT in the
 * term: the cursor and the quorum frontier start where the chain ends.
 *
 * <p>⚠️ A RETRY OF AN EXPOSED BATCH IS EXPOSED AGAIN (M13.27s review round 1,
 * P1): its writer lost the EXPOSED it was sent, so {@link #exposures} owes it
 * once more -- {@link FastWriteLeader#expose} names each batch only once.
 */
public final class CommitDesk {

    private final FastWriteLeader write;
    private final ToLongFunction<RunKey> committedNext;
    private final long waitNanos;
    private final Set<RunKey> opened = new HashSet<>();
    private final List<FastWriteLeader.Exposure> reExposed = new ArrayList<>();
    private final Map<FastJournalRecord.IdempotencyKey,
            CompletableFuture<FastWriteFrame.Assigned>> owed = new ConcurrentHashMap<>();
    /** When this term last uploaded, MIN_VALUE before its first (M13.31c). */
    private long lastUpload = Long.MIN_VALUE;
    /** An upload asked for at once, by a departing pod (M13.31c). */
    private boolean asked;
    /** This term's publication: interest, and what each exposure owes (M13.30d). */
    private final PublishDesk publish = new PublishDesk();

    /**
     * @param committedNext a stream's committed next offset, from the chain
     * @param wait how long a writer whose batch another caller's flush holds
     *     waits for that caller to hand it its answer
     */
    public CommitDesk(FastWriteLeader write, ToLongFunction<RunKey> committedNext,
            Duration wait) {
        this.write = Objects.requireNonNull(write, "write");
        this.committedNext = Objects.requireNonNull(committedNext, "committedNext");
        this.waitNanos = wait.toNanos();
        write.publishTo(publish::exposed);
    }

    /** {@code stream}'s quorum frontier: what is below it is exposed (M13.30o). */
    public long frontier(RunKey stream) {
        return write.frontier(stream);
    }

    /** This term's publication (ADR-0081 §6; M13.30d). */
    public PublishDesk publications() {
        return publish;
    }

    /**
     * The answer to {@code commit} from {@code writer}: ASSIGNED, or REFUSED
     * {@code BACKPRESSURE}.
     *
     * @param available the pods available to the leader, its own among them
     * @throws IOException the journal failed; the writer retries
     */
    public FastFrame.Body answer(Holder writer, FastWriteFrame.Commit commit, Roster roster,
            Set<String> available) throws IOException {
        open(commit);
        return switch (write.commit(writer, commit, roster, available)) {
            case FastWriteLeader.Answered answered -> {
                exposeAgain(new FastWriteLeader.Exposure(writer.podUid(), answered.exposed()));
                yield answered.assigned();
            }
            case FastWriteLeader.Wait wait -> new FastFrame.Refused(
                    FastFrame.Reason.BACKPRESSURE, Optional.empty(), wait.why());
            case FastWriteLeader.Pending pending -> flushFor(commit.key());
        };
    }

    /** A writer's CONFIRM (M13.27v): its copy counted, or replaced if not journaled. */
    public void confirm(Holder writer, FastWriteFrame.Confirm confirm) {
        write.confirm(writer, confirm);
    }

    /** The REPLICAs to send now to {@code available} holders (M13.27x). */
    public List<FastWriteLeader.ReplicaSend> replicas(List<Holder> available) {
        return write.replicas(available);
    }

    /** The REPLICAs still waiting on {@code holder}, to re-send (M13.27y). */
    public List<FastWriteLeader.ReplicaSend> resends(Holder holder) {
        return write.resends(holder);
    }

    /** A holder's REPLICA_ACK: its copies count (M13.27x). */
    public void replicaAck(Holder holder, FastWriteFrame.ReplicaAck ack) {
        write.replicaAck(holder, ack);
    }

    /** A holder refused a REPLICA: its copies are replaced elsewhere (M13.27x). */
    public void declined(Holder holder, FastWriteFrame.Replica replica) {
        write.declined(holder, replica);
    }

    /** A pod answered without refusing: it may be chosen again (M13.27x). */
    public void returned(String podUid) {
        write.returned(podUid);
    }

    /** The desk's leader itself, for this term's upload (M13.31a). */
    FastWriteLeader leader() {
        return write;
    }

    /** The pods holding a copy of an unreleased entry, the leader aside (M13.31f). */
    public java.util.Set<String> holders() {
        return write.holders();
    }

    /**
     * {@code podUid} was lost -- its UID gone, or out of the EndpointSlice
     * (ADR-0081 §7; M13.31e): an upload is asked for at once when it holds a
     * copy of an unreleased entry, so a second loss has the least time to be
     * quorum loss. Whether one was asked; ⚠️ nothing for a pod holding none.
     */
    public synchronized boolean holderLost(String podUid) {
        if (!write.holders().contains(podUid)) {
            return false;
        }
        asked = true;
        return true;
    }

    /** An upload is asked for at once: a departing pod's DEPART phase 1 (ADR-0081 §7, §9). */
    public synchronized void askUpload() {
        asked = true;
    }

    /**
     * Whether a paced upload is due at {@code nowNanos} (ADR-0081 §7; M13.31c):
     * asked for, pressed (a stream at {@code B / 2}, the journal past half the
     * leader's share), the term's first, or {@code flushTimer} -- the tightest
     * {@code flush_timer} among fast indices -- since its last. ⚠️ Never more
     * often than the term's own {@code min_upload_interval} pacing allows.
     */
    public synchronized boolean uploadDue(long nowNanos, Duration flushTimer) {
        return asked || write.uploadPressed() || lastUpload == Long.MIN_VALUE
                || nowNanos - lastUpload >= flushTimer.toNanos();
    }

    /** An upload was made at {@code nowNanos}: the next waits its trigger. */
    public synchronized void uploaded(long nowNanos) {
        lastUpload = nowNanos;
        asked = false;
    }

    /** Each batch's quorum wait, told to {@code waits} from now (M13.36). */
    public void observeQuorumWait(MonotonicClock clock, java.util.function.LongConsumer waits) {
        write.observe(clock, waits);
    }

    /** The desk's leader, as a takeover holds it (M13.33o). */
    public TakeoverRun.Hold hold() {
        return write;
    }

    /**
     * The EXPOSED answers now owed, a retried exposed batch's among them: what
     * the EXPOSED sender (M13.27m) drains.
     */
    public List<FastWriteLeader.Exposure> exposures() {
        List<FastWriteLeader.Exposure> owedNow;
        synchronized (this) {
            owedNow = new ArrayList<>(reExposed);
            reExposed.clear();
        }
        owedNow.addAll(write.expose());
        return owedNow;
    }

    private synchronized void exposeAgain(FastWriteLeader.Exposure exposure) {
        reExposed.add(exposure);
    }

    private synchronized void open(FastWriteFrame.Commit commit) {
        for (FastWriteFrame.CommitRun run : commit.runs()) {
            // ⚠️ MARKED OPEN ONLY ONCE OPENED (M13.34j review round 1, R1): the
            // chain's read can fail, and a stream marked first would never open.
            if (!opened.contains(run.stream())) {
                write.open(run.stream(), committedNext.applyAsLong(run.stream()));
                opened.add(run.stream());
            }
        }
    }

    /**
     * ⚠️ EVERY CALLER FLUSHES AFTER ITS OWN COMMIT: either its flush answers
     * its batch, or an earlier caller's flush already took it and hands it
     * over -- a batch is never left waiting for a flush nobody runs.
     */
    private FastWriteFrame.Assigned flushFor(FastJournalRecord.IdempotencyKey key)
            throws IOException {
        CompletableFuture<FastWriteFrame.Assigned> mine = owed.computeIfAbsent(key,
                k -> new CompletableFuture<>());
        try {
            for (FastWriteLeader.Answer each : write.flushGroup()) {
                owed.computeIfAbsent(each.key(), k -> new CompletableFuture<>())
                        .complete(each.assigned());
            }
            return mine.get(waitNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for the group holding " + key,
                    interrupted);
        } catch (ExecutionException | TimeoutException notAnswered) {
            throw new IOException("the group holding " + key + " was not answered",
                    notAnswered);
        } finally {
            owed.remove(key, mine);
        }
    }
}
