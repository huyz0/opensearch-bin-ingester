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
            if (opened.add(run.stream())) {
                write.open(run.stream(), committedNext.applyAsLong(run.stream()));
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
