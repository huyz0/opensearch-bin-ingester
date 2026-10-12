// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A fast index's appends batched into one COMMIT of many runs (M13.29d;
 * ADR-0081 §2), by group commit: the first append sends at once, and every
 * append that arrives while a COMMIT is in flight goes in the next -- so
 * COMMITs scale with how often the leader answers, never with appends.
 *
 * <p>⚠️ ONE RUN PER STREAM: the appends of a stream are concatenated in the
 * order they arrived, so each producer's offsets are its own slice of the
 * run's, and no run passes {@code B} -- an append that would is left for the
 * next COMMIT, its stream taking nothing more, and one longer than {@code B}
 * alone goes the default way.
 *
 * <p>⚠️ A SENDER SENDS ONE COMMIT, then hands the turn to the first append
 * still queued (its review round 1, P2): an appender that kept sending while
 * others queued would answer its own producer only once the queue emptied,
 * and under load that producer would time out on a write already exposed.
 * No thread of its own, so an idle batcher costs nothing.
 */
public final class FastBatcher implements FastPath {

    /** One COMMIT of {@code runs}: each stream's first offset, or empty on NOT_FAST. */
    @FunctionalInterface
    public interface Batch {
        Optional<Map<RunKey, Long>> write(List<FastWriteFrame.CommitRun> runs)
                throws IOException;
    }

    /** An append waiting: answered, or handed the turn to send. */
    private record Waiting(RunKey stream, List<SegmentRecord> records,
            CompletableFuture<Optional<AppendResult>> answer, CompletableFuture<Void> turn) {
    }

    private final Batch batch;
    private final long bound;
    private final ArrayDeque<Waiting> queue = new ArrayDeque<>();
    private boolean sending;

    /** @param bound {@code B}: no run of a COMMIT is longer */
    public FastBatcher(Batch batch, long bound) {
        this.batch = Objects.requireNonNull(batch, "batch");
        if (bound < 1) {
            throw new IllegalArgumentException("a bound of at least one record");
        }
        this.bound = bound;
    }

    @Override
    public Optional<AppendResult> append(RunKey stream, List<SegmentRecord> records)
            throws IOException {
        if (records.size() > bound) {
            return Optional.empty();
        }
        Waiting mine = new Waiting(stream, List.copyOf(records), new CompletableFuture<>(),
                new CompletableFuture<>());
        synchronized (this) {
            queue.add(mine);
            if (!sending) {
                sending = true;
                mine.turn().complete(null);
            }
        }
        while (!mine.answer().isDone()) {
            // ⚠️ WAITED ON WITHOUT ITS FAILURE (its review round 2, P4): an
            // answer failed by another appender's COMMIT is unwrapped below,
            // as the IOException the producer retries on.
            CompletableFuture.anyOf(mine.answer().handle((answer, failed) -> null),
                    mine.turn()).join();
            if (!mine.answer().isDone()) {
                sendOne();
            }
        }
        try {
            return mine.answer().join();
        } catch (CompletionException failed) {
            if (failed.getCause() instanceof IOException io) {
                throw io;
            }
            throw failed;
        }
    }

    /** How many appends wait for a COMMIT, for a test. */
    synchronized int queued() {
        return queue.size();
    }

    /**
     * Sends one COMMIT of what is queued, then hands the turn on. ⚠️ THE TURN
     * PASSES WHATEVER HAPPENS (its review round 1, P1): a sender that threw,
     * an {@code Error} included, would otherwise leave every later append
     * waiting for a turn nobody gives.
     */
    private void sendOne() {
        List<Waiting> taken;
        synchronized (this) {
            taken = take();
        }
        try {
            send(taken);
        } finally {
            synchronized (this) {
                Waiting next = queue.peek();
                if (next == null) {
                    sending = false;
                } else {
                    next.turn().complete(null);
                }
            }
        }
    }

    /**
     * The queued appends one COMMIT carries: in order, no stream's run past
     * the bound. ⚠️ A STREAM HELD BACK TAKES NOTHING MORE, so its appends are
     * assigned in the order they arrived.
     */
    private List<Waiting> take() {
        List<Waiting> taken = new ArrayList<>();
        Map<RunKey, Long> sizes = new LinkedHashMap<>();
        Set<RunKey> held = new HashSet<>();
        for (Iterator<Waiting> it = queue.iterator(); it.hasNext(); ) {
            Waiting w = it.next();
            long size = sizes.getOrDefault(w.stream(), 0L) + w.records().size();
            if (held.contains(w.stream()) || size > bound) {
                held.add(w.stream());
                continue;
            }
            sizes.put(w.stream(), size);
            taken.add(w);
            it.remove();
        }
        return taken;
    }

    private void send(List<Waiting> taken) {
        if (taken.isEmpty()) {
            return;
        }
        Map<RunKey, List<SegmentRecord>> runs = new LinkedHashMap<>();
        Map<Waiting, Integer> at = new IdentityHashMap<>();
        for (Waiting w : taken) {
            List<SegmentRecord> run = runs.computeIfAbsent(w.stream(), k -> new ArrayList<>());
            at.put(w, run.size());
            run.addAll(w.records());
        }
        List<FastWriteFrame.CommitRun> commit = new ArrayList<>();
        runs.forEach((stream, records) -> commit.add(new FastWriteFrame.CommitRun(stream,
                records)));
        Optional<Map<RunKey, Long>> firsts;
        try {
            firsts = batch.write(commit);
        } catch (Throwable failed) {
            // ⚠️ EACH ITS OWN EXCEPTION (its review round 1, P3), and an Error
            // rethrown once every waiter is answered.
            taken.forEach(w -> w.answer().completeExceptionally(
                    new IOException("the COMMIT failed: " + failed, failed)));
            if (failed instanceof Error fatal) {
                throw fatal;
            }
            return;
        }
        try {
            for (Waiting w : taken) {
                if (firsts.isEmpty()) {
                    w.answer().complete(Optional.empty());
                    continue;
                }
                Long first = firsts.get().get(w.stream());
                if (first == null) {
                    w.answer().completeExceptionally(new IOException(
                            "the COMMIT was answered without a run of " + w.stream()));
                    continue;
                }
                long from = first + at.get(w);
                w.answer().complete(Optional.of(new AppendResult(w.records().size(), from,
                        from + w.records().size() - 1)));
            }
        } catch (RuntimeException failed) {
            // ⚠️ NO WAITER LEFT UNANSWERED (its review round 2, P5): one
            // already answered keeps its answer.
            taken.forEach(w -> w.answer().completeExceptionally(
                    new IOException("answering the COMMIT failed: " + failed, failed)));
        }
    }
}
