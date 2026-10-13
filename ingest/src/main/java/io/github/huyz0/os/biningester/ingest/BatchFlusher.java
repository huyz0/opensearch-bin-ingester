// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A detached batch's store work: PUT its segment, commit it, and settle its
 * waiters and its push. Runs on {@link FlushCoordinator}'s worker, without the
 * owning {@link DefaultIngest}'s lock; producers fill the replacement buffer.
 *
 * <p>Moved out of {@code DefaultIngest} unchanged by M11.24a.
 */
final class BatchFlusher {

    private static final System.Logger LOG = System.getLogger(BatchFlusher.class.getName());

    private final SegmentPublisher publisher;
    private final Sequencer sequencer;
    private final String podShortId;
    /**
     * ⚠️ Half the idempotency key, with {@code podId} (M4.10). A plain
     * {@code long}, not an atomic, because every increment happens inside the
     * drain that the single {@link FlushCoordinator} already serialises.
     */
    private long flushSeq;
    // ⚠️ ONCE PER INSTANCE, never per flush (ADR-0036). A new process is a new
    // incarnation by construction -- no clock, no coordination -- which is what
    // lets a replay be told from a restart when `flushSeq` restarts at 0 and
    // `podShortId` does not. Minting this inside the flush worker keeps
    // every sequencer-seam suite green, and makes dedup a no-op in production.
    private final String incarnationId = java.util.UUID.randomUUID().toString();
    private final DurableSegmentListener durableSegmentListener;
    private final PushQueue pushQueue;
    /** Where a redirected run's records are re-sent (M13.34l): the fast route's path. */
    private final java.util.function.Supplier<FastPath> fastPath;
    BatchFlusher(SegmentPublisher publisher, Sequencer sequencer, String podShortId,
            DurableSegmentListener durableSegmentListener, PushQueue pushQueue,
            java.util.function.Supplier<FastPath> fastPath) {
        this.publisher = publisher;
        this.sequencer = sequencer;
        this.podShortId = podShortId;
        this.durableSegmentListener = durableSegmentListener;
        this.pushQueue = pushQueue;
        this.fastPath = fastPath;
    }

    /** Runs without the ingester's lock; producers fill the replacement buffer. */
    void flush(FlushCoordinator.Batch queued) throws IOException {
        List<DefaultIngest.Pending> batch = queued.pending();
        try {
            SegmentPublisher.Published published = publisher.publish(queued.accumulator())
                    .orElseThrow(() -> new IOException(
                            "the accumulator produced no segment for a non-empty batch"));
            // ⚠️ BUILT ONCE, SO A RETRY CARRIES THE SAME TRIPLE -- the only thing
            // that lets the sequencer answer one instead of committing it twice
            // (M5.2, M5.32). `flushSeq++` stays inside this constructor call.
            CommitRequest request = new CommitRequest(podShortId, incarnationId, flushSeq++,
                    published.key(), published.recordCounts());
            // ⚠️ M13.34l: ADR-0082 §7's answer -- the deltas that committed the
            // request's runs, and the runs the leader redirected to the fast path.
            CommitAnswer answer;
            try {
                answer = sequencer.commitAnswering(request);
            } catch (io.github.huyz0.os.biningester.sequencer.CommitDeferredException deferred) {
                // ⚠️ INTENT DURABLE (ADR-0058): a 202, no offset, nothing to push.
                batch.forEach(p -> queued.settle(
                        () -> p.done().complete(AppendResult.deferred(p.count()))));
                return;
            }

            List<DefaultIngest.Pending> redirected = new java.util.ArrayList<>();
            DurableSegmentAcknowledgement.complete(published, answer, batch,
                    durableSegmentListener, queued::settle, redirected::add);
            // ⚠️ Submitted only after BOTH objects are durable, and only after
            // the waiters are released: append promises DURABILITY, and a
            // consumer told about a segment it cannot GET would fail its read.
            // ⚠️ AT FLUSH TIME, not at push time: the pusher runs off this lock
            // and can lag, so reading it there labels a push with the chain's
            // LATER epoch (M5.15d). READ HERE and handed to the settlement,
            // which runs after the acks (M10.13), so the offer cannot re-read it.
            long epoch = sequencer.epoch();
            for (CommitDelta delta : answer.deltas()) { // ⚠️ one push per delta
                queued.settle(() -> pushQueue.offer(delta, published.key(),
                        published.segment(), epoch));
            }
            // ⚠️ THE LAST SETTLEMENT (M13.34n review round 1, P1): the committed
            // appends are acked and the deltas pushed first, never behind a
            // redirect's quorum; then each redirected append is re-sent, in
            // arrival order, before the next batch -- its stream's appends keep
            // their order, and a close's last flush leaves none in flight. A
            // later batch waits behind them, milliseconds, while a writer's
            // registration disagrees with its leader's catalog.
            if (!redirected.isEmpty()) {
                queued.settle(() -> resendFast(published, redirected));
            }
        } catch (IOException | RuntimeException | Error e) { // an Error strands no producer
            for (DefaultIngest.Pending p : batch) {
                queued.settle(() -> p.done().completeExceptionally(e));
            }
            throw e;
        }
    }

    /**
     * Each redirected run's append re-sent through the fast path (M13.34l;
     * ADR-0081 §8: "the writer re-sends those records by COMMIT"): exactly its
     * records, read back from the segment this flush published -- each run
     * decoded once -- and acked at the offsets its fast write is answered with.
     */
    private void resendFast(SegmentPublisher.Published published,
            List<DefaultIngest.Pending> redirected) {
        Map<RunKey, List<SegmentRecord>> runs = new HashMap<>();
        for (DefaultIngest.Pending pending : redirected) {
            try {
                List<SegmentRecord> run = runs.get(pending.stream());
                if (run == null) {
                    SegmentReader reader = SegmentReader.open(published.segment());
                    run = reader.read(reader.find(pending.stream()).orElseThrow(
                            () -> new IOException("the published segment holds no run of "
                                    + pending.stream())));
                    runs.put(pending.stream(), run);
                }
                java.util.Optional<AppendResult> fast = fastPath.get().append(pending.stream(),
                        run.subList(pending.offsetWithinRun(),
                                pending.offsetWithinRun() + pending.count()));
                if (fast.isPresent()) {
                    pending.done().complete(fast.get());
                } else {
                    pending.done().completeExceptionally(new IOException("stream "
                            + pending.stream() + " was redirected to the fast path, which"
                            + " answered NOT_FAST: nothing was committed, retry"));
                }
            } catch (Throwable failed) {
                // ⚠️ FAILED, LOGGED, NEVER RETHROWN (its review round 2, P4): this
                // is a settlement on the flush worker, and an Error escaping it
                // ends the worker -- no batch flushed again, every producer
                // stranded. The append is failed with it, the rest answered.
                pending.done().completeExceptionally(failed);
                LOG.log(System.Logger.Level.ERROR, "a redirected append of "
                        + pending.stream() + " failed on its fast path", failed);
            }
        }
    }
}
