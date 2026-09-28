// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.List;

/**
 * A detached batch's store work: PUT its segment, commit it, and settle its
 * waiters and its push. Runs on {@link FlushCoordinator}'s worker, without the
 * owning {@link DefaultIngest}'s lock; producers fill the replacement buffer.
 *
 * <p>Moved out of {@code DefaultIngest} unchanged by M11.24a.
 */
final class BatchFlusher {

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

    BatchFlusher(SegmentPublisher publisher, Sequencer sequencer, String podShortId,
            DurableSegmentListener durableSegmentListener, PushQueue pushQueue) {
        this.publisher = publisher;
        this.sequencer = sequencer;
        this.podShortId = podShortId;
        this.durableSegmentListener = durableSegmentListener;
        this.pushQueue = pushQueue;
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
            CommitDelta delta;
            try {
                delta = sequencer.commit(request);
            } catch (io.github.huyz0.os.biningester.sequencer.CommitDeferredException deferred) {
                // ⚠️ INTENT DURABLE (ADR-0058): a 202, no offset, nothing to push.
                batch.forEach(p -> queued.settle(
                        () -> p.done().complete(AppendResult.deferred(p.count()))));
                return;
            }

            DurableSegmentAcknowledgement.complete(published, delta, batch,
                    durableSegmentListener, queued::settle);
            // ⚠️ Submitted only after BOTH objects are durable, and only after
            // the waiters are released: append promises DURABILITY, and a
            // consumer told about a segment it cannot GET would fail its read.
            // ⚠️ AT FLUSH TIME, not at push time: the pusher runs off this lock
            // and can lag, so reading it there labels a push with the chain's
            // LATER epoch (M5.15d). READ HERE and handed to the settlement,
            // which runs after the acks (M10.13), so the offer cannot re-read it.
            long epoch = sequencer.epoch();
            queued.settle(() -> pushQueue.offer(delta, published.key(), published.segment(),
                    epoch));
        } catch (IOException | RuntimeException | Error e) { // an Error strands no producer
            for (DefaultIngest.Pending p : batch) {
                queued.settle(() -> p.done().completeExceptionally(e));
            }
            throw e;
        }
    }
}
