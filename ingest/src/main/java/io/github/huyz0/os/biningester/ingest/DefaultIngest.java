// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * The production composition behind {@link Ingest}: accumulate across producers,
 * build ONE segment per flush, PUT it, commit, publish — and return to each
 * caller only once its own records are durable.
 *
 * <p>⚠️ MANY APPENDS SHARE ONE SEGMENT. This is the architecture, not an
 * optimisation: request rate scales with segments, AZs and nodes, never with
 * records, shards, partitions or indices (non-negotiable 6). A flush happens
 * when {@link Accumulator} says it is due — the interval or the size trigger of
 * {@link IngestConfig} — so a pod ingesting from a thousand producers still
 * spends two requests per interval, not two per request.
 *
 * <p>⚠️ An earlier draft of this class flushed on EVERY append. That is exactly
 * the configuration {@code IngestConfig} refuses to be constructed with
 * ({@code flushInterval == 0}, "one PUT per record — the shape non-negotiable 6
 * forbids outright"), and it made every field of the config inert. Recorded
 * because the code looked correct and cost 2 requests per append.
 *
 * <p>⚠️ BLOCKING, because {@code append} returns when the records are DURABLE
 * (FR-4). Virtual threads make a blocking call the concurrent API, and callers
 * wait on their own future rather than on the flush lock.
 */
public final class DefaultIngest implements Ingest {

    private final IngestConfig config;
    private final SegmentPublisher publisher;
    /** Each index's share of this pod's store requests (M11.2, ADR-0077). */
    private final IndexCostLedger costLedger;
    /**
     * ⚠️ THE INGESTER CHOOSES THE FETCH MODE, NOT THE CONSUMER (FR-6). It is
     * built here, from the backend's own prices, because this is where the
     * segment's size and the queue's memory budget are both in hand -- and
     * because a consumer that could demand `direct` at fan-out 300 reproduces
     * the $3,732/month design ADR-0004 rejected.
     *
     * <p>⚠️ DERIVED, NOT CONFIGURED, AND THAT IS A LIMIT WORTH NAMING: the
     * `direct` fan-out threshold is configuration by M5.11's own acceptance
     * criteria, and nothing here lets a deployment set it. It does not matter
     * yet because `direct` is unreachable until M5.45d gives it a serving path
     * and M5.43 gives it a setting; both rows own making the dial reachable.
     */
    private final SegmentServing serving;
    private final Sequencer sequencer;
    /** The detached batch's store work, off the flush path's lock. */
    private final BatchFlusher batchFlusher;
    private final SubscriptionHub hub;
    private final StreamResolver streams;

    /**
     * How many segments' worth of records may be buffered and not yet durable
     * on a pod before an append waits (M11.7, ADR-0079).
     */
    public static final int UNFLUSHED_SEGMENTS = 4;

    /**
     * The ceiling's floor: a segment budget of a few bytes still lets the
     * next append buffer behind a flush in flight (ADR-0079).
     */
    public static final long MIN_UNFLUSHED_BYTES = 1L << 20;

    /** Delivers durable segments to subscribers, in order, off the flush path's lock. */
    private final PushQueue pushQueue;

    /** One caller waiting for the flush that will carry its records. */
    record Pending(RunKey stream, int offsetWithinRun, int count,
            CompletableFuture<AppendResult> done) {
    }

    /** The active buffer, its waiters and the flush loop (M13.1a). */
    private final FlushPath flushPath;

    /** How many pushes could not be delivered; see {@link PushQueue#undeliverable()}. */
    public long undeliverablePushes() {
        return pushQueue.undeliverable();
    }

    /**
     * How this pod serves segment bytes, so a test can see what was WIRED.
     *
     * <p>⚠️ PACKAGE-PRIVATE AND FOR THAT REASON ONLY. Without it, building the
     * proxy with no cache -- the pre-M5.40b line -- leaves every test green
     * while production issues a fresh GET for every repeat read, which is the
     * whole defect that row records. The serving path itself is reached
     * through {@code hub.publish}, never through this.
     */
    SegmentServing serving() {
        return serving;
    }

    /** How many pushes were dropped because a subscriber could not keep up. */
    public long droppedPushes() {
        return pushQueue.dropped();
    }

    /**
     * Charges this pod's data PUTs and segment GETs into {@code costLedger} --
     * the one the composition root also hands its commit-charging store
     * (M11.22), so a pod has one ledger.
     *
     * <p>⚠️ THE ONLY CONSTRUCTOR, AND IT TAKES THE LEDGER (M12.1, M11 review
     * F2): an overload that made its own charged a ledger nothing reads, so a
     * path built with it issued requests no report could see.
     */
    public DefaultIngest(IngestConfig config, BinStore store, String prefix, String podShortId,
            Sequencer sequencer, SubscriptionHub hub, Clock clock, StreamResolver streams,
            DurableSegmentListener durableSegmentListener, IndexCostLedger costLedger)
            throws IOException {
        this.costLedger = Objects.requireNonNull(costLedger, "costLedger");
        this.config = Objects.requireNonNull(config, "config");
        this.publisher = new SegmentPublisher(Objects.requireNonNull(store, "store"),
                Objects.requireNonNull(prefix, "prefix"),
                Objects.requireNonNull(podShortId, "podShortId"), costLedger);
        this.serving = SegmentServing.forPod(config, store, costLedger);
        // ⚠️ WRAPPED HERE, ONCE, so no flush path can reach the bare seam and
        // skip the resend -- M5.69 moved the policy out of this class and the
        // wrapping is what keeps it un-bypassable. `ResendOnceSequencer` says
        // what it costs and why it is exactly once.
        this.sequencer = new ResendOnceSequencer(
                Objects.requireNonNull(sequencer, "sequencer"));
        this.hub = Objects.requireNonNull(hub, "hub");
        this.streams = Objects.requireNonNull(streams, "streams");
        Objects.requireNonNull(durableSegmentListener, "durableSegmentListener");

        // ⚠️ NO RECOVERY HERE ANY MORE, and the startup-latency note that used
        // to sit here moved WITH the responsibility to `LocalSequencer.start`.
        // Establishing where a chain ended is the SEQUENCER's, because only it
        // knows which epoch it may write to.

        this.flushPath = new FlushPath(config, Objects.requireNonNull(clock, "clock"),
                this::flushBatch);
        this.pushQueue = new PushQueue(hub, serving, config.maxQueuedPushBytes());
        this.batchFlusher = new BatchFlusher(publisher, this.sequencer, podShortId,
                durableSegmentListener, pushQueue);
    }

    @Override
    public AppendResult append(Principal principal, String index, int partition,
            RecordSource records) throws IOException {
        return append(principal, index, partition, (byte) 0, records);
    }

    /**
     * {@inheritDoc}
     *
     * <p>⚠️ THE LANE IS CHECKED BEFORE ANYTHING IS BUFFERED, against this pod's
     * active set (ADR-0074): a refusal after the first record was added would
     * leak the refused records into the next flush.
     */
    @Override
    public AppendResult append(Principal principal, String index, int partition, byte lane,
            RecordSource records) throws IOException {
        return append(principal, index, partition, lane, records, () -> { });
    }

    /**
     * ⚠️ {@code buffered} RUNS ONCE THE RECORDS ARE IN THE ACCUMULATOR AND THE
     * LOCK IS RELEASED, before the durable wait (M11.7) -- and only then: an
     * append refused before it buffered anything never runs it, and its caller
     * releases what it holds on the way out.
     */
    @Override
    public AppendResult append(Principal principal, String index, int partition, byte lane,
            RecordSource records, Runnable buffered) throws IOException {
        Objects.requireNonNull(buffered, "buffered");
        Objects.requireNonNull(principal, "principal");
        if (!acceptsLane(lane)) {
            throw new PlacementRefusedException("lane " + lane + " is not active on this "
                    + "ingester; the active lanes are " + config.lanes());
        }
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(records, "records");
        if (flushPath.isClosed()) {
            throw new IOException("this ingester is closed");
        }
        // ⚠️ THE TRUST DOMAIN, not just the index. Both halves are in hand only
        // here, and ADR-0021 makes the domain the boundary: segments are never
        // bundled across domains. Without this a principal of domain B whose
        // allow-list happens to name `logs` would have its records land in
        // domain A's prefix, segment, commit log and subscriber pushes.
        if (!config.trustDomain().equals(principal.trustDomain())) {
            throw new IllegalArgumentException("the principal belongs to another trust domain");
        }
        if (!principal.canWriteTo(index)) {
            throw new IllegalArgumentException("the principal may not write to this index");
        }
        // ⚠️ Nothing above buffers anything. A refusal that had already added to
        // the accumulator would leak the refused records into the next flush.
        RunKey stream = new RunKey(streams.streamIdOf(index), partition);
        Pending mine = flushPath.buffer(stream, lane, records);

        buffered.run();
        // ⚠️ Waited on OUTSIDE the lock, so a producer whose flush is not yet due
        // does not hold every other producer out of the accumulator.
        try {
            return mine.done().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    /**
     * ⚠️ REFUSED AS THE PLAIN ROUTED FORM IS: this ingester has no catalog, so it
     * cannot compute a partition from a routing value ({@link RoutedIngest} can).
     * {@code buffered} runs after the refusal, as it always ran here (M12.2).
     */
    @Override
    public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            byte lane, RecordSource records, Runnable buffered) throws IOException {
        try {
            return appendRouted(principal, indexOrAlias, routing, lane, records);
        } finally {
            buffered.run();
        }
    }

    /** No catalog, so no alias: every name is its own index (M12.2). */
    @Override
    public String concreteIndex(String indexOrAlias) {
        return indexOrAlias;
    }

    @Override
    public boolean acceptsLane(byte lane) {
        return config.lanes().contains(lane);
    }

    /** Flushes whatever is buffered, whether or not the trigger says it is due. */
    public void flushNow() throws IOException {
        flushPath.flushNow();
    }

    /**
     * Runs {@code onRelease} when the {@code index}-th waiting caller is
     * released, for M10.13's test that no producer is released while its
     * batch is still queued; {@link #flushQueued()} is what it reads.
     */
    void whenReleased(int index, Runnable onRelease) {
        flushPath.whenReleased(index, onRelease);
    }

    /** Whether a detached batch is still queued or in flight. */
    boolean flushQueued() {
        return flushPath.flushQueued();
    }

    /** Whether an append is parked waiting for room under the unflushed ceiling (M12.3). */
    boolean waitingForRoom() {
        return flushPath.waitingForRoom();
    }

    /** How many callers are waiting for the next flush. */
    int pendingAppends() {
        return flushPath.pendingAppends();
    }

    /** Runs off the flush path's lock; see {@link BatchFlusher#flush}. */
    private void flushBatch(FlushCoordinator.Batch queued) throws IOException {
        batchFlusher.flush(queued);
    }

    /**
     * The flush spacing in force, in millis, for the cost governor (M10.11,
     * ADR-0075 §3): the ACTIVE buffer's {@code max(floor, adaptiveInterval ÷
     * 2^L)}, L the highest positive lane it holds (M10.7), so a +2 trickle's
     * four flushes per ceiling are expected rather than read as a regression.
     * ⚠️ AND THE SMALLER OF THAT AND THE BATCH IN FLIGHT's, captured when it
     * was detached: the governor samples this AT the data PUT, when the active
     * buffer is empty and the detached one drained, so the active buffer alone
     * answered the ceiling for every +2 flush (M10.7 review). Cleared when the
     * flush completes. ⚠️ Not the detached buffer's LIVE interval, which has
     * already adapted past the one that made the flush due.
     */
    public long flushSpacingMillis() {
        return flushPath.flushSpacingMillis();
    }

    /** Each index's apportioned share of this pod's store requests (ADR-0077). */
    public IndexCostLedger costLedger() {
        return costLedger;
    }

    /** The shared serving proxy, for the composition root's cache prefetcher. */
    public SegmentProxy segmentProxy() {
        return serving.proxy();
    }

    @Override
    public void close() throws IOException {
        flushPath.markClosed();
        // ⚠️ HELD so the `finally` can suppress against it rather than replace it;
        // a bare `finally` cannot see the exception in flight. `Throwable`, not
        // `IOException`, and the difference is reachable: an UncheckedIOException
        // out of the flush would leave an IOException-typed field null and the
        // release failure would replace it -- the same masking, one type narrower.
        Throwable flushFailure = null;
        try {
            // ⚠️ The LAST flush goes through the same path as every other one; a
            // second copy of publish/commit/push here is how the shutdown path --
            // the one whose failure loses the final segment -- drifts out of
            // step with the path that is actually tested.
            flushNow();
        } catch (Throwable failed) {
            flushFailure = failed;
            throw failed;
        } finally {
            // ⚠️ Anything that slipped in between is FAILED, never left waiting.
            flushPath.failRemaining();
            pushQueue.drain();
            // ⚠️ RELEASES THE LEASE, and doing it here rather than leaving it to
            // the caller is the point. The Sequencer contract calls close "the
            // difference between a failover in milliseconds and one bounded by
            // the lease TTL" (ADR-0007 puts that at ~10 s worst case) -- and
            // this method IS the pod shutting down.
            // ⚠️ AFTER the final flush, never before: that flush commits through
            // the sequencer, and closing first would fail the very segment the
            // shutdown path exists to save.
            //
            // ⚠️ SUPPRESSED, NEVER SUBSTITUTED. Everything else in this `finally`
            // is non-throwing by construction -- `drainPushes()` declares no
            // checked exception for exactly that reason, and its javadoc below
            // records this class already paying for the defect once. A release is
            // I/O and CAN fail (a stale CAS version after a self-fence, a 503 at
            // shutdown), so bare here it would REPLACE `flushNow()`'s exception:
            // "could not release the lease", reported for a pod that just lost
            // its final segment. A lost segment always wins the report.
            try {
                sequencer.close();
            } catch (IOException releaseFailed) {
                if (flushFailure != null) {
                    flushFailure.addSuppressed(releaseFailed);
                } else {
                    throw releaseFailed;
                }
            }
        }
    }

}
