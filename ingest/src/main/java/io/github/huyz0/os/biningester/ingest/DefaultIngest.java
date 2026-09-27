// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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

    private static final System.Logger LOG =
            System.getLogger(DefaultIngest.class.getName());

    private final IngestConfig config;
    /** ⚠️ Volatile for {@link #flushSpacingMillis()}, read off-lock; writers hold it. */
    private volatile Accumulator accumulator;
    private final SegmentPublisher publisher;
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
    private final SubscriptionHub hub;
    private final StreamResolver streams;
    private final DurableSegmentListener durableSegmentListener;

    /**
     * ⚠️ Serializes active-buffer mutation and flush detachment. The detached
     * accumulator's PUT and commit run on {@link FlushCoordinator}, whose
     * single queue preserves segment/offset order without holding this lock
     * across object-store I/O.
     *
     * <p>⚠️ It does NOT cover the push — see {@link PushQueue}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /** ⚠️ Replaces a 1 ms poll that woke 1000x/s and took this lock each time. */
    private final Condition work = lock.newCondition();


    /** Delivers durable segments to subscribers, in order, off {@link #lock}. */
    private final PushQueue pushQueue;

    /** One caller waiting for the flush that will carry its records. */
    record Pending(RunKey stream, int offsetWithinRun, int count,
            CompletableFuture<AppendResult> done) {
    }

    private final List<Pending> pending = new ArrayList<>();
    private final Map<RunKey, Integer> bufferedPerStream = new HashMap<>();
    private final Thread flusher;
    private final FlushCoordinator flushes;
    private volatile boolean closed;


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

    public DefaultIngest(IngestConfig config, BinStore store, String prefix, String podShortId,
            Sequencer sequencer, SubscriptionHub hub, Clock clock, StreamResolver streams)
            throws IOException {
        this(config, store, prefix, podShortId, sequencer, hub, clock, streams, ignored -> { });
    }

    public DefaultIngest(IngestConfig config, BinStore store, String prefix, String podShortId,
            Sequencer sequencer, SubscriptionHub hub, Clock clock, StreamResolver streams,
            DurableSegmentListener durableSegmentListener) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.accumulator = new Accumulator(config, Objects.requireNonNull(clock, "clock"));
        this.publisher = new SegmentPublisher(Objects.requireNonNull(store, "store"),
                Objects.requireNonNull(prefix, "prefix"),
                Objects.requireNonNull(podShortId, "podShortId"));
        Capabilities storeCapabilities = store.capabilities();
        // ⚠️ THE STARTUP REFUSAL, AND M5.43 IS WHAT GAVE IT A CALLER. Criterion
        // 7 asks that a deployment wanting `direct` against a backend that
        // cannot sign fail at STARTUP rather than at the first fetch, and until
        // this line nothing expressed "this deployment wants direct" -- so the
        // refusal `Capabilities.requirePresignedUrls` implements had no call
        // site anywhere in the tree.
        //
        // ⚠️ CONDITIONAL, NECESSARILY. Calling it unconditionally fails every
        // pod to start on both shipping backends, neither of which presigns;
        // calling it lazily puts the refusal back at the first fetch, which is
        // what it exists to prevent.
        if (config.directEnabled()) {
            storeCapabilities.requirePresignedUrls();
        }
        this.serving = new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(
                        storeCapabilities.costs(), config.directEnabled())),
                storeCapabilities,
                // ⚠️ THE CACHE IS ON IN PRODUCTION, which is what makes
                // M5.40b a number rather than a capability. A repeat read
                // across publishes -- a late subscriber, an AZ replaying a
                // backlog -- costs no GET.
                new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES,
                        // ⚠️ FROM THE CONFIG, NOT FROM THE DEFAULT CONSTANT.
                        // A deployment that configures a larger segment than
                        // the default would otherwise exceed a fixed ceiling
                        // with EVERY segment, cache nothing, and say nothing.
                        SegmentCache.forSegmentsOf(config.maxSegmentBytes())),
                // ⚠️ ONCE PER POD, NOT ONCE PER PUBLISH. An issuer per publish
                // would allocate on the serving path for every flush, and it
                // would put the TTL ceiling's configuration in a loop rather
                // than at one site. ⚠️ AND NULL WHEN `direct` IS OFF, because
                // the constructor REFUSES a backend that cannot presign -- so
                // over a backend that cannot sign there is no issuer to hold, which
                // is the same refusal the line above already made.
                config.directEnabled() ? new GrantIssuer(store) : null);
        // ⚠️ WRAPPED HERE, ONCE, so no flush path can reach the bare seam and
        // skip the resend -- M5.69 moved the policy out of this class and the
        // wrapping is what keeps it un-bypassable. `ResendOnceSequencer` says
        // what it costs and why it is exactly once.
        this.sequencer = new ResendOnceSequencer(
                Objects.requireNonNull(sequencer, "sequencer"));
        this.podShortId = podShortId;
        this.hub = Objects.requireNonNull(hub, "hub");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.durableSegmentListener = Objects.requireNonNull(
                durableSegmentListener, "durableSegmentListener");

        // ⚠️ NO RECOVERY HERE ANY MORE, and the startup-latency note that used
        // to sit here moved WITH the responsibility to `LocalSequencer.start`.
        // Establishing where a chain ended is the SEQUENCER's, because only it
        // knows which epoch it may write to.

        this.flushes = new FlushCoordinator(lock, work, this::flushBatch,
                batch -> accumulator.adoptAdaptiveStateFrom(batch.accumulator()));
        this.pushQueue = new PushQueue(hub, serving, config.maxQueuedPushBytes());
        this.flusher = Thread.ofVirtual().name("binstore-flush").start(this::flushLoop);
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
        Objects.requireNonNull(principal, "principal");
        if (!acceptsLane(lane)) {
            throw new PlacementRefusedException("lane " + lane + " is not active on this "
                    + "ingester; the active lanes are " + config.lanes());
        }
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(records, "records");
        if (closed) {
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
        Pending mine;
        lock.lock();
        try {
            // ⚠️ CHECKED AGAIN, under the lock. The check above is outside it, so
            // a thread that passed it can still be waiting here while close()
            // runs its final flush: it would then register a Pending that
            // nothing will ever complete, and CompletableFuture.join is
            // UNINTERRUPTIBLE, so the producer hangs until SIGKILL with its
            // records stranded in a dead accumulator.
            if (closed) {
                throw new IOException("this ingester is closed");
            }
            int before = bufferedPerStream.getOrDefault(stream, 0);
            // ⚠️ A one-element array, not a local: a lambda captures effectively
            // final variables only, and this is mutated once per record as
            // `records` is consumed -- which is the whole point (java-style.md
            // rule 8): nothing here requires `records` to have a known size, or
            // any backing collection at all, before this loop starts.
            int[] count = {0};
            // ⚠️ If `records` throws partway through (a producer's body turns
            // out malformed after some valid records -- see Ingest.append's
            // javadoc), whatever this lambda already handed to `accumulator`
            // must stay reflected in `bufferedPerStream` -- updated PER RECORD
            // below, not once after `forEachRecord` returns -- or the NEXT
            // append to this same stream computes `before` from a stale count
            // and hands out a Pending whose offset slice does not match what
            // the eventual RunCommit assigns. No Pending is registered for
            // THIS append when that happens: it has already failed, so nothing
            // should be waiting on a future for it.
            records.forEachRecord(record -> {
                accumulator.add(stream, record, lane);
                count[0]++;
                bufferedPerStream.put(stream, before + count[0]);
            });
            if (count[0] == 0) {
                // ⚠️ Refused rather than flushed: an empty append that still
                // wrote a segment would spend two requests on nothing. Detected
                // HERE rather than up front -- a RecordSource does not know its
                // own size before it has been run -- but nothing has been
                // buffered for THIS append (count is 0), so there is nothing to
                // leak into the next flush.
                throw new IllegalArgumentException(
                        "an append of no records has nothing to make durable");
            }
            mine = new Pending(stream, before, count[0], new CompletableFuture<>());
            pending.add(mine);
            if (accumulator.isFlushDue()) {
                enqueueFlushLocked();
            } else {
                // ⚠️ Wakes the flusher so it re-computes its deadline rather
                // than discovering this append up to a poll interval later.
                work.signal();
            }
        } finally {
            lock.unlock();
        }

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

    @Override
    public boolean acceptsLane(byte lane) {
        return config.lanes().contains(lane);
    }

    /** Flushes whatever is buffered, whether or not the trigger says it is due. */
    public void flushNow() throws IOException {
        while (true) {
            CompletableFuture<Void> done;
            lock.lock();
            try {
                if (!pending.isEmpty() && !flushes.isQueued()) {
                    done = enqueueFlushLocked();
                } else if (flushes.isQueued()) {
                    done = flushes.currentDone();
                } else {
                    return;
                }
            } finally {
                lock.unlock();
            }
            FlushCoordinator.await(done);
        }
    }

    /**
     * Runs {@code onRelease} when the {@code index}-th waiting caller is
     * released, for M10.13's test that no producer is released while its
     * batch is still queued; {@link #flushQueued()} is what it reads.
     */
    void whenReleased(int index, Runnable onRelease) {
        lock.lock();
        try {
            pending.get(index).done().whenComplete((result, failure) -> onRelease.run());
        } finally {
            lock.unlock();
        }
    }

    /** Whether a detached batch is still queued or in flight. */
    boolean flushQueued() {
        lock.lock();
        try {
            return flushes.isQueued();
        } finally {
            lock.unlock();
        }
    }

    /** How many callers are waiting for the next flush. */
    int pendingAppends() {
        lock.lock();
        try {
            return pending.size();
        } finally {
            lock.unlock();
        }
    }

    private void flushLoop() {
        lock.lock();
        try {
            while (!closed) {
                if (!pending.isEmpty() && !flushes.isQueued() && accumulator.isFlushDue()) {
                    enqueueFlushLocked();
                    continue;
                }
                try {
                    // ⚠️ A quarter of the interval, so the trigger is noticed
                    // promptly without polling; an append signals this condition
                    // as well, so the wait is an upper bound rather than a poll.
                    work.awaitNanos(Math.max(1_000_000L, config.intervalFloor().toNanos() / 4));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            lock.unlock();
        }
    }


    /** Detaches the active buffer and queues its store work; caller holds {@link #lock}. */
    private CompletableFuture<Void> enqueueFlushLocked() {
        if (pending.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<Pending> batch = List.copyOf(pending);
        pending.clear();
        bufferedPerStream.clear();
        Accumulator detached = accumulator;
        accumulator = accumulator.emptyCopy();
        return flushes.enqueue(batch, detached);
    }

    /** Runs without {@link #lock}; producers fill the replacement buffer. */
    private void flushBatch(FlushCoordinator.Batch queued) throws IOException {
        List<Pending> batch = queued.pending();
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
        } catch (IOException | RuntimeException e) {
            for (Pending p : batch) {
                queued.settle(() -> p.done().completeExceptionally(e));
            }
            throw e;
        }
    }

    /**
     * The flush spacing in force, in millis, for the cost governor (M10.11,
     * ADR-0075 §3): the ACTIVE buffer's adaptive interval, never below the floor.
     * ⚠️ The active buffer's, not the detached one mid-flush, which has already
     * adapted past the interval that made the flush due. ⚠️ No lane term yet:
     * every record is lane 0 until M10.7's deadlines, which refine this.
     */
    public long flushSpacingMillis() {
        return Math.max(config.intervalFloor().toMillis(),
                accumulator.currentInterval().toMillis());
    }

    /** The shared serving proxy, for the composition root's cache prefetcher. */
    public SegmentProxy segmentProxy() {
        return serving.proxy();
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            closed = true;
            work.signalAll();
        } finally {
            lock.unlock();
        }
        flusher.interrupt();
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
            // join() is uninterruptible, so a Pending nobody completes is a
            // producer thread hung until SIGKILL.
            lock.lock();
            try {
                for (Pending p : pending) {
                    p.done().completeExceptionally(
                            new IOException("the ingester closed before this append was flushed"));
                }
                pending.clear();
                bufferedPerStream.clear();
            } finally {
                lock.unlock();
            }
            flushes.close();
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
