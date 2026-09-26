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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
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
    private Accumulator accumulator;
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
     * <p>⚠️ It does NOT cover the push — see {@link #pushes}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /** ⚠️ Replaces a 1 ms poll that woke 1000x/s and took this lock each time. */
    private final Condition work = lock.newCondition();

    /**
     * ⚠️ Pushes run OFF the ingest lock, on one thread so they stay ordered.
     * {@link SubscriptionHub} calls {@code sink.accept} synchronously and its own
     * contract says "a slow or dead subscriber must not block a commit" — it
     * isolates a subscriber that throws, not one that is slow. Publishing under
     * the lock would let one consumer's slow sink stall every producer on the
     * pod; publishing from many threads would let the push for offset 10
     * overtake the push for offset 5.
     *
     * <p>⚠️ ONE DEDICATED THREAD, not a single-thread executor: java-style
     * rule 4 forbids pooling virtual threads.
     *
     * <p>⚠️ The bound is BYTES ({@code maxQueuedPushBytes}, rule 7), enforced
     * when a push is offered — not a count of entries. An earlier version
     * bounded the COUNT and derived its byte figure from {@code maxSegmentBytes},
     * which is wrong: that is a flush TRIGGER checked only after a caller's whole
     * record list is buffered, so one 32 MiB `_bulk` body makes one ~32 MiB
     * segment and eight of them would have retained ~256 MiB — the very
     * OutOfMemoryError the bound exists to prevent.
     *
     * <p>⚠️ Past the budget a push is DROPPED and COUNTED. In M1 that is record
     * LOSS for the subscriber, not lag: nothing yet detects an offset gap or
     * falls back to the commit log. That is M1.6d; {@link #droppedPushes()} is
     * the only signal until it lands.
     */
    private final LinkedBlockingQueue<PendingPush> pushes = new LinkedBlockingQueue<>();
    private final AtomicLong queuedPushBytes = new AtomicLong();
    private final Thread pusher;

    /** One caller waiting for the flush that will carry its records. */
    record Pending(RunKey stream, int offsetWithinRun, int count,
            CompletableFuture<AppendResult> done) {
    }

    private final List<Pending> pending = new ArrayList<>();
    private final Map<RunKey, Integer> bufferedPerStream = new HashMap<>();
    private final Thread flusher;
    private final FlushCoordinator flushes;
    private volatile boolean closed;
    private long droppedPushes;
    private final AtomicLong undeliverablePushes = new AtomicLong();

    /**
     * One queued delivery; the shutdown sentinel is the instance below.
     *
     * <p>⚠️ THE SEGMENT IS ALWAYS ATTACHED, whatever mode it is served under.
     * The mode governs how the bytes reach each SUBSCRIBER; this pod wrote
     * them and holding one copy costs one copy, bounded by
     * {@code maxQueuedPushBytes}.
     */
    private record PendingPush(CommitDelta delta, String segmentKey, byte[] segment, int bytes,
            long sequencerEpoch) {
    }

    /** Ends {@link #pushLoop} without interrupting a delivery in flight. */
    private static final PendingPush POISON =
            new PendingPush(null, null, new byte[0], 0, SubscriptionHub.EPOCH_UNKNOWN);

    /**
     * How many pushes {@link #pushLoop} could not deliver.
     *
     * <p>⚠️ A DIFFERENT CAUSE FROM {@link #droppedPushes()}, and two counters
     * on purpose. That one is BACK-PRESSURE -- the queue budget was full, the
     * subscriber is behind, and the answer is to slow down or raise the budget.
     * This one is a delivery that THREW -- any {@link RuntimeException} out of
     * {@link SubscriptionHub#publish}, which today is a segment that could not
     * be read and is not limited to that -- so the count alone does not say
     * where to look and the log line beside the increment quotes the cause.
     * Collapsing the two into one number would make the metric unactionable,
     * which is the objection to the silence this counter ends.
     *
     * <p>⚠️ NO SEGMENT KEY HERE. observability.md rule 1 names an object key
     * among the things that are never a label, so WHICH segment failed goes in
     * the log line beside the increment and only the count is exported.
     */
    public long undeliverablePushes() {
        return undeliverablePushes.get();
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
        lock.lock();
        try {
            return droppedPushes;
        } finally {
            lock.unlock();
        }
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
        this.pusher = Thread.ofVirtual().name("binstore-push").start(this::pushLoop);
        this.flusher = Thread.ofVirtual().name("binstore-flush").start(this::flushLoop);
    }

    @Override
    public AppendResult append(Principal principal, String index, int partition,
            RecordSource records) throws IOException {
        Objects.requireNonNull(principal, "principal");
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
                accumulator.add(stream, record);
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

    /** Delivers pushes in order, off the ingest lock. */
    private void pushLoop() {
        while (true) {
            PendingPush next;
            try {
                next = pushes.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (next == POISON) {
                return;
            }
            try {
                hub.publish(next.delta(), next.segmentKey(), next.segment(), serving,
                        next.sequencerEpoch());
            } catch (RuntimeException e) {
                // ⚠️ A subscriber's failure is its own. SubscriptionHub already
                // isolates a sink that throws; this is the backstop that keeps
                // one bad sink from ending delivery for every other consumer.
                //
                // ⚠️ BUT IT IS NO LONGER SILENT, which is M5.48. Until this
                // counter and this line existed, a store outage and a quiet
                // window were indistinguishable from outside the process:
                // `droppedPushes` counts the OTHER drop, the queue budget, so a
                // reader of it was told about back-pressure and told nothing
                // about a failed read.
                //
                // ⚠️ THE KEY GOES IN THE LOG AND NOT IN A LABEL. An operator
                // needs to know WHICH segment; observability.md rule 1 names an
                // object key among the things that are never a metric label, so
                // the counter carries the count and the line carries the name.
                undeliverablePushes.incrementAndGet();
                // ⚠️ THE CAUSE NAMES THE FAILING SEGMENT, not this frame. The
                // only key here is `next.segmentKey()`, the object this pod just
                // PUT and the store therefore holds; a read only ever happens
                // for a segment the pod does NOT hold, so naming this one would
                // send an operator to a healthy object and a wrong conclusion.
                LOG.log(System.Logger.Level.WARNING,
                        "push undelivered: " + e.getMessage()
                                + " -- this pod's own segment for that commit was "
                                + next.segmentKey()
                                + "; the commit is durable and consumers recover from the "
                                + "commit log", e);
                continue;
            } finally {
                queuedPushBytes.addAndGet(-next.bytes());
            }
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
            ChainPublisher chained = chain;
            if (chained != null) {
                // ⚠️ BEFORE THE COMMIT (ADR-0075): the push for it can arrive
                // before this call returns, and must find the bytes.
                chained.hold(published.key(), published.segment());
            }
            CommitDelta delta;
            try {
                delta = sequencer.commit(request);
            } catch (io.github.huyz0.os.biningester.sequencer.CommitDeferredException deferred) {
                // ⚠️ INTENT DURABLE (ADR-0058): a 202, no offset, nothing to push.
                if (chained != null) {
                    chained.forget(published.key());
                }
                batch.forEach(p -> p.done().complete(AppendResult.deferred(p.count())));
                return;
            } catch (IOException | RuntimeException failed) {
                if (chained != null) {
                    chained.forget(published.key());
                }
                throw failed;
            }

            DurableSegmentAcknowledgement.complete(published, delta, batch,
                    durableSegmentListener);
            if (chained != null) {
                return; // the leaseholder's publication, not this flush, delivers it
            }
            // ⚠️ Submitted only after BOTH objects are durable, and only after
            // the waiters are released: append promises DURABILITY, and a
            // consumer told about a segment it cannot GET would fail its read.
            // ⚠️ THE ARRAY IS QUEUED WHATEVER MODE IS CHOSEN, and an earlier
            // draft of this line dropped it for `proxy` on the theory that
            // holding it was the memory cost the mode exists to avoid. That is
            // WRONG IN BOTH DIRECTIONS. It buys nothing against SPEC criterion
            // 6, whose bound is flat in the CONSUMER count K and not in the
            // queue's depth -- one array serves every subscriber either way,
            // and `maxQueuedPushBytes` already bounds the depth. And it costs a
            // GET PER SEGMENT for bytes this pod wrote and still holds, which
            // is a request bought for nothing.
            //
            // ⚠️ SO THE MODE IS ABOUT THE HAND-OFF, NOT ABOUT POSSESSION:
            // `inline` gives each subscriber the whole segment at once,
            // `proxy` gives it a chunk at a time. Reading the store is for
            // bytes this pod does NOT hold -- another pod's segment, or a late
            // subscriber after the array is gone, which is M5.16's prefetch.
            int bytes = published.segment().length;
            if (queuedPushBytes.get() + bytes > config.maxQueuedPushBytes()) {
                // ⚠️ Dropped, not blocked: blocking here would put a slow
                // subscriber back on the commit path by another route. ⚠️ And in
                // M1 a drop is record LOSS for that subscriber, not lag --
                // nothing yet detects an offset gap or falls back to the log.
                // That is M1.6d; this counter is the only signal until it lands.
                lock.lock();
                try {
                    droppedPushes++;
                } finally {
                    lock.unlock();
                }
            } else {
                queuedPushBytes.addAndGet(bytes);
                pushes.add(new PendingPush(delta, published.key(), published.segment(), bytes,
                        // ⚠️ AT FLUSH TIME, not at push time: the pusher runs
                        // off this lock and can lag, so reading it there labels
                        // a push with the chain's LATER epoch (M5.15d).
                        sequencer.epoch()));
            }
        } catch (IOException | RuntimeException e) {
            for (Pending p : batch) {
                p.done().completeExceptionally(e);
            }
            throw e;
        }
    }

    /**
     * Stops this ingest publishing its own commits and returns the publisher
     * that will, fed by the leaseholder in chain order (M10.18, ADR-0075).
     * Holds at most {@code maxHeldBytes} of this pod's own segments; call once,
     * before writes. The caller closes it, AFTER this ingest: the final flush
     * still holds bytes for a push that may arrive.
     */
    public ChainPublisher publishThroughChain(long maxHeldBytes) {
        ChainPublisher created = new ChainPublisher(hub, serving, maxHeldBytes);
        chain = created;
        return created;
    }

    private volatile ChainPublisher chain;

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
            drainPushes();
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

    /**
     * ⚠️ A SHORT wait, and an IOException rather than an UncheckedIOException.
     * The old 30 s exceeded Kubernetes' default grace period, so a slow
     * subscriber turned a graceful drain into a SIGKILL for data that was
     * already durable — and an unchecked throw from a {@code finally} escaped
     * every {@code catch (IOException)} while masking a failed final flush.
     */
    private void drainPushes() {
        // ⚠️ POISON is queued BEHIND the pushes already waiting, so shutting
        // down DELIVERS them rather than abandoning them.
        //
        // ⚠️ An earlier version wrote `while (!pushes.offer(POISON)) pushes.poll();`
        // against a COUNT-bounded queue. poll() removes from the HEAD, so on a
        // full queue that silently discarded a queued push -- uncounted -- which
        // is the exact opposite of what the comment claimed. The queue is
        // unbounded in count now (the budget is BYTES, enforced at offer time),
        // so the sentinel always lands last and nothing is displaced.
        pushes.add(POISON);
        try {
            // ⚠️ FIVE seconds, not thirty. The old wait exceeded Kubernetes'
            // default grace period, so a slow subscriber turned a graceful drain
            // into a SIGKILL -- for data that is already durable by this point.
            if (!pusher.join(Duration.ofSeconds(5))) {
                pusher.interrupt();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pusher.interrupt();
        }
    }
}
