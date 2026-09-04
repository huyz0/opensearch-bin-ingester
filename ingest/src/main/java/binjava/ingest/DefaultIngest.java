// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.security.Principal;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.Sequencer;
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

    private final IngestConfig config;
    private final Accumulator accumulator;
    private final SegmentPublisher publisher;
    private final Sequencer sequencer;
    private final String podShortId;
    /**
     * ⚠️ Half the idempotency key, with {@code podId} (M4.10). A plain
     * {@code long}, not an atomic, because every increment happens inside the
     * drain that {@code lock} already serialises — an atomic here would imply a
     * concurrency this class deliberately excludes.
     */
    private long flushSeq;
    private final SubscriptionHub hub;
    private final StreamResolver streams;

    /**
     * ⚠️ Covers accumulate + drain + PUT + commit as one step: the segment, its
     * commit and the offsets it assigns must agree, and two threads draining
     * concurrently would each PUT a segment and then race in the commit log,
     * leaving the loser's offsets describing bytes it did not write.
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
    private record Pending(RunKey stream, int offsetWithinRun, int count,
            CompletableFuture<AppendResult> done) {
    }

    private final List<Pending> pending = new ArrayList<>();
    private final Map<RunKey, Integer> bufferedPerStream = new HashMap<>();
    private final Thread flusher;
    private volatile boolean closed;
    private long droppedPushes;

    /** One queued delivery; the shutdown sentinel is the instance below. */
    private record PendingPush(CommitDelta delta, byte[] segment, int bytes) {
    }

    /** Ends {@link #pushLoop} without interrupting a delivery in flight. */
    private static final PendingPush POISON = new PendingPush(null, new byte[0], 0);

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
        this.config = Objects.requireNonNull(config, "config");
        this.accumulator = new Accumulator(config, Objects.requireNonNull(clock, "clock"));
        this.publisher = new SegmentPublisher(Objects.requireNonNull(store, "store"),
                Objects.requireNonNull(prefix, "prefix"),
                Objects.requireNonNull(podShortId, "podShortId"));
        this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
        this.podShortId = podShortId;
        this.hub = Objects.requireNonNull(hub, "hub");
        this.streams = Objects.requireNonNull(streams, "streams");

        // ⚠️ NO RECOVERY HERE ANY MORE, and the startup-latency note that used
        // to sit here moved WITH the responsibility to `LocalSequencer.start`.
        // Establishing where a chain ended is the SEQUENCER's, because only it
        // knows which epoch it may write to.

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
                flushLocked();
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
    void flushNow() throws IOException {
        lock.lock();
        try {
            flushLocked();
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
                if (!pending.isEmpty() && accumulator.isFlushDue()) {
                    try {
                        flushLocked();
                    } catch (IOException | RuntimeException e) {
                        // ⚠️ CATCH EVERYTHING and keep looping. flushLocked has
                        // already completed this batch's waiters exceptionally,
                        // so nothing is stranded -- but a loop that DIED here
                        // would strand every LATER append: flushes would then
                        // happen only when a new append happened to find the
                        // trigger true, so a producer that appends once and goes
                        // quiet would block forever. An earlier version caught
                        // only InterruptedException and IOException while
                        // flushLocked rethrows RuntimeException, so a store
                        // backend throwing IllegalStateException killed the
                        // timer silently and permanently.
                        continue;
                    }
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
                hub.publish(next.delta(), next.segment());
            } catch (RuntimeException e) {
                // ⚠️ A subscriber's failure is its own. SubscriptionHub already
                // isolates a sink that throws; this is the backstop that keeps
                // one bad sink from ending delivery for every other consumer.
                continue;
            } finally {
                queuedPushBytes.addAndGet(-next.bytes());
            }
        }
    }

    /**
     * ⚠️ Caller holds {@link #lock}. Completes every waiting append with the
     * slice of the run that its own records occupy.
     */
    private void flushLocked() throws IOException {
        if (pending.isEmpty()) {
            return;
        }
        List<Pending> batch = List.copyOf(pending);
        pending.clear();
        bufferedPerStream.clear();
        try {
            SegmentPublisher.Published published = publisher.publish(accumulator)
                    .orElseThrow(() -> new IOException(
                            "the accumulator produced no segment for a non-empty batch"));
            CommitDelta delta = sequencer.commit(new CommitRequest(podShortId, flushSeq++,
                    published.key(), published.recordCounts()));

            Map<RunKey, Long> firstOffsets = new HashMap<>();
            for (RunCommit run : delta.runs()) {
                firstOffsets.put(run.key(), run.firstOffset());
            }
            for (Pending p : batch) {
                Long base = firstOffsets.get(p.stream());
                if (base == null) {
                    p.done().completeExceptionally(new IOException(
                            "the commit did not carry the stream this append wrote"));
                    continue;
                }
                // ⚠️ The caller's SLICE of the run, not the whole run. Several
                // appends to one stream share a flush, so each gets the range it
                // actually contributed -- returning the run's own count would
                // trip AppendResult's contiguity invariant the moment two
                // appends batch together.
                long first = base + p.offsetWithinRun();
                p.done().complete(new AppendResult(p.count(), first, first + p.count() - 1));
            }
            // ⚠️ Submitted only after BOTH objects are durable, and only after
            // the waiters are released: append promises DURABILITY, and a
            // consumer told about a segment it cannot GET would fail its read.
            int bytes = published.segment().length;
            if (queuedPushBytes.get() + bytes > config.maxQueuedPushBytes()) {
                // ⚠️ Dropped, not blocked: blocking here would put a slow
                // subscriber back on the commit path by another route. ⚠️ And in
                // M1 a drop is record LOSS for that subscriber, not lag --
                // nothing yet detects an offset gap or falls back to the log.
                // That is M1.6d; this counter is the only signal until it lands.
                droppedPushes++;
            } else {
                queuedPushBytes.addAndGet(bytes);
                pushes.add(new PendingPush(delta, published.segment(), bytes));
            }
        } catch (IOException | RuntimeException e) {
            for (Pending p : batch) {
                p.done().completeExceptionally(e);
            }
            throw e;
        }
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
