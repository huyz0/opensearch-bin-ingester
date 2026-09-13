// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Serves one segment to many consumers with ONE read and no per-consumer copy
 * (FR-6, M5.12) — the {@code proxy} fetch mode.
 *
 * <p>⚠️ ONE READ PER CALL, HOWEVER LARGE THE FAN-OUT. Doc 10 §4 prices proxy at
 * "1 GET + pod bandwidth" against direct's "N GETs", and that ratio is why
 * proxy is the fallback of choice for a busy stream once a batch is too large
 * to inline. ⚠️ NOT "the default" -- an earlier draft said that, and it
 * collides with M5's SPEC ("`inline` is the default and stays it") and with
 * {@code FetchPolicy}'s load-bearing inline-first ordering. The code is
 * right; the sentence was not.
 *
 * <p>⚠️ THAT IS NOT YET cost.md R5, and an earlier draft of this paragraph
 * claimed it was. R5 is "one fetch per object per NODE, shared by every shard
 * on it"; this class holds no cache and coalesces nothing in flight, so a
 * second call for the same segment is a second GET. M5's SPEC names "a late
 * subscriber" as a proxy trigger, and a late subscriber is by construction not
 * in a list that has already been served — so one fan-out of 64 plus three
 * late subscribers is four GETs for one object on one node. ⚠️ The per-AZ
 * prefetch that makes it one-per-node is M5.16's, and the cache SPEC rule R11
 * puts in the ingester pods is not built; until then this class's only byte
 * source is the store. M5.40 owns closing it.
 *
 * <p>⚠️ AND PER-CONSUMER FETCHING IS NOT AN NFR-4 VIOLATION, which an earlier
 * draft of this paragraph also got wrong — the same inversion M5.10's review
 * corrected in {@code BinStore.presign}'s javadoc, written again here. NFR-4
 * forbids scaling with shards, partitions or indices, and a consumer is one per
 * NODE, which NFR-4 explicitly allows. The disproof is in this milestone's own
 * subject: {@code direct} IS one GET per consumer and is a sanctioned mode. The
 * real objection to fetching per consumer is narrower and sufficient: proxy
 * exists to make a large fan-out cheap, so paying K GETs where one serves all K
 * spends the entire advantage the mode has over {@code direct}.
 *
 * <p>⚠️ STREAMED, NEVER BUFFERED (ADR-0004). The loop below reads a chunk and
 * hands that same array to every consumer before reading the next, so the
 * bytes this class materialises are {@code chunkBytes} — independent of the
 * SEGMENT SIZE and of K. Buffering the segment and forwarding it is the
 * implementation M5's SPEC names as this row's falsifier: it makes service
 * memory scale with fan-out, which is NFR-6, and it is what ADR-0004 rejected
 * at $3,732/month.
 *
 * <p>⚠️ THE ORDER OF THE TWO LOOPS IS THE PROPERTY. Reading the whole segment
 * and then looping over consumers is buffer-then-forward however small the
 * chunks are; reading a chunk and then looping over consumers is streaming.
 * Nothing but the nesting distinguishes them, which is why
 * {@code SegmentProxyTest} asserts the peak hand-off rather than reading this
 * file.
 *
 * <p>⚠️ NOTHING HERE PUTS A MODE ON THE WIRE. {@code Push} still carries bytes
 * inline and {@code FetchMode} is not serialized; M5.14 owns the subscription's
 * shape, its ADR and its golden files. Shipping half a format change is what
 * `wire-format-change`'s one-commit rule forbids.
 */
public final class SegmentProxy {

    /**
     * 64 KiB — large enough that a syscall per chunk is not the cost, small
     * enough that the bound is uninteresting beside a 256 MB heap.
     *
     * <p>⚠️ IT IS NOT DERIVED FROM THE SEGMENT SIZE, deliberately. A chunk that
     * scaled with the segment would satisfy "flat in K" and still let one
     * 32 MiB bulk body (the ceiling {@code IngestConfig} documents) materialise
     * whole.
     */
    public static final int DEFAULT_CHUNK_BYTES = 64 * 1024;

    private final BinStore store;
    private final int chunkBytes;

    public SegmentProxy(BinStore store) {
        this(store, DEFAULT_CHUNK_BYTES);
    }

    public SegmentProxy(BinStore store, int chunkBytes) {
        this.store = Objects.requireNonNull(store, "store");
        if (chunkBytes <= 0) {
            throw new IllegalArgumentException(
                    "a chunk of " + chunkBytes + " bytes streams nothing");
        }
        this.chunkBytes = chunkBytes;
    }

    /** The largest slice any consumer is handed. */
    public int chunkBytes() {
        return chunkBytes;
    }

    /**
     * Streams {@code segmentKey} to every sink in {@code consumers}.
     *
     * <p>⚠️ A CONSUMER THAT THROWS IS DROPPED, NOT PROPAGATED, and the read
     * continues for the rest. This is {@code SubscriptionHub.publishRun}'s
     * discipline and it is here for the same reason: the commit is already
     * durable, so a dead consumer must not stall or roll back a write that
     * succeeded — it falls behind and recovers from the commit log, which is
     * what the log is for.
     *
     * <p>⚠️ A SINK THAT BLOCKS IS NOT HANDLED, and an earlier draft of this
     * paragraph said "slow or dead" as though it were. Only a sink that THROWS
     * is dropped. A sink that simply does not return — the expected shape once
     * these bytes go to a consumer's socket — holds the serving thread, this
     * buffer and the open store stream for the whole fan-out. The failure is
     * not hypothetical: consumer 7 of 64 stalls on a zero TCP window, the store
     * connection idles past its read timeout and throws, this method
     * propagates, and all 64 sinks are left holding a truncated prefix — the
     * outcome the paragraph below says the design avoids. ⚠️ Closing it needs a
     * per-sink deadline and somewhere to put the slow ones, which is a design
     * this row did not carry. M5.41 added it, on the THREE-argument overload
     * below; this method still has no deadline, so a caller must not hand it a
     * sink that can block indefinitely. Wiring the other one waits on M5.58.
     *
     * <p>⚠️ THE READ IS NOT ABANDONED WHEN ONE CONSUMER DIES, because the
     * others are mid-segment and a truncated stream is worse than a slow one:
     * a consumer that received half a segment has no way to tell that from a
     * whole one.
     *
     * <p>⚠️ ONE CALL IS ONE READ, SO THE CALLER OWNS THE GRANULARITY. This
     * method makes no attempt to recognise that two calls name the same
     * segment, so a caller looping the per-{@code RunKey} subscriber map would
     * issue one GET per SHARD per flush -- ~1,600 for a single 8 MiB segment.
     * Call it ONCE PER SEGMENT with every interested sink, not once per run.
     * ⚠️ AND ONE ENTRY PER CONSUMER, not one per subscription: a node
     * subscribed to several runs of the same segment is ONE sink, not several.
     * This method takes a {@code List} and writes every chunk to every entry,
     * so the flattened union of a per-{@code RunKey} map sends one node the
     * same 8 MiB once per {@code RunKey} it hosts -- around 178 of a segment's
     * ~1,600 runs. The GET count stays 1 and every test stays green; what is
     * spent is the pod bandwidth doc 10 §4 prices beside it.
     * ⚠️ Nothing enforces that here and no test can see it, because every test
     * in this file measures a single call; M5.40 carries it.
     *
     * <p>⚠️ AN EMPTY LIST STILL READS, and that is a description rather than a
     * recommendation. An earlier draft justified it by claiming the same branch
     * reused for "every consumer has died" would stop store failures being
     * reported; round-3 review showed that argument is simply wrong -- an entry
     * guard on {@code consumers.isEmpty()} is not that branch and cannot become
     * one, because the loop below never re-checks emptiness and the paragraph
     * above forbids abandoning the read on consumer death.
     *
     * <p>⚠️ SO THE HONEST STATEMENT IS A WARNING, NOT A DEFENCE: calling this
     * with nothing to serve buys a GET for no one. M5's SPEC criterion 8 names
     * "a serving path that fetches once per empty poll" as the falsifier its
     * zero-idle-requests proof must red against, so a caller that reaches here
     * on an empty poll is building exactly what M5.19 has to disprove. The
     * guard belongs at the call site, where whether the poll was empty is
     * known.
     *
     * @return how many sinks received the whole segment
     * @throws IOException if the STORE fails — that is not one consumer's
     *     problem and every consumer's stream is then incomplete
     */
    public int streamTo(String segmentKey, List<? extends SegmentSink> consumers)
            throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(consumers, "consumers");
        // ⚠️ REJECTED HERE RATHER THAN ABSORBED BELOW. The catch around each
        // hand-off treats any throw as "this consumer is gone", so a null in
        // the caller's list would be silently dropped as a dead consumer --
        // a caller's bug reported as a consumer's death, with no log, metric
        // or diagnostic anywhere. Round-1 review named it.
        for (SegmentSink sink : consumers) {
            Objects.requireNonNull(sink, "a null sink is a caller error, not a dead consumer");
        }

        // ⚠️ ONE array for the whole fan-out, allocated once before the read.
        // Allocating inside the loop would be correct and would still be flat
        // in K; it is out here so that the ONLY allocation proportional to
        // anything is this one, and it is proportional to the chunk.
        byte[] buffer = new byte[chunkBytes];
        List<SegmentSink> live = new ArrayList<>(consumers);

        try (InputStream in = store.get(segmentKey)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (read == 0) {
                    continue;
                }
                // ⚠️ CHUNK OUTER, CONSUMER INNER. Swapping these two loops is
                // buffer-then-forward, and is the mutation this class exists
                // to make fail.
                for (int i = live.size() - 1; i >= 0; i--) {
                    try {
                        live.get(i).write(buffer, 0, read);
                    } catch (IOException | RuntimeException slowOrDeadConsumer) {
                        // ⚠️ Iterating BACKWARDS is what makes removal safe
                        // here without a copy per chunk.
                        live.remove(i);
                    }
                }
            }
        }
        return live.size();
    }

    /**
     * Streams the segment, dropping any sink that has not taken a chunk within
     * {@code perChunkDeadline}.
     *
     * <p>⚠️ THE TWO-ARGUMENT METHOD HANDLES A SINK THAT THROWS; this one also
     * handles a sink that BLOCKS, which is the same thing from the fan-out's
     * point of view and was not. Consumer 7 of 64 stalling on a zero TCP window
     * held the serving thread, the shared chunk buffer and the open store
     * {@code InputStream}; the store connection then idled past its read
     * timeout and threw, and all 64 sinks were left holding a TRUNCATED PREFIX
     * they cannot tell from a whole segment.
     *
     * <p>⚠️ SLOW IS DROPPED, NOT BUFFERED, and that is the decision M5.41
     * asked whoever took it to make. Buffering a slow consumer reintroduces
     * per-consumer memory, which M5's criterion 6 forbids by name; dropping
     * costs that consumer nothing it cannot recover, because it replays from
     * the commit log exactly as a dead one does. So a slow consumer and a dead
     * consumer get the same answer.
     *
     * <p>⚠️ THE DEADLINE IS PER CHUNK, NOT PER SEGMENT, because a per-segment
     * budget would let one consumer spend the whole of it and stall the others
     * for its duration -- the defect, merely bounded. Per chunk, a consumer
     * that cannot keep up is dropped at the FIRST chunk it misses.
     *
     * ⚠️ THE BOUND IS PER HAND-OFF AND NOT PER CALL, which is worth stating
     * because the two are easy to confuse: 128 chunks of an 8 MiB segment at a
     * 200 ms deadline is 25.6 s of worst-case serving, MORE than a per-segment
     * budget of the same number. What per-chunk buys is not a shorter total, it
     * is that no single consumer can hold the others for longer than one
     * deadline at a time, and that a slow one is shed early rather than at the
     * end.
     *
     * <p>⚠️ THIS CODE READS NO CLOCK, which is a narrower claim than "no
     * clock is read": {@code invokeAll} takes the bound and does the waiting,
     * and inside the JDK that waiting reads {@code System.nanoTime()}. What it
     * buys is that nothing HERE sources time, so `check-io-seam.sh` and
     * non-negotiable 7 are satisfied without a seam -- and a caller wanting a
     * deterministic deadline cannot have one, because the executor is built
     * inline rather than injected. The case below is therefore a wall-clock
     * test with a real budget, and says so.
     *
     * <p>⚠️ A DROPPED SINK MAY STILL BE INSIDE {@code write} WHEN THE BUFFER
     * IS REUSED -- and ONLY a dropped one, because every sink still in the live
     * list provably returned from {@code write} before {@code invokeAll}
     * returned. That is deliberate rather than overlooked: the array is
     * refilled for the next chunk while a cancelled task may still be reading
     * it, so a dropped consumer can be handed bytes that are neither its chunk
     * nor a whole one. It is dropped -- {@code SubscriptionHub} completes only
     * sinks that took every byte -- so what it received is never presented as a
     * segment. Handing it a private copy would be per-consumer memory, which is
     * the thing this method must not spend.
     *
     * ⚠️ BUT THE HUB CANNOT YET TELL A TIMED-OUT SINK FROM A SERVED ONE, and
     * an earlier draft of this paragraph claimed it could. {@code
     * SubscriptionHub.Tracking} marks a sink failed only from a CATCH, so a
     * sink dropped for missing its deadline never throws, is never marked, and
     * would be handed {@code complete(...)} — exactly the truncated prefix the
     * failure chain above ends in. Nothing catches it: {@code deliver} ignores
     * this method's return value, and a COUNT cannot say WHICH sink went.
     *
     * ⚠️ SO THIS OVERLOAD IS NOT WIRED, and must not be until the caller can
     * learn which sinks were dropped. The hub calls the two-argument method,
     * where every drop is a throw the wrapper sees. Whoever wires a blocking
     * sink owes that before using this one, and M5.58 owns it -- M5.13, M5.14
     * and M5.41 are all done, so naming them would have left the obligation
     * with nobody, which is the defect M5.44's row records.
     *
     * @param perChunkDeadline how long a sink may take over ONE chunk before it
     *     is dropped; must be positive
     * @return how many sinks took every chunk
     * @throws IOException if the STORE fails, or if this thread is interrupted
     *     while waiting on a hand-off -- the second is not a store failure, and
     *     the two-argument method cannot raise it. ⚠️ IT IS NOT THE ONLY
     *     DIFFERENCE: that method calls {@code write} on the CALLER's thread
     *     and this one enters it on a fresh virtual thread per chunk, so a sink
     *     with thread affinity behaves differently here even though {@link
     *     SegmentSink} states no affinity contract
     * @throws IllegalArgumentException if {@code perChunkDeadline} is not
     *     positive, which would drop every consumer
     */
    public int streamTo(String segmentKey, List<? extends SegmentSink> consumers,
            java.time.Duration perChunkDeadline) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(consumers, "consumers");
        Objects.requireNonNull(perChunkDeadline, "perChunkDeadline");
        if (perChunkDeadline.isZero() || perChunkDeadline.isNegative()) {
            throw new IllegalArgumentException(
                    "a per-chunk deadline of " + perChunkDeadline + " drops every consumer");
        }
        for (SegmentSink sink : consumers) {
            Objects.requireNonNull(sink, "a null sink is a caller error, not a dead consumer");
        }

        byte[] buffer = new byte[chunkBytes];
        List<SegmentSink> live = new ArrayList<>(consumers);
        // ⚠️ NOT try-with-resources ON THE EXECUTOR, and this is the whole
        // point of the method. `ExecutorService.close()` is `shutdown()` plus
        // an UNBOUNDED `awaitTermination`, so closing it would wait for the
        // very sink the deadline just dropped. Review MEASURED that: a 200 ms
        // deadline against a task that swallows interruption for 4 s returned
        // from `invokeAll` at 202 ms and from the try block at 4,011 ms. That
        // is the stall this method exists to remove, moved four lines down.
        var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        try (InputStream in = store.get(segmentKey)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (read == 0) {
                    continue;
                }
                handOff(workers, live, buffer, read, perChunkDeadline);
            }
        } finally {
            // ⚠️ INTERRUPT AND WALK AWAY. `shutdownNow` interrupts what is
            // still running and does NOT wait, so a sink that ignores
            // interruption delays nobody. Its task is abandoned holding a
            // reference to the buffer, which is the race the class javadoc
            // records.
            workers.shutdownNow();
        }
        return live.size();
    }

    /**
     * Hands one chunk to every live sink at once, and removes those that threw
     * or ran out of time.
     *
     * <p>⚠️ ONE VIRTUAL THREAD PER SINK PER CHUNK, which is what makes the
     * deadline enforceable at all: {@code SegmentSink.write} is a blocking call
     * and nothing can bound it from the outside without another thread. The
     * threads are virtual, so the cost is a task rather than an OS thread --
     * but the count is worth naming rather than waving at: an 8 MiB segment at
     * the 64 KiB default is 128 chunks, so a fan-out of 64 submits 8,192 tasks
     * and crosses 128 K-way barriers for one segment. That is the price of
     * bounding a blocking call, and it is paid per served segment.
     *
     * <p>⚠️ THE BYTES STAY FLAT IN K -- one shared buffer, as before, and no
     * per-consumer copy. What is NOT flat in K is the task count, and saying so
     * is the honest scope of criterion 6's claim for this path: the criterion
     * measures the largest array the serving path materialises, and that is
     * still the chunk.
     */
    private void handOff(java.util.concurrent.ExecutorService workers,
            List<SegmentSink> live, byte[] buffer, int read,
            java.time.Duration deadline) throws IOException {
        List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>(live.size());
        for (SegmentSink sink : live) {
            tasks.add(() -> {
                sink.write(buffer, 0, read);
                return null;
            });
        }
        List<java.util.concurrent.Future<Void>> results;
        try {
            results = workers.invokeAll(tasks, deadline.toNanos(),
                    java.util.concurrent.TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            // ⚠️ NOT `InterruptedIOException`, which says this precisely:
            // `check-io-seam.sh` bans the `java.io` PACKAGE and carves out the
            // byte and exception types this tree uses, and that one is not on
            // the list. Refusing it is a false refusal of the M0.112 class --
            // an exception type reaches nothing -- and widening the carve-out
            // is that row's, not this commit's.
            throw new IOException("serving was interrupted");
        }
        // ⚠️ BACKWARDS, so removal needs no copy per chunk.
        for (int i = results.size() - 1; i >= 0; i--) {
            if (!tookIt(results.get(i))) {
                live.remove(i);
            }
        }
    }

    /** Whether one sink finished its chunk without throwing. */
    private static boolean tookIt(java.util.concurrent.Future<Void> result) {
        if (result.isCancelled()) {
            return false;
        }
        try {
            result.get();
            return true;
        } catch (java.util.concurrent.ExecutionException failed) {
            // ⚠️ AN `Error` IS NOT A DEAD CONSUMER. The two-argument loop
            // catches `IOException | RuntimeException` and lets an `Error`
            // propagate; unwrapping keeps this overload identical rather than
            // turning an OutOfMemoryError into "that consumer went away".
            if (failed.getCause() instanceof Error error) {
                throw error;
            }
            return false;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
