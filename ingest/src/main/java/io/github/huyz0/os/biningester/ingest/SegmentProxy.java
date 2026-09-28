// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
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
 * <p>⚠️ HALF OF cost.md R5, AND THE HALF THAT IS LEFT IS NAMED. R5 is "one
 * fetch per object per NODE, shared by every shard on it". A repeat across
 * publishes now costs no GET: {@link SegmentCache} holds whole segments and
 * this class consults it, so one fan-out of 64 plus three LATE subscribers is
 * ONE read (M5.40b). And two publishes of the same segment that OVERLAP IN
 * TIME are one read too since M5.63: the second caller attaches to the first
 * caller's read in flight rather than missing and fetching again. ⚠️ And the
 * per-AZ prefetch that
 * makes one read serve a whole availability zone is M5.16's, which is a
 * different scope from either.
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
 * <p>⚠️ STREAMED, NEVER BUFFERED PER CONSUMER (ADR-0004). The loop below reads
 * a chunk and hands that same array to every consumer before reading the next,
 * so what this class materialises is independent of K — one chunk serves the
 * whole fan-out, whether K is 1 or 64. ⚠️ IT IS NO LONGER INDEPENDENT OF THE
 * SEGMENT SIZE, and M5.40b is what changed that: a cache admission accumulates
 * the segment so it can be held. That is ADR-0004's own design rather than its
 * violation -- the ADR's text is "the pod that wrote a segment still holds it
 * in RAM" -- but the claim this paragraph used to make, that the bytes
 * materialised are {@code chunkBytes} full stop, was true before that commit
 * and is not now. With a cache of capacity 0 it is true again, which is what
 * {@code SegmentProxyTest}'s criterion-6 cases build. Buffering the segment and forwarding it is the
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
    private final SegmentCache cache;
    private final IndexCostLedger ledger;

    /**
     * Charges each store GET it issues to the indices of the segment it read,
     * into {@code ledger} (M11.3, ADR-0077).
     *
     * <p>⚠️ THE ONLY CONSTRUCTOR, AND IT TAKES THE LEDGER (M12.1, M11 review
     * F2): an overload that made its own charged a ledger nothing reads, so a
     * path built with it issued requests no report could see.
     *
     * @param cache repeats across publishes are served from here rather than
     *     from a GET; a capacity of {@code 0} turns caching off and restores
     *     the pre-M5.40b behaviour exactly
     */
    public SegmentProxy(BinStore store, int chunkBytes, SegmentCache cache,
            IndexCostLedger ledger) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.store = Objects.requireNonNull(store, "store");
        if (chunkBytes <= 0) {
            throw new IllegalArgumentException(
                    "a chunk of " + chunkBytes + " bytes streams nothing");
        }
        this.chunkBytes = chunkBytes;
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    /** The cache repeats are served from; capacity {@code 0} means none. */
    public SegmentCache cache() {
        return cache;
    }

    private final java.util.concurrent.ConcurrentHashMap<String, InFlightRead> inFlight =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * ⚠️ A TEST SEAM: runs between a caller's cache miss and its claim of the
     * key, the one window in which a previous winner can fill the cache and
     * leave. Package-private and a no-op in production.
     */
    volatile Runnable betweenMissAndClaim = () -> { };

    /** ⚠️ A TEST SEAM: between a claim and its re-check of the cache. No-op in production. */
    volatile Runnable betweenClaimAndRecheck = () -> { };

    /** ⚠️ A TEST SEAM: between a read's completion and its entry's removal. No-op in production. */
    volatile Runnable betweenCompleteAndRemove = () -> { };

    /** How many callers are waiting on the read of {@code segmentKey} now. */
    int joinersOf(String segmentKey) {
        InFlightRead read = inFlight.get(segmentKey);
        return read == null ? 0 : read.joiners();
    }

    /** Whether a read of {@code segmentKey} is registered as in flight. */
    boolean inFlight(String segmentKey) {
        return inFlight.containsKey(segmentKey);
    }

    /** The largest slice any consumer is handed. */
    public int chunkBytes() {
        return chunkBytes;
    }

    /**
     * Streams {@code segmentKey} to every sink in {@code consumers}.
     *
     * <p>⚠️ A CONSUMER THAT THROWS IS DROPPED, NOT PROPAGATED, and the read
     * continues for the rest. This is {@code SegmentServingPath.deliver}'s
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
     * <p>⚠️ THE CALLER STILL OWNS THE GRANULARITY. Since M5.63 two calls that
     * OVERLAP on one cold key share one read, but two that follow each other
     * are one read only while the cache holds the segment, so a caller looping
     * the per-{@code RunKey} subscriber map would still issue one GET per SHARD
     * per flush for any segment the cache cannot hold -- ~1,600 for a single
     * 8 MiB segment.
     * Call it ONCE PER SEGMENT with every interested sink, not once per run.
     * ⚠️ AND ONE ENTRY PER CONSUMER, not one per subscription: a node
     * subscribed to several runs of the same segment is ONE sink, not several.
     * This method takes a {@code List} and writes every chunk to every entry,
     * so the flattened union of a per-{@code RunKey} map sends one node the
     * same 8 MiB once per {@code RunKey} it hosts -- around 178 of a segment's
     * ~1,600 runs. The GET count stays 1 and every test stays green; what is
     * spent is the pod bandwidth doc 10 §4 prices beside it.
     * ⚠️ STILL NOTHING ENFORCES IT HERE -- every test in this file measures a
     * single call -- but it IS enforced one layer up, and a test does see it:
     * {@code SegmentServingPath.publishSegment} groups by {@code Subscriber}
     * identity so a consumer is one entry however many runs it holds, and
     * {@code OneStreamPerConsumerTest} exercises that through
     * {@code SegmentServing} into this method (M5.40a). ⚠️ WHAT IS STILL OPEN
     * IS M5.62, the plugin wiring that makes a node register ONE subscriber
     * instead of one per run -- until that lands, production still hands this
     * method a node's runs as separate consumers, so the duplication above is
     * real in a deployment even though the hub can now avoid it.
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
        List<SegmentSink> live = new ArrayList<>(consumers);

        // ⚠️ A HIT COSTS NO GET, which is the whole of M5.40b. The bytes are
        // still handed over A CHUNK AT A TIME: a hit must not become the
        // buffer-then-forward this class exists to forbid just because the
        // buffer happens to be ours.
        //
        // ⚠️ AND IT IS CHECKED BEFORE THE CHUNK BUFFER IS ALLOCATED, so a hit
        // garbages nothing.
        byte[] hit = cache.get(segmentKey);
        if (hit != null) {
            writeChunked(hit, live);
            return live.size();
        }

        // ⚠️ M5.63: ONE IN-FLIGHT READ PER KEY. A second caller of a cold key
        // ATTACHES to the first read instead of issuing its own GET -- the
        // normal case for the proxy segment route, where every node of an AZ
        // asks for a fresh segment at once (research 10 §4). See InFlightRead for
        // how a joiner streams. With caching off (capacity 0) nothing is
        // shared, which keeps the pre-M5.40b behaviour exact.
        if (cache.capacityBytes() == 0) {
            readThrough(segmentKey, live, null, false);
            return live.size();
        }
        betweenMissAndClaim.run();
        InFlightRead mine = new InFlightRead();
        InFlightRead leader = inFlight.putIfAbsent(segmentKey, mine);
        if (leader != null) {
            int served = leader.join(live, chunkBytes);
            if (served >= 0) {
                return served;
            }
            // ⚠️ TOO LARGE TO SHARE, and the winner has already shown it will
            // not fit: read for this caller alone, and do not try to admit it,
            // or K late callers each grow a cache-sized buffer to discard.
            readThrough(segmentKey, live, null, false);
            return live.size();
        }
        Throwable failure = null;
        try {
            // ⚠️ RE-CHECKED AFTER CLAIMING: the previous winner may have filled
            // the cache and left between this caller's miss and its claim.
            betweenClaimAndRecheck.run();
            byte[] late = cache.get(segmentKey);
            if (late != null) {
                for (int offset = 0; offset < late.length; offset += chunkBytes) {
                    int length = Math.min(chunkBytes, late.length - offset);
                    mine.relay(late, offset, length, late, offset + length);
                }
                writeChunked(late, live);
                return live.size();
            }
            // ⚠️ THE CACHE IS FILLED INSIDE, BEFORE THIS RETURNS -- and so
            // before the entry is removed below: a caller arriving between the
            // two finds one or the other, never neither.
            readThrough(segmentKey, live, mine, true);
            return live.size();
        } catch (Throwable anyFailure) {
            // ⚠️ ANY THROWABLE, AN `Error` INCLUDED: a joiner waits without a
            // timeout, so a read that ended without completing its entry would
            // hold every joiner's consumers for ever.
            failure = anyFailure;
            throw anyFailure;
        } finally {
            mine.complete(failure);
            betweenCompleteAndRemove.run();
            inFlight.remove(segmentKey, mine);
        }
    }

    /**
     * One caller's read of a cold key: streamed to {@code live}, relayed to
     * {@code shared}'s joiners when there is one, and admitted to the cache
     * when {@code admit} and it fits.
     */
    private void readThrough(String segmentKey, List<SegmentSink> live, InFlightRead shared,
            boolean admit) throws IOException {
        byte[] buffer = new byte[chunkBytes];

        // ⚠️ GROWN, NOT DOUBLED WITHOUT A LIMIT, and abandoned the moment the
        // running total would cross the ceiling. Accumulating first and
        // checking afterwards would hold a whole oversized segment to decide
        // not to keep it -- the ceiling breached by the one object most able to
        // exhaust the heap.
        //
        // ⚠️ THE CEILING BOUNDS RESIDENCY, NOT THE TRANSIENT PEAK, and an
        // earlier version of this used a `ByteArrayOutputStream`, whose
        // doubling review measured at ~67 MiB transient for a 16 MiB segment.
        // Growth is clamped to the ceiling here and the trim below copies once,
        // so the peak is the accumulator plus one copy rather than an unbounded
        // double-and-copy -- stated because `bytesHeld()` cannot show it.
        byte[] admitting = admit && cache.capacityBytes() > 0 ? new byte[0] : null;
        int admitted = 0;
        // ⚠️ ONE CHARGE PER STORE GET, HOWEVER IT ENDS (M11.3, ADR-0077): split
        // by the directory when the whole segment was held, else unattributed
        // -- a GET that threw or was never held was still billed and counted.
        byte[] charged = null;

        try {
            try (InputStream in = store.get(segmentKey)) {
                int read;
                while ((read = in.read(buffer)) != -1) {
                    if (read == 0) {
                        continue;
                    }
                    if (admitting != null) {
                        // ⚠️ LONG ARITHMETIC. `admitted + read` in int overflows
                        // against a ceiling above 2 GiB and turns a refusal into an
                        // OutOfMemoryError mid-serve.
                        if ((long) admitted + read > cache.capacityBytes()) {
                            admitting = null;
                        } else {
                            if (admitted + read > admitting.length) {
                                // ⚠️ LONG THROUGHOUT. `admitting.length * 2` and
                                // `admitted + read` in int overflow negative above a
                                // 2 GiB ceiling, turning a growth step into a
                                // NegativeArraySizeException mid-serve.
                                long want = Math.max((long) admitting.length * 2,
                                        (long) admitted + read);
                                admitting = java.util.Arrays.copyOf(admitting,
                                        (int) Math.min(want, cache.capacityBytes()));
                            }
                            System.arraycopy(buffer, 0, admitting, admitted, read);
                            admitted += read;
                        }
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
                    if (shared != null) {
                        shared.relay(buffer, 0, read, admitting, admitted);
                    }
                }
            }
            // ⚠️ ADMITTED ONLY AFTER THE READ COMPLETED, so a stream that THREW
            // part way leaves by the exception above and its prefix is never handed
            // to a later subscriber as a whole segment.
            //
            // ⚠️ A SHORT READ THAT DOES NOT THROW IS NOT COVERED BY THAT, and the
            // cache makes it worse rather than better: an `InputStream` that ends
            // early without an exception used to cost ONE truncated delivery, and
            // now that truncation is admitted and served to every later subscriber
            // for as long as the entry lives. Closing it needs the expected length,
            // which this method does not have without a `stat` -- one request per
            // segment, which is the thing this row exists to remove. M5.64.
            if (admitting != null) {
                charged = admitted == admitting.length
                        ? admitting
                        : java.util.Arrays.copyOf(admitting, admitted);
                cache.put(segmentKey, charged);
            }
        } finally {
            SegmentCharges.chargeGet(ledger, charged);
        }
    }

    /**
     * Hands an in-memory segment over a chunk at a time.
     *
     * <p>⚠️ CHUNKED, EVEN THOUGH WE HOLD IT ALL. Writing the whole array in one
     * call would make every consumer's sink see a segment-sized slice, which is
     * the shape {@code SegmentSink}'s contract exists to avoid and what a
     * socket-backed consumer would have to buffer.
     */
    private void writeChunked(byte[] segment, List<SegmentSink> live) {
        for (int offset = 0; offset < segment.length && !live.isEmpty(); offset += chunkBytes) {
            int length = Math.min(chunkBytes, segment.length - offset);
            for (int i = live.size() - 1; i >= 0; i--) {
                try {
                    live.get(i).write(segment, offset, length);
                } catch (IOException | RuntimeException slowOrDeadConsumer) {
                    live.remove(i);
                }
            }
        }
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
     * ⚠️ THE CALLER CAN NOW TELL A TIMED-OUT SINK FROM A SERVED ONE, which is
     * what M5.58a changed and what the {@code @return} below is: this method
     * used to return a COUNT, and a count cannot say WHICH sink went. Two
     * earlier drafts of this paragraph have been wrong in opposite directions
     * -- one claimed the hub could tell before it could, this one would claim
     * the work is finished -- so what is left is stated narrowly.
     *
     * ⚠️ WHAT IS LEFT IS THE CALLER'S HALF, AND THIS OVERLOAD STAYS UNWIRED
     * UNTIL IT LANDS. {@code SegmentServingPath.Tracking} marks a sink failed
     * only from a CATCH, so a sink dropped for missing its deadline still
     * arrives at {@code complete(...)} unmarked unless the caller crosses this
     * list off against it -- and nothing in {@code SegmentServing}, in this
     * class or in any config owns a per-chunk deadline to pass in the first
     * place. The hub calls the two-argument method, where every drop is a throw
     * the wrapper already sees. M5.58b owns both halves; M5.58, which this
     * paragraph used to name, is split.
     *
     * @param perChunkDeadline how long a sink may take over ONE chunk before it
     *     is dropped; must be positive
     * @return the sinks this call DROPPED, in the order they were supplied,
     *     empty when every one took every chunk -- ⚠️ THE DROPS RATHER THAN
     *     THE SURVIVORS, and rather than the count this method used to return
     *     (M5.58a). A count cannot say WHICH, and which is the only thing a
     *     caller can act on: completing a sink that was dropped hands it a
     *     truncated prefix it cannot tell from a segment. The drops are the
     *     exceptional set, so the list is empty on the happy path and is the
     *     smaller of the two at every fan-out. ⚠️ BY IDENTITY, NOT BY VALUE:
     *     the caller matches these against the sinks it passed in, and a
     *     {@link SegmentSink} is caller-supplied and may implement
     *     {@code equals} however it likes
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
    public List<SegmentSink> streamTo(String segmentKey, List<? extends SegmentSink> consumers,
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
        // ⚠️ SUPPLY ORDER, NOT DROP ORDER. `handOff` removes backwards so that
        // removal needs no copy per chunk, and a caller pairing this list
        // against the one it passed in would otherwise read the drops of a
        // single chunk in reverse. Built at the end from the supplied list
        // rather than appended to as sinks go, which costs one pass over K and
        // makes the order a property of the parameter instead of of the
        // removal loop.
        List<SegmentSink> supplied = List.copyOf(consumers);
        // ⚠️ NOT try-with-resources ON THE EXECUTOR, and this is the whole
        // point of the method. `ExecutorService.close()` is `shutdown()` plus
        // an UNBOUNDED `awaitTermination`, so closing it would wait for the
        // very sink the deadline just dropped. Review MEASURED that: a 200 ms
        // deadline against a task that swallows interruption for 4 s returned
        // from `invokeAll` at 202 ms and from the try block at 4,011 ms. That
        // is the stall this method exists to remove, moved four lines down.
        var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        // ⚠️ NEVER HELD, SO NEVER SPLIT (M11.3): one unattributed charge per GET.
        SegmentCharges.chargeGet(ledger, null);
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
        // ⚠️ IDENTITY, NOT `removeAll`. `List.removeAll` uses `equals`, and a
        // sink is caller-supplied: two sinks that merely COMPARE EQUAL are
        // still two consumers. MEASURED, and the direction is the bad one --
        // `removeAll` strips from the drop list every element equal to a
        // SURVIVOR, so the sink that actually went is removed by its healthy
        // twin and the call reports NOBODY as dropped. That completes a sink
        // handed spliced bytes, which is the silent failure this return shape
        // exists to prevent. Same reason `SegmentServingPath.ByIdentity`
        // exists; pinned by
        // `SegmentProxyTest.aSinkIsDroppedByIDENTITYNotByEQUALS`.
        List<SegmentSink> dropped = new ArrayList<>();
        for (SegmentSink sink : supplied) {
            boolean stillLive = false;
            for (SegmentSink survivor : live) {
                if (survivor == sink) {
                    stillLive = true;
                    break;
                }
            }
            if (!stillLive) {
                dropped.add(sink);
            }
        }
        return List.copyOf(dropped);
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
     * measures the largest array the serving path materialises, and on THIS
     * overload that is still the chunk -- it has no cache (M5.40b gave one only
     * to the two-argument form, which is the only one with a production
     * caller), so nothing here accumulates a segment.
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
