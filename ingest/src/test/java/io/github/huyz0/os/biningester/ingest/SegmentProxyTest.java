// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.KEY;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.RecordingSink;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.SEGMENT_BYTES;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.segment;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.sinks;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.storeHolding;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;


import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code proxy} streams through, and one read serves the fan-out (M5.12).
 *
 * <p>⚠️ WHAT THESE TESTS PROVE AND WHAT THEY DO NOT. They constrain the
 * ALLOCATION DISCIPLINE — the largest array the serving path materialises, and
 * how many times it opens the store — not JVM heap retention. M1.18's
 * {@code memoryBoundTest} task exists for real {@code -Xmx}-enforced ceilings
 * and its own javadoc records that every mutation it tried crashed before its
 * diagnostic assertions ran. For THIS falsifier the allocation bound is the
 * stronger instrument: buffer-then-forward is caught by an assertion that
 * fails deterministically, rather than by garbage collection that might not
 * run.
 */
class SegmentProxyTest {


    /**
     * Criterion 1: ONE store read serves K consumers, at K=1 and at K=64.
     *
     * <p>⚠️ THE COUNT IS TAKEN AFTER THE PUT, not from zero, because staging
     * the segment is itself a request. Doc 10 §4 prices proxy at "1 GET + pod
     * bandwidth" against direct's "N GETs"; a path opening the store per
     * consumer answers 64 here.
     *
     * <p>⚠️ THAT IS NOT AN NFR-4 VIOLATION, and an earlier version of this
     * javadoc said it was -- the same inversion M5.10's review corrected in
     * {@code BinStore.presign}. NFR-4 forbids scaling with shards, partitions
     * or indices; a consumer is one per NODE, which it allows, and
     * {@code direct} is one GET per consumer and sanctioned. What K GETs
     * actually costs is the whole advantage proxy has over direct.
     */
    @Test
    void ONEStoreReadServesTheWholeFanOut() throws Exception {
        for (int k : new int[] {1, 64}) {
            CountingBinStore store = storeHolding(segment());
            long before = store.counts().gets();
            new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, sinks(k));
            assertThat(store.counts().gets() - before)
                    .as("K=%d consumers, and proxy is priced at ONE GET however many there are", k)
                    .isEqualTo(1);
        }
    }

    /**
     * Criterion 2: no consumer is ever handed more than one chunk.
     *
     * <p>⚠️ THIS IS THE FALSIFIER M5's SPEC NAMES, "a buffer-then-forward
     * implementation", and the assertion that catches it. Such a path reads
     * the segment whole and hands each consumer a 1 MiB array — 16x the chunk
     * — so the peak hand-off, not the total delivered, is what separates the
     * two designs. Both deliver the same bytes.
     */
    @Test
    void noConsumerIsEverHandedMoreThanONECHUNK() throws Exception {
        CountingBinStore store = storeHolding(segment());
        SegmentProxy proxy = new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(8);
        proxy.streamTo(KEY, consumers);
        for (SegmentProxyFixtures.RecordingSink c : consumers) {
            assertThat(c.largestHandOff)
                    .as("a hand-off larger than the chunk is the segment materialised whole")
                    .isLessThanOrEqualTo(proxy.chunkBytes());
        }
        assertThat(consumers.get(0).handOffs)
                .as("and a 1 MiB segment at a 64 KiB chunk is many hand-offs, not one")
                .isGreaterThan(1);
    }

    /**
     * The FIRST hand-off happens before the stream is drained -- streaming, not
     * chunked buffer-then-forward.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THE NAMED FALSIFIER SURVIVING IN ITS
     * CHUNKED FORM, and this test exists because of it: a proxy that calls
     * {@code readAllBytes()} and then hands out {@code chunkBytes}-sized SLICES
     * of the result passed all nine tests. Service memory is O(segment), which
     * is exactly what criterion 6 forbids -- and the peak hand-off is
     * identical, because HAND-OFF SIZE DOES NOT BOUND THE MATERIALISED ARRAY.
     * Only the crudest variant, one N-byte hand-off, was caught.
     *
     * <p>⚠️ SO THE ASSERTION HAS TO BE ON THE INTERLEAVING, which is what the
     * production javadoc calls "the property": how much has been READ when the
     * first byte is DELIVERED. Streaming answers one chunk; any
     * read-it-all-first design answers the whole segment, whatever it does
     * afterwards.
     */
    @Test
    void theFIRSTHandOffHappensBeforeTheStreamIsDRAINED() throws Exception {
        byte[] expected = segment();
        long[] readWhenFirstDelivered = {-1};
        long[] readSoFar = {0};
        SegmentProxyFixtures.StubStore counting = new SegmentProxyFixtures.StubStore(expected, readSoFar, 0);
        SegmentProxy proxy = new SegmentProxy(counting, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        SegmentSink watcher = (buf, off, len) -> {
            if (readWhenFirstDelivered[0] < 0) {
                readWhenFirstDelivered[0] = readSoFar[0];
            }
        };
        proxy.streamTo(KEY, List.of(watcher));

        assertThat(readWhenFirstDelivered[0])
                .as("a proxy that drains the stream before delivering anything has read the "
                        + "whole %d-byte segment by its first hand-off; a streaming one has "
                        + "read at most one chunk", SEGMENT_BYTES)
                .isLessThanOrEqualTo(proxy.chunkBytes());
        assertThat(readWhenFirstDelivered[0]).isPositive();
    }

    /**
     * A chunk that does NOT divide the segment: the final short read is
     * delivered at its true length.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED {@code write(buffer, 0, read)} ->
     * {@code write(buffer, 0, buffer.length)} SURVIVING ALL NINE TESTS, hidden
     * by two fixture facts at once: 1 MiB is an exact multiple of the 64 KiB
     * default chunk, so there was never a partial final chunk; and
     * {@code MemoryBinStore.get} hands back a {@code ByteArrayInputStream},
     * whose {@code read(byte[])} always fills. Every read in every test was
     * exactly {@code buffer.length}.
     *
     * <p>⚠️ THE PRODUCTION CONSEQUENCE IS CORRUPTION, NOT A SHORT READ. Against
     * a real HTTP-backed stream, or any segment whose size is not a multiple of
     * the chunk, every consumer receives the tail padded with whatever the
     * previous chunk left in the buffer -- and decodes it as records.
     *
     * <p>⚠️ IT ALSO PINS THE CONFIGURED CHUNK AT A VALID VALUE for the first
     * time. Every other test supplies only 0 and -1, both rejected at
     * construction, and compares against {@code proxy.chunkBytes()} -- the
     * implementation's own answer -- so a two-arg constructor that ignored its
     * argument survived.
     *
     * <p>⚠️ IT DOES NOT PIN THE DEFAULT, and an earlier draft of this paragraph
     * said it did. Round-4 review MEASURED {@code DEFAULT_CHUNK_BYTES} moved to
     * 128 KiB surviving all eighteen tests: what is asserted here is the value
     * this test PASSES IN. Large values die only incidentally, because two
     * failure tests need three or more chunks from the fixture -- so the real
     * constraint is "the fixture yields 3+ chunks", not a number. That is the
     * right state: no criterion names 64 KiB, and it is a dial
     * performance.md would settle with a benchmark rather than an assertion.
     */
    @Test
    void aChunkThatDoesNOTDivideTheSegmentDeliversTheShortFinalREAD() throws Exception {
        byte[] expected = segment();
        CountingBinStore store = storeHolding(expected);
        SegmentProxy proxy = new SegmentProxy(store, 7000, new SegmentCache(0), new IndexCostLedger());
        assertThat(proxy.chunkBytes())
                .as("the constructor must USE its argument, not ignore it")
                .isEqualTo(7000);

        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(4);
        proxy.streamTo(KEY, consumers);
        assertThat(SEGMENT_BYTES % 7000).as("the fixture must actually have a short tail")
                .isNotZero();
        for (SegmentProxyFixtures.RecordingSink c : consumers) {
            assertThat(c.largestHandOff)
                    .as("the CONFIGURED chunk, exactly -- not the default, and not more")
                    .isEqualTo(7000);
            assertThat(c.received.toByteArray())
                    .as("and the tail is the segment's own bytes, not the buffer's leftovers")
                    .isEqualTo(expected);
        }
    }





    /**
     * The segment served is the segment REQUESTED.
     *
     * <p>⚠️ ROUND-3 REVIEW MEASURED {@code store.get(segmentKey)} ->
     * {@code store.get("seg/proxy")} SURVIVING ALL SIXTEEN TESTS, because every
     * fixture held one object under one constant key. This is backlog M5.37's
     * class exactly -- "a presign ignoring its key would have passed" -- and
     * the regression is queued rather than hypothetical: M5.40 inserts a CACHE
     * into this very call path, and a cache keyed on the wrong thing serves one
     * segment's bytes under another's name.
     */
    @Test
    void theSegmentServedIsTheSegmentREQUESTED() throws Exception {
        byte[] wanted = segment();
        byte[] decoy = new byte[SEGMENT_BYTES];
        java.util.Arrays.fill(decoy, (byte) 0x5A);

        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("seg/decoy", Body.ofBytes(decoy));
        store.put("seg/wanted", Body.ofBytes(wanted));

        SegmentProxyFixtures.RecordingSink sink = new SegmentProxyFixtures.RecordingSink();
        new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo("seg/wanted", List.of(sink));
        assertThat(sink.received.toByteArray())
                .as("a proxy ignoring its key serves whichever object it happens to name")
                .isEqualTo(wanted);
    }

    /**
     * A SHORT or ZERO read is not the end of the stream, and its bytes are
     * delivered exactly once.
     *
     * <p>⚠️ EVERY OTHER FIXTURE HERE IS {@code ByteArrayInputStream}-BACKED, so
     * no test ever produced a short intermediate read or a zero read -- the two
     * shapes a socket produces routinely. Round-2 review measured both loop
     * boundaries unreachable: deleting {@code if (read == 0) continue;}
     * survived, and so did {@code != -1} weakened to {@code > 0}, which would
     * end the whole segment at the first zero read and truncate every consumer.
     */
    @Test
    void aSHORTOrZEROReadIsNotTheEndOfTheStream() throws Exception {
        byte[] expected = segment();
        long[] readSoFar = {0};
        SegmentProxyFixtures.StubStore awkward = new SegmentProxyFixtures.StubStore(expected, readSoFar, 5);
        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(3);
        int served = new SegmentProxy(awkward, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, consumers);
        assertThat(served).isEqualTo(3);
        for (SegmentProxyFixtures.RecordingSink c : consumers) {
            assertThat(c.received.toByteArray())
                    .as("short reads reassemble into the whole segment, once")
                    .isEqualTo(expected);
            // ⚠️ THE ZERO-READ HALF, which round-3 review measured still
            // unkilled: deleting `if (read == 0) continue;` passed all sixteen
            // tests, because a zero-length hand-off writes nothing, does not
            // move the peak, and leaves the bytes identical. The stub emits 53
            // of them, so forwarding one is visible only if counted.
            assertThat(c.emptyHandOffs)
                    .as("a zero-length read is not a delivery and must not be forwarded")
                    .isZero();
        }
    }


    /**
     * Criterion 3: flat in K, asserted as an EQUALITY between K=1 and K=64.
     *
     * <p>⚠️ IT KILLS NOTHING THE OTHER TESTS DO NOT, and round-1 review
     * MEASURED that rather than leaving it as a claim. I argued the equality
     * was stronger than two separate bounds; it is not. The serving path does
     * not branch on K, so the two measurements cannot differ without a
     * K-dependent hand-off size, and any such mutation truncates delivery and
     * dies on criterion 4 first. ⚠️ WORSE, IT IS SATISFIED EXACTLY BY THE
     * FALSIFIER ITS OWN DOCSTRING ONCE CITED IT AGAINST: under a
     * {@code readAllBytes} proxy both peaks are 1 MiB and both read counts are
     * 1, so it passes.
     *
     * <p>⚠️ KEPT AS DEFENCE IN DEPTH, and labelled as such rather than as
     * proof -- the same honesty {@code MemoryFlatUnderTenXBodySizeTest} applies
     * to its own diagnostic assertions. It is the only place the two fan-outs
     * are compared to each other, so it is where a future K-dependent serving
     * path would show up first. It is not evidence for criterion 6 on its own,
     * and M5.20 must not cite it as such.
     */
    @Test
    void thePeakHandOffAndTheReadCountAreIDENTICALAtKONEAndKSIXTYFOUR() throws Exception {
        CountingBinStore one = storeHolding(segment());
        List<SegmentProxyFixtures.RecordingSink> single = sinks(1);
        long oneBefore = one.counts().gets();
        new SegmentProxy(one, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, single);
        long oneGets = one.counts().gets() - oneBefore;
        int onePeak = single.get(0).largestHandOff;

        CountingBinStore many = storeHolding(segment());
        List<SegmentProxyFixtures.RecordingSink> sixtyFour = sinks(64);
        long manyBefore = many.counts().gets();
        new SegmentProxy(many, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, sixtyFour);
        long manyGets = many.counts().gets() - manyBefore;
        int manyPeak = sixtyFour.stream().mapToInt(s -> s.largestHandOff).max().orElseThrow();

        assertThat(manyPeak).as("peak hand-off must not move with K").isEqualTo(onePeak);
        assertThat(manyGets).as("nor must the store read count").isEqualTo(oneGets);
    }

    /**
     * ONE array serves every hand-off to every consumer.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THIS UNASSERTED: wrapping the hand-off in
     * {@code Arrays.copyOfRange} passed every test. Retained memory stays
     * O(chunk) either way, which is why this is not the criterion-6 assertion
     * -- but allocation rate goes O(K per chunk), and the interface's whole
     * reason for its awkward signature evaporates.
     *
     * <p>⚠️ IDENTITY, NOT EQUALITY. Two arrays with the same contents are what
     * a fresh copy produces; what is pinned is that there is only ever ONE
     * array.
     *
     * <p>⚠️ IT DOES NOT PIN "NO COPY AT ALL", and round-2 review measured that
     * too: one shared scratch array plus a {@code System.arraycopy} into it per
     * consumer per chunk passes, because every consumer still sees exactly one
     * array and the same one. {@code SegmentSink}'s javadoc once claimed the
     * stronger property and has been narrowed to match what is actually
     * enforced -- memory O(chunk) rather than O(K x chunk).
     */
    @Test
    void ONEArrayServesEveryHandOffToEveryConsumer() throws Exception {
        CountingBinStore store = storeHolding(segment());
        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(16);
        new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, consumers);
        java.util.Set<Integer> all = new java.util.HashSet<>();
        for (SegmentProxyFixtures.RecordingSink c : consumers) {
            assertThat(c.arrayIdentities)
                    .as("a consumer handed more than one array is being copied to")
                    .hasSize(1);
            all.addAll(c.arrayIdentities);
        }
        assertThat(all)
                .as("and every consumer must see the SAME array, or the copy is per consumer")
                .hasSize(1);
    }

    /**
     * Criterion 4: every consumer receives exactly the segment, in order.
     *
     * <p>⚠️ WITHOUT THIS, FLAT MEMORY IS FREE. A proxy that delivered nothing
     * at all, or the first chunk only, passes every other assertion in this
     * file: no read count moves, no hand-off exceeds a chunk, and the peak is
     * identical at both fan-outs.
     */
    @Test
    void EVERYConsumerReceivesEXACTLYTheSegmentInOrder() throws Exception {
        byte[] expected = segment();
        CountingBinStore store = storeHolding(expected);
        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(64);
        int served = new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()).streamTo(KEY, consumers);
        assertThat(served).isEqualTo(64);
        for (SegmentProxyFixtures.RecordingSink c : consumers) {
            assertThat(c.received.toByteArray())
                    .as("byte for byte, not merely the right length")
                    .isEqualTo(expected);
        }
    }

    /**
     * A sink that BLOCKS is dropped, and every other consumer is served WHOLE.
     *
     * <p>⚠️ THE FAILURE CHAIN THIS PREVENTS IS CONCRETE. Consumer 7 of 64
     * stalls on a zero TCP window and holds the serving thread, the shared
     * chunk buffer and the open store {@code InputStream}; the store connection
     * idles past its read timeout and throws; {@code streamTo} propagates, and
     * all 64 sinks are left holding a TRUNCATED PREFIX they cannot tell from a
     * whole segment. That is the outcome this method's own "the read is not
     * abandoned" paragraph exists to prevent, and only the THROWING half of it
     * was implemented.
     *
     * <p>⚠️ DROPPED, NOT BUFFERED, and the row that opened this asked whoever
     * took it to decide. Buffering a slow consumer reintroduces per-consumer
     * memory, which criterion 6 forbids by name; dropping costs that consumer
     * nothing it cannot recover, because it replays from the commit log
     * exactly as a dead one does. A slow consumer and a dead consumer get the
     * same answer, which is also the simplest contract to state.
     *
     * <p>⚠️ THE WHOLE-SEGMENT ASSERTIONS ARE THE OTHER HALF OF THE PROPERTY,
     * and they are here rather than in a case of their own: a serving loop that
     * gave up on the SEGMENT when one sink stalled would also leave the stalled
     * consumer uncounted, so the count alone does not separate "dropped the
     * slow one" from "failed the whole fan-out". What separates them is that
     * the survivors hold every byte. ⚠️ A SEPARATE CASE FOR IT WOULD HAVE HAD
     * NO RED RECORD: the unfixed loop already delivered the whole segment to a
     * live sink -- it merely blocked first -- so that assertion passes before
     * and after, and testing.md rule 2 wants a test observed failing first.
     */
    @Test
    void aSinkThatBLOCKSIsDroppedAndTheOthersAreServedWHOLE() throws Exception {
        byte[] bytes = segment();
        SegmentProxy proxy = new SegmentProxy(storeHolding(bytes), SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        BlockingSegmentSink stalled = new BlockingSegmentSink();
        RecordingSink first = new RecordingSink();
        RecordingSink second = new RecordingSink();

        List<SegmentSink> dropped = proxy.streamTo(KEY, List.of(first, stalled, second),
                Duration.ofMillis(200));

        assertThat(stalled.writes())
                .as("PREMISE: the serving path did hand the stalled sink a chunk -- without "
                        + "this the count below would pass against a fan-out that never "
                        + "reached it")
                .isPositive();
        assertThat(dropped)
                .as("the stalled consumer is NAMED as the one that went, which a count of "
                        + "survivors cannot say -- identity against an equal-comparing twin is "
                        + "a separate case, because `containsExactly` compares with `equals`")
                .containsExactly(stalled);
        assertThat(first.received.toByteArray())
                .as("a live consumer gets the WHOLE segment")
                .isEqualTo(bytes);
        assertThat(second.received.toByteArray())
                .as("and so does the other one")
                .isEqualTo(bytes);
        stalled.release();
    }


    /**
     * A sink that IGNORES interruption does not hold the call.
     *
     * <p>⚠️ THE CASE THAT WOULD HAVE CAUGHT THE FIRST FIX BEING WRONG. That
     * version closed its executor with try-with-resources, and {@code
     * ExecutorService.close()} is {@code shutdown()} plus an UNBOUNDED {@code
     * awaitTermination} -- so the deadline dropped the slow sink from the
     * fan-out and the call then waited for it anyway, holding the store {@code
     * InputStream} open, which is the stall this method exists to remove.
     * Review MEASURED 202 ms to leave {@code invokeAll} and 4,011 ms to leave
     * the try block. Every case here passed, because the other fixture returns
     * when interrupted and so cannot tell waiting from walking away.
     *
     * <p>⚠️ THE ASSERTION IS ORDERING, NOT TIMING: if {@code streamTo} has
     * returned while the sink is STILL INSIDE {@code write}, the serving path
     * did not wait for it. Asking the same question with a stopwatch would be
     * the flaky way.
     */
    @Test
    void aSinkThatIGNORESInterruptionDoesNotHoldTheCall() throws Exception {
        byte[] bytes = segment();
        SegmentProxy proxy = new SegmentProxy(storeHolding(bytes), SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        UninterruptibleSegmentSink deaf = new UninterruptibleSegmentSink();
        RecordingSink live = new RecordingSink();

        List<SegmentSink> dropped = proxy.streamTo(KEY, List.of(deaf, live), Duration.ofMillis(200));

        assertThat(deaf.everEntered())
                .as("PREMISE: the serving path did hand this sink a chunk")
                .isTrue();
        assertThat(deaf.stillInsideWrite())
                .as("streamTo returned while the deaf sink was still in write(), so it walked "
                        + "away rather than waiting -- false if the executor is closed with "
                        + "try-with-resources")
                .isTrue();
        assertThat(dropped).as("and the deaf sink is the one named as dropped")
                .containsExactly(deaf);
        assertThat(live.received.toByteArray())
                .as("which still received the whole segment")
                .isEqualTo(bytes);
        deaf.release();
    }

    /**
     * Two sinks dropped from one fan-out come back in SUPPLY order (M5.58a).
     *
     * <p>⚠️ THE ORDER IS A PROPERTY OF THE PARAMETER, NOT OF THE REMOVAL LOOP.
     * {@code handOff} removes backwards so that removal needs no copy per
     * chunk, so a list appended to as sinks go would hand a caller the drops of
     * one chunk in reverse -- and a caller pairing this list against the one it
     * passed in, which is what {@code SegmentServingPath.deliver} does by index
     * on its own parallel lists, would pair the wrong ones. TWO stalled sinks
     * either side of a live one is the smallest fixture that tells the two
     * orders apart.
     *
     * <p>⚠️ AND THE LIVE SINK IN THE MIDDLE IS NOT DECORATION: with the drops
     * adjacent, a reversal is invisible at this length.
     */
    @Test
    void TWODroppedSinksComeBackInSUPPLYOrder() throws Exception {
        byte[] bytes = segment();
        SegmentProxy proxy = new SegmentProxy(storeHolding(bytes), SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        BlockingSegmentSink firstStalled = new BlockingSegmentSink();
        RecordingSink live = new RecordingSink();
        BlockingSegmentSink secondStalled = new BlockingSegmentSink();

        List<SegmentSink> dropped = proxy.streamTo(KEY,
                List.of(firstStalled, live, secondStalled), Duration.ofMillis(200));

        assertThat(firstStalled.writes())
                .as("PREMISE: the serving path really handed the first stalled sink a chunk")
                .isPositive();
        assertThat(dropped)
                .as("both stalled sinks are named, in the order they were SUPPLIED -- "
                        + "reversed if the list is built as `handOff` removes")
                .containsExactly(firstStalled, secondStalled);
        assertThat(live.received.toByteArray())
                .as("and the consumer between them still received the whole segment")
                .isEqualTo(bytes);
        firstStalled.release();
        secondStalled.release();
    }

    /**
     * A sink is dropped by IDENTITY, not by {@code equals} (M5.58a).
     *
     * <p>⚠️ THE PROPERTY WAS CLAIMED IN FOUR PLACES AND PINNED IN NONE, which
     * both reviewers measured independently: replacing the whole identity loop
     * with {@code dropped.removeAll(live)} left every case in the module green,
     * because no fixture in the tree overrode {@code equals} -- so
     * {@code equals} WAS {@code ==} for every sink that existed and the two
     * matchers could not disagree. A {@code SegmentSink} is caller-supplied and
     * may implement {@code equals} however it likes, which is the whole reason
     * the production loop does not use it.
     *
     * <p>⚠️ THE ASSERTION IS {@code isSameAs}, NOT {@code containsExactly}:
     * AssertJ compares with {@code equals} too, so against this fixture the
     * usual matcher cannot tell the right sink from its twin either.
     *
     * <p>⚠️ AND THE DIRECTION OF THE FAILURE IS THE POINT. Under
     * {@code removeAll} the list comes back EMPTY, not holding both:
     * {@code removeAll} strips from the drop list every element equal to a
     * SURVIVOR, and the stalled sink compares equal to the survivor, so it
     * removes the very sink that went. Under-reporting is the worse direction
     * -- over-reporting loses a healthy subscriber, which recovers from the
     * log, while under-reporting completes a sink that was handed spliced
     * bytes, which is the silent error this row exists to prevent.
     */
    @Test
    void aSinkIsDroppedByIDENTITYNotByEQUALS() throws Exception {
        byte[] bytes = segment();
        SegmentProxy proxy = new SegmentProxy(storeHolding(bytes), SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger());
        EqualToEveryOtherSink stalled = new EqualToEveryOtherSink(true);
        EqualToEveryOtherSink healthy = new EqualToEveryOtherSink(false);

        List<SegmentSink> dropped = proxy.streamTo(KEY, List.of(stalled, healthy),
                Duration.ofMillis(200));

        assertThat(stalled.equals(healthy))
                .as("PREMISE: these two sinks COMPARE EQUAL, or this case is the identity one "
                        + "written twice")
                .isTrue();
        assertThat(stalled)
                .as("PREMISE: and they are DISTINCT OBJECTS -- collapsing the two fixtures "
                        + "leaves every assertion below green while the case stops separating "
                        + "`==` from `equals` at all")
                .isNotSameAs(healthy);
        assertThat(stalled.writes())
                .as("PREMISE: the serving path did hand the stalled sink a chunk")
                .isPositive();
        assertThat(dropped)
                .as("ONE sink went, and it is the stalled one -- `removeAll` strips from the "
                        + "drop list every element EQUAL to a SURVIVOR, so it reports NOBODY as "
                        + "dropped and the list comes back empty")
                .singleElement()
                .isSameAs(stalled);
        stalled.release();
    }
}
