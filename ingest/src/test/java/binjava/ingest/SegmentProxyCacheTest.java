// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * One fetch per node across SEPARATE publishes (M5.40b, cost.md R5, NFR-4).
 *
 * <p>⚠️ REPEATS WITHIN ONE PUBLISH WERE ALREADY MERGED and are not what this is
 * about: {@code streamTo} takes the whole consumer list and reads once for all
 * of it, which {@code SegmentProxyTest} pins. What could not be merged is a
 * repeat ACROSS publishes -- a late subscriber is by construction not in a list
 * that was already served, and the second publish has not happened when the
 * first is being served. So a call-site rule cannot close it and a cache can.
 */
class SegmentProxyCacheTest {

    /**
     * ⚠️ NON-REPEATING, AND "DISTINCT" WAS NOT ENOUGH. Review measured an
     * all-zero fixture making every content assertion vacuous -- writing the
     * FIRST chunk four times instead of four different chunks passed, because
     * {@code isEqualTo} on two zero-filled arrays of equal length is true
     * whatever order the bytes arrived in. The first fix was
     * {@code b[i] = i * 31 + 7}, which is STILL vacuous here and measurably so:
     * that sequence has period 256, the chunk size is 1024, and 31 x 1024 is
     * 0 mod 256 -- so every chunk is byte-identical and repeating the first one
     * reproduces the whole array. A seeded {@code Random} has no period at this
     * scale, and the seed keeps it reproducible.
     */
    /**
     * ⚠️ 4097, AND THE ODD BYTE IS THE POINT. At 4096 with a 1024 chunk the
     * doubling ladder lands EXACTLY, so {@code admitted == admitting.length}
     * in every case here and the trim is only ever taken in its no-op
     * direction -- review measured {@code cache.put(key, admitting)} without
     * the trim surviving the whole suite. In production a segment is any size
     * up to the flush trigger, so an untrimmed accumulator carries trailing
     * ZERO PADDING and every late subscriber is handed a segment with garbage
     * appended, silently, which is the precise opposite of what this row is
     * for.
     */
    private static final byte[] SEGMENT = nonRepeatingBytes(4097);

    private static byte[] nonRepeatingBytes(int size) {
        byte[] b = new byte[size];
        new java.util.Random(20260913L).nextBytes(b);
        return b;
    }

    /**
     * Two segments through one proxy do not become one.
     *
     * <p>⚠️ REVIEW MEASURED BOTH KEY ARGUMENTS REPLACED BY A CONSTANT SURVIVING
     * the whole tree -- every other case here puts one key through one cache,
     * so nothing observed that the cache is keyed BY SEGMENT at all. The
     * consequence is not a lost saving: consumers asking for segment B are
     * handed segment A's bytes, silently, for the life of the entry.
     */
    @Test
    void TWOSegmentsThroughONEProxyKeepTheirOWNBytes() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] other = nonRepeatingBytes(2048);
        store.put("seg-a", Body.ofBytes(SEGMENT));
        store.put("seg-b", Body.ofBytes(other));
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(1 << 20));

        List<byte[]> firstA = new ArrayList<>();
        proxy.streamTo("seg-a", sinks(1, firstA));
        List<byte[]> firstB = new ArrayList<>();
        proxy.streamTo("seg-b", sinks(1, firstB));
        List<byte[]> againA = new ArrayList<>();
        proxy.streamTo("seg-a", sinks(1, againA));
        List<byte[]> againB = new ArrayList<>();
        proxy.streamTo("seg-b", sinks(1, againB));

        assertThat(firstA.get(0)).isEqualTo(SEGMENT);
        assertThat(firstB.get(0)).isEqualTo(other);
        assertThat(againA.get(0))
                .as("a hit on A is A, not whatever was cached last")
                .isEqualTo(SEGMENT);
        assertThat(againB.get(0))
                .as("and a hit on B is B")
                .isEqualTo(other);
        assertThat(store.counts().gets())
                .as("two segments, two reads -- one each, and no more")
                .isEqualTo(2);
    }

    /**
     * A consumer that throws on a HIT is dropped, and the others are served.
     *
     * <p>⚠️ REVIEW MEASURED THE HIT PATH'S DEAD-CONSUMER DROP UNPINNED: the
     * miss path's identical logic is covered in
     * {@code AssembledServingPathFailureTest} and
     * {@code SegmentProxyFailureTest}, and the hit path -- the one production
     * takes for every late subscriber -- had no throwing-sink case at all.
     * Emptying its {@code catch} survived. ⚠️ AN EARLIER DRAFT OF THIS
     * SENTENCE CITED {@code SegmentProxyTest}, which holds no throwing sink at
     * all; a maintainer following it would have found nothing.
     */
    @Test
    void aConsumerThatTHROWSOnAHitIsDroppedAndTheOthersAreSERVED() throws Exception {
        CountingBinStore store = storeHolding("seg-hot");
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(1 << 20));
        proxy.streamTo("seg-hot", sinks(1, new ArrayList<>()));

        java.io.ByteArrayOutputStream healthy = new java.io.ByteArrayOutputStream();
        java.util.concurrent.atomic.AtomicInteger deadWrites =
                new java.util.concurrent.atomic.AtomicInteger();
        int stillLive = proxy.streamTo("seg-hot", List.of(
                (buffer, offset, length) -> {
                    deadWrites.incrementAndGet();
                    throw new IOException("this consumer went away");
                },
                (buffer, offset, length) -> healthy.write(buffer, offset, length)));

        assertThat(deadWrites.get())
                .as("written to once and then dropped, not re-offered every chunk")
                .isEqualTo(1);
        assertThat(healthy.toByteArray())
                .as("and the healthy consumer still gets the whole segment")
                .isEqualTo(SEGMENT);
        assertThat(stillLive).as("one of the two survived").isEqualTo(1);
        assertThat(store.counts().gets())
                .as("and this was the HIT path, which is what the case is named for -- every "
                        + "other assertion here is produced identically by a miss")
                .isEqualTo(1);
    }

    private static CountingBinStore storeHolding(String key) throws IOException {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(key, Body.ofBytes(SEGMENT));
        return store;
    }

    private static List<SegmentSink> sinks(int k, List<byte[]> into) {
        List<SegmentSink> list = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            into.add(null);
            int slot = i;
            list.add((buffer, offset, length) -> {
                out.write(buffer, offset, length);
                into.set(slot, out.toByteArray());
            });
        }
        return list;
    }

    @Test
    void aFanOutOfSIXTYFOURPlusThreeLATESubscribersIsONEGetNotFOUR() throws Exception {
        CountingBinStore store = storeHolding("seg-hot");
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(1 << 20));
        long before = store.counts().gets();

        List<byte[]> firstWave = new ArrayList<>();
        proxy.streamTo("seg-hot", sinks(64, firstWave));
        for (int late = 0; late < 3; late++) {
            List<byte[]> latecomer = new ArrayList<>();
            proxy.streamTo("seg-hot", sinks(1, latecomer));
            assertThat(latecomer.get(0))
                    .as("a late subscriber gets the SEGMENT, not an empty hit")
                    .isEqualTo(SEGMENT);
        }

        assertThat(store.counts().gets() - before)
                .as("four publishes of one object on one node cost ONE read")
                .isEqualTo(1);
        assertThat(firstWave.get(0)).isEqualTo(SEGMENT);
        assertThat(firstWave.get(63)).isEqualTo(SEGMENT);
    }

    /**
     * A HIT is handed over a chunk at a time, like a miss.
     *
     * <p>⚠️ REVIEW MEASURED {@code writeChunked} COLLAPSED TO ONE
     * SEGMENT-SIZED WRITE SURVIVING. The miss path has this pinned in
     * {@code SegmentProxyTest}; the hit path -- the one already holding the
     * whole segment, and so the one that can most easily become the
     * buffer-then-forward ADR-0004 forbids -- had nothing.
     */
    @Test
    void aHITIsHandedOverACHUNKAtATimeAndNotInONEWrite() throws Exception {
        CountingBinStore store = storeHolding("seg-hot");
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(1 << 20));
        proxy.streamTo("seg-hot", sinks(1, new ArrayList<>()));

        List<Integer> sliceSizes = new ArrayList<>();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        proxy.streamTo("seg-hot", List.of((buffer, offset, length) -> {
            sliceSizes.add(length);
            out.write(buffer, offset, length);
        }));

        assertThat(sliceSizes)
                .as("no consumer is handed more than a chunk, however much WE hold")
                .allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(1024));
        assertThat(sliceSizes)
                .as("and 4097 bytes at 1024 a time is five writes, not one")
                .hasSize(5);
        assertThat(out.toByteArray())
                .as("reassembled in order, so the chunking is real rather than repeated")
                .isEqualTo(SEGMENT);
    }

    /**
     * A segment EXACTLY the size of the ceiling fits.
     *
     * <p>⚠️ REVIEW MEASURED THREE {@code >} TO {@code >=} FLIPS SURVIVING,
     * because no fixture sat on the boundary. Off by one here is a cache that
     * silently holds nothing whenever the ceiling is set to exactly one
     * segment -- which is the most obvious way to configure it.
     */
    @Test
    void aSegmentEXACTLYTheSizeOfTheCeilingIsCACHED() throws Exception {
        CountingBinStore store = storeHolding("seg-exact");
        SegmentCache cache = new SegmentCache(SEGMENT.length);
        SegmentProxy proxy = new SegmentProxy(store, 1024, cache);

        proxy.streamTo("seg-exact", sinks(1, new ArrayList<>()));
        List<byte[]> second = new ArrayList<>();
        proxy.streamTo("seg-exact", sinks(1, second));

        assertThat(cache.bytesHeld())
                .as("exactly the ceiling is within the ceiling")
                .isEqualTo(SEGMENT.length);
        assertThat(second.get(0)).isEqualTo(SEGMENT);
        assertThat(store.counts().gets()).as("so the second publish is a hit").isEqualTo(1);
    }

    /**
     * A read that dies part way caches NOTHING -- never the prefix it got.
     *
     * <p>⚠️ THE DEFECT THIS FORBIDS IS SILENT DATA LOSS, not a failed publish.
     * Caching what was read before the failure would hand the NEXT subscriber a
     * truncated segment with no error anywhere, and it would hit rather than
     * re-read, so the truncation would persist for as long as the entry lived.
     */
    @Test
    void aREADThatDIEDPartWayCachesNOTHING() throws Exception {
        CountingBinStore store = new CountingBinStore(
                new StoreFakes.ReadThrowsPartWayThrough(new MemoryBinStore(), 2048));
        store.put("seg-torn", Body.ofBytes(SEGMENT));
        SegmentCache cache = new SegmentCache(1 << 20);
        SegmentProxy proxy = new SegmentProxy(store, 1024, cache);

        assertThatThrownBy(() -> proxy.streamTo("seg-torn", sinks(1, new ArrayList<>())))
                .isInstanceOf(IOException.class);

        assertThat(cache.bytesHeld())
                .as("a prefix is not a segment, so nothing is admitted")
                .isZero();
        assertThat(cache.get("seg-torn"))
                .as("and the next subscriber misses and reads again rather than hitting a stub")
                .isNull();
    }

    /**
     * With the cache off, every publish reads again -- the pre-M5.40b shape.
     *
     * <p>⚠️ THIS IS WHAT MAKES THE CASE ABOVE MEAN SOMETHING. Without it, a
     * {@code streamTo} that read nothing at all and handed every consumer an
     * empty array would also report one GET.
     */
    @Test
    void withTheCacheOFFEveryPublishReadsAGAIN() throws Exception {
        CountingBinStore store = storeHolding("seg-cold");
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(0));
        long before = store.counts().gets();

        for (int publish = 0; publish < 4; publish++) {
            List<byte[]> got = new ArrayList<>();
            proxy.streamTo("seg-cold", sinks(1, got));
            assertThat(got.get(0)).isEqualTo(SEGMENT);
        }

        assertThat(store.counts().gets() - before)
                .as("a zero ceiling is the off switch, and off means the old behaviour exactly")
                .isEqualTo(4);
    }

    /**
     * The pod a deployment actually runs has the cache ON.
     *
     * <p>⚠️ WITHOUT THIS, THE WHOLE ROW IS OPT-IN AND NOBODY OPTS IN. Every
     * other case here builds its own {@code SegmentProxy} with a cache handed
     * to it, so reverting {@code DefaultIngest} to {@code new SegmentProxy(store)}
     * -- the one-argument constructor, whose ceiling is zero -- leaves all of
     * them green while production issues a fresh GET for every repeat read.
     * That is the same shape as M5.40a, whose hub contract production does not
     * yet use (M5.62); this row does not repeat it.
     */
    @Test
    void theINGESTERAPodActuallyRunsHasTheCacheON() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        // ⚠️ 16 MiB, NOT THE DEFAULT 8. Review measured this case built with
        // the default size, where `4 x configured` and `DEFAULT_CAPACITY_BYTES`
        // are the same number -- so reverting the wiring to the hardcoded
        // constant, which is the round-1 cost regression, passed.
        long configuredSegmentBytes = 16L << 20;
        try (DefaultIngest ingest = new DefaultIngest(
                        IngestTestSupport.pinnedIntervalConfig(
                                IngestTestSupport.NEVER, configuredSegmentBytes),
                        store, IngestTestSupport.PREFIX, "pod1",
                        IngestTestSupport.sequencer(store, "pod1"), hub,
                        java.time.Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            // ⚠️ 64 MiB WRITTEN OUT, NOT `DEFAULT_SEGMENTS_HELD * configured`.
            // Review measured the self-referential form surviving
            // `DEFAULT_SEGMENTS_HELD = 4` -> `= 1`: both sides of the equality
            // move together, and `isNotEqualTo(DEFAULT_CAPACITY_BYTES)` still
            // holds because that constant moves too. The deployed ceiling
            // silently becomes ONE segment and nothing says so.
            assertThat(ingest.serving().proxy().cache().capacityBytes())
                    .as("four segments of the CONFIGURED size, as a number")
                    .isEqualTo(64L << 20);
            assertThat(ingest.serving().proxy().cache().capacityBytes())
                    .as("and that is not the default, or this case cannot see the difference")
                    .isNotEqualTo(SegmentCache.DEFAULT_CAPACITY_BYTES);
        }
    }

    /**
     * A segment too big for the ceiling is served, and served again, and the
     * ceiling is never breached.
     */
    @Test
    void aSegmentTOOBIGForTheCeilingIsStillSERVEDAndNeverRESIDENT() throws Exception {
        CountingBinStore store = storeHolding("seg-huge");
        SegmentCache cache = new SegmentCache(100);
        SegmentProxy proxy = new SegmentProxy(store, 1024, cache);

        List<byte[]> first = new ArrayList<>();
        proxy.streamTo("seg-huge", sinks(1, first));
        List<byte[]> second = new ArrayList<>();
        proxy.streamTo("seg-huge", sinks(1, second));

        assertThat(first.get(0)).as("streamed in full").isEqualTo(SEGMENT);
        assertThat(second.get(0)).as("and in full again").isEqualTo(SEGMENT);
        assertThat(cache.bytesHeld())
                .as("4 KiB never enters a 100-byte ceiling, not even briefly")
                .isZero();
        assertThat(store.counts().gets()).as("so both publishes read").isEqualTo(2);
    }
}
