// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The cache that makes a repeat read cost no GET, and what it costs to have it.
 *
 * <p>⚠️ A CACHE IS A NEW RESIDENCY BOUND, which is why every case here is about
 * the ceiling rather than about hit rate. {@code SegmentProxy}'s memory was
 * one chunk however many consumers or segments; a cache adds C, and C is a
 * number somebody has to be able to trust. M5's criterion 6 cannot check it:
 * it asks that service memory be FLAT IN K with a bound independent of K, and
 * a cache of capacity C adds C at every K -- still flat, still K-independent --
 * so nothing measuring criterion 6 distinguishes C = 8 MiB from C = 8 GiB.
 */
class SegmentCacheTest {

    private static byte[] segmentOf(int size, byte fill) {
        byte[] b = new byte[size];
        java.util.Arrays.fill(b, fill);
        return b;
    }

    @Test
    void aSegmentPutIsHandedBackBYTEForBYTE() {
        SegmentCache cache = new SegmentCache(1024);
        byte[] segment = segmentOf(100, (byte) 7);

        cache.put("seg-a", segment);

        assertThat(cache.get("seg-a"))
                .as("a hit is the segment, not a prefix and not a different object's bytes")
                .isEqualTo(segment);
        assertThat(cache.get("seg-never-written"))
                .as("and a miss is null, which is what makes the caller issue its GET")
                .isNull();
    }

    @Test
    void theCEILINGHoldsHoweverManySegmentsArePut() {
        SegmentCache cache = new SegmentCache(250);

        for (int i = 0; i < 50; i++) {
            cache.put("seg-" + i, segmentOf(100, (byte) i));
            assertThat(cache.bytesHeld())
                    .as("after put %d the cache must still be within its ceiling", i)
                    .isLessThanOrEqualTo(250);
        }

        assertThat(cache.get("seg-0"))
                .as("the oldest is gone -- a cache that never evicted would hold 5,000 bytes")
                .isNull();
        assertThat(cache.get("seg-49"))
                .as("and the newest is there, or eviction has eaten what it just stored")
                .isNotNull();
    }

    /**
     * A segment bigger than the whole ceiling is REFUSED, not admitted and then
     * evicted down to nothing.
     *
     * <p>⚠️ ADMITTING IT FIRST IS THE BUG THIS FORBIDS: a cache that stores
     * then evicts holds the oversized array for the width of that window, so
     * the ceiling a reader was promised is exceeded by the one object most
     * likely to exhaust the heap.
     */
    @Test
    void aSegmentLARGERThanTheCeilingIsREFUSEDRatherThanAdmittedAndEvicted() {
        SegmentCache cache = new SegmentCache(250);
        cache.put("seg-small", segmentOf(100, (byte) 1));

        cache.put("seg-huge", segmentOf(1000, (byte) 2));

        assertThat(cache.get("seg-huge"))
                .as("too big to hold, so it is not held")
                .isNull();
        assertThat(cache.bytesHeld())
                .as("and it did not displace what was already there on its way past")
                .isEqualTo(100);
        assertThat(cache.get("seg-small")).isNotNull();
    }

    /**
     * A HIT keeps a segment alive; an untouched older one goes first.
     *
     * <p>⚠️ WITHOUT THIS, INSERTION ORDER IS ALL THAT MATTERS and the segment
     * every late subscriber is asking for is evicted while one nobody wants
     * survives -- which is the exact case M5.40b exists for.
     */
    @Test
    void aSegmentSTILLBeingReadSurvivesAnOlderUntouchedOne() {
        SegmentCache cache = new SegmentCache(250);
        cache.put("seg-old", segmentOf(100, (byte) 1));
        cache.put("seg-new", segmentOf(100, (byte) 2));

        assertThat(cache.get("seg-old")).as("touch the older one").isNotNull();
        cache.put("seg-newest", segmentOf(100, (byte) 3));

        assertThat(cache.get("seg-old"))
                .as("recently read, so it stays")
                .isNotNull();
        assertThat(cache.get("seg-new"))
                .as("untouched since it was stored, so it is the one that goes")
                .isNull();
    }

    /**
     * Re-putting a key does not leak its old size into the accounting.
     *
     * <p>⚠️ REVIEW MEASURED DELETING THE DECREMENT SURVIVING. It is reachable:
     * M5.63's overlapping publishes both miss and both admit the same key,
     * after which {@code bytesHeld()} over-reports permanently and can exceed
     * {@code capacityBytes()} -- so the number this class tells an operator to
     * trust breaks its own javadoc, and eviction starts throwing away live
     * entries to satisfy a total that is not real.
     */
    @Test
    void REPUTTINGAKeyReplacesItsBytesRatherThanAddingThem() {
        SegmentCache cache = new SegmentCache(250);

        cache.put("seg-a", segmentOf(100, (byte) 1));
        cache.put("seg-a", segmentOf(100, (byte) 2));
        cache.put("seg-a", segmentOf(100, (byte) 3));

        assertThat(cache.bytesHeld())
                .as("one key, one entry, one hundred bytes -- not three hundred")
                .isEqualTo(100);
        assertThat(cache.get("seg-a"))
                .as("and the newest bytes are what a reader gets")
                .isEqualTo(segmentOf(100, (byte) 3));
    }

    /**
     * A ceiling of exactly N segments holds N of them, not N-1.
     *
     * <p>⚠️ REVIEW MEASURED THE EVICTION {@code >} FLIPPED TO {@code >=}
     * SURVIVING, because the boundary case that exists pins the REFUSAL
     * boundary and its cache is empty, so the eviction loop never runs. Under
     * the flip a four-segment ceiling permanently holds three, which is a
     * quarter of the cache silently unavailable.
     */
    @Test
    void aCeilingOfEXACTLYNSegmentsHoldsNOfThem() {
        SegmentCache cache = new SegmentCache(300);

        cache.put("seg-1", segmentOf(100, (byte) 1));
        cache.put("seg-2", segmentOf(100, (byte) 2));
        cache.put("seg-3", segmentOf(100, (byte) 3));

        assertThat(cache.bytesHeld()).isEqualTo(300);
        assertThat(cache.get("seg-1")).as("the first still fits").isNotNull();
        assertThat(cache.get("seg-2")).isNotNull();
        assertThat(cache.get("seg-3")).isNotNull();
    }

    @Test
    void aNEGATIVECeilingIsRefusedAndZeroCachesNOTHING() {
        assertThatThrownBy(() -> new SegmentCache(-1))
                .isInstanceOf(IllegalArgumentException.class);

        SegmentCache off = new SegmentCache(0);
        off.put("seg-a", segmentOf(1, (byte) 1));
        assertThat(off.get("seg-a"))
                .as("a zero ceiling is the OFF switch, not a cache of one")
                .isNull();
        assertThat(off.bytesHeld()).isZero();
    }
}
