// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Whole segments held by key, so a repeat read costs no GET (M5.40b, NFR-4).
 *
 * <p>⚠️ WHY A CACHE AND NOT A CALL-SITE RULE. Repeats WITHIN one publish are
 * already merged: {@code SegmentProxy.streamTo} takes the whole consumer list
 * and reads once for all of it. What cannot be merged by the caller is a repeat
 * ACROSS publishes -- a late subscriber is by construction not in a list that
 * was already served, and the second publish has not happened when the first is
 * being served. One fan-out of 64 plus three late subscribers was four GETs for
 * one object on one node.
 *
 * <p>⚠️ AND A CACHE IS A NEW RESIDENCY BOUND, which is the cost of having it.
 * {@code SegmentProxy}'s memory was one chunk however many consumers and
 * however many segments; this adds C. So C owes an eviction rule, a ceiling,
 * and an assertion of its own -- M5's criterion 6 cannot supply one, because it
 * asks that service memory be flat in K with a bound independent of K, and a
 * cache of capacity C adds C at every K. Still flat, still K-independent, and
 * nothing measuring criterion 6 tells C = 8 MiB from C = 8 GiB.
 *
 * <p>⚠️ NOT A {@code BinStore} DECORATOR, which was the other place to put it.
 * A caching store would cache every GET in the process -- the commit log and
 * the chain included, where a stale hit is a correctness bug rather than a
 * saved request -- and {@code BinStore.get} returns an {@code InputStream},
 * so caching behind that seam means materialising inside the seam that exists
 * to avoid materialising. This sits BESIDE the store, knows it holds whole
 * segments, and bounds itself in bytes.
 *
 * <p>⚠️ THREAD SAFE, by the simplest means available: every method
 * synchronizes on the instance. That is enough here because each is
 * O(evicted) and none does I/O while holding the monitor, so the critical
 * section is bounded by work this class does itself. An earlier draft of this
 * paragraph opened "NOT THREAD SAFE BY ITSELF" and then described the lock
 * that makes it so, which is a contradiction rather than a caveat.
 *
 * <p>⚠️ WHAT THE LOCK DOES NOT GIVE is atomicity ACROSS calls: two callers
 * that both miss will both read and both admit. That is M5.63, and no lock
 * inside this class could fix it -- the read happens outside.
 */
public final class SegmentCache {

    /** How many segments at the configured maximum the default ceiling holds. */
    public static final int DEFAULT_SEGMENTS_HELD = 4;

    /**
     * Four segments at {@link IngestConfig#DEFAULT_MAX_SEGMENT_BYTES}.
     *
     * <p>⚠️ ONLY RIGHT FOR THE DEFAULT SEGMENT SIZE, which is why production
     * builds its cache with {@link #forSegmentsOf} instead. Review measured
     * the trap: with this constant hardcoded and {@code maxSegmentBytes}
     * configured to 64 MiB -- a size this project's own javadoc names -- every
     * segment fails the oversized check, {@link #put} returns silently,
     * {@link #bytesHeld()} reads zero forever, and every repeat read is a fresh
     * GET again. No log, no metric, no error, and every test green: the silent
     * regression M5.48 exists to make impossible, one file over.
     */
    public static final long DEFAULT_CAPACITY_BYTES =
            DEFAULT_SEGMENTS_HELD * IngestConfig.DEFAULT_MAX_SEGMENT_BYTES;

    /**
     * A ceiling of {@link #DEFAULT_SEGMENTS_HELD} times this deployment's
     * {@code maxSegmentBytes}.
     *
     * <p>⚠️ THAT IS A FLUSH TRIGGER, NOT A SEGMENT CAP, and an earlier draft of
     * this sentence called it "the size THIS deployment is configured to
     * write", which overstates it. {@link IngestConfig} documents it as the
     * size at which a flush FIRES, so a single large {@code _bulk} body can
     * produce a segment above it. Such a segment is served uncached -- the
     * pre-M5.40b behaviour rather than a failure: the read happens, every
     * consumer is served, and only the saving is lost. ⚠️ AND NOT BY THIS
     * CLASS REFUSING IT, which an earlier draft of this sentence said:
     * {@code SegmentProxy} abandons its accumulator the moment the running
     * total would cross the ceiling, so an oversized segment never reaches
     * {@link #put} at all. {@link #put}'s own refusal is the backstop for a
     * caller that offers one anyway.
     *
     * <p>⚠️ FOUR IS CHOSEN, NOT DERIVED, and saying so is the point. It is
     * enough that a late subscriber to any of the last few publishes hits,
     * which is the case M5.40b names. It is NOT computed from a heap budget --
     * if serving-path residency turns out to matter, this is the first number
     * to revisit.
     */
    public static SegmentCache forSegmentsOf(long maxSegmentBytes) {
        if (maxSegmentBytes <= 0) {
            throw new IllegalArgumentException(
                    "a segment size of " + maxSegmentBytes + " caches nothing");
        }
        return new SegmentCache(DEFAULT_SEGMENTS_HELD * maxSegmentBytes);
    }

    private final long capacityBytes;
    private final Map<String, byte[]> held;
    private long bytesHeld;

    /**
     * @param capacityBytes the ceiling; {@code 0} disables caching entirely
     * @throws IllegalArgumentException if negative
     */
    public SegmentCache(long capacityBytes) {
        if (capacityBytes < 0) {
            throw new IllegalArgumentException(
                    "cache capacity must not be negative, was " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
        // ⚠️ ACCESS ORDER, not insertion order. The segment every late
        // subscriber is asking for is the one being read RIGHT NOW; under
        // insertion order it ages out while a segment nobody wants survives,
        // which is the exact case this class exists for.
        this.held = new LinkedHashMap<>(16, 0.75f, true);
    }

    /** The ceiling this cache was built with. */
    public long capacityBytes() {
        return capacityBytes;
    }

    /** How many bytes are resident now, which never exceeds {@link #capacityBytes()}. */
    public synchronized long bytesHeld() {
        return bytesHeld;
    }

    /** The segment's bytes, or {@code null} -- a miss, which the caller answers with a GET. */
    public synchronized byte[] get(String segmentKey) {
        Objects.requireNonNull(segmentKey, "segmentKey");
        return held.get(segmentKey);
    }

    /**
     * Offers a segment for caching. It may be refused, and a refusal is normal.
     *
     * <p>⚠️ TOO BIG IS REFUSED OUTRIGHT, never admitted and then evicted down
     * to nothing. Storing first would hold the oversized array for the width of
     * that window, so the ceiling a reader was promised is exceeded by the one
     * object most likely to exhaust the heap.
     *
     * <p>⚠️ THE ARRAY IS TAKEN BY REFERENCE, not copied. The caller has just
     * read it from the store and does not keep it; copying would double the
     * residency of every admission for nothing.
     */
    public synchronized void put(String segmentKey, byte[] segment) {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(segment, "segment");
        if (segment.length > capacityBytes) {
            return;
        }
        byte[] replaced = held.remove(segmentKey);
        if (replaced != null) {
            bytesHeld -= replaced.length;
        }
        Iterator<Map.Entry<String, byte[]>> oldestFirst = held.entrySet().iterator();
        while (bytesHeld + segment.length > capacityBytes && oldestFirst.hasNext()) {
            bytesHeld -= oldestFirst.next().getValue().length;
            oldestFirst.remove();
        }
        held.put(segmentKey, segment);
        bytesHeld += segment.length;
    }
}
