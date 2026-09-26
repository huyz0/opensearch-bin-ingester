// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One consumer's read of one segment, for the proxy route (M10.1, FR-6,
 * ADR-0073).
 *
 * <p>⚠️ **SINGLE-FLIGHT PER SEGMENT.** {@link SegmentProxy#streamTo} makes no
 * attempt to recognise that two calls name the same segment, so N consumers
 * arriving together on a cold segment would be N store GETs -- a rate scaling
 * with consumers of one segment rather than with segments. Here the first
 * cold read FILLS the node's {@link SegmentCache} under a per-key gate, and
 * every read, the first included, is then served from it.
 *
 * <p>⚠️ **AND THE GATE NEVER SPANS A CONSUMER'S SINK.** The fill writes to no
 * one; the gate is released before any byte goes to a socket. Holding it while
 * streaming to the first consumer would let one stalled consumer -- a zero TCP
 * window -- stall every other consumer of that segment on this node, which is
 * the failure {@code SegmentProxy}'s own javadoc names for a blocking sink.
 * The price is that a cold read's first byte waits for the whole store read
 * rather than the first chunk; on the tail path the publish-time serve and the
 * prefetcher (M8.56) have usually warmed the cache before a consumer asks.
 *
 * <p>⚠️ **A CACHE THAT CANNOT HOLD THE SEGMENT IS NOT FILLED TWICE.** With
 * capacity 0 a fill would read the whole object and keep none of it, so every
 * read would cost two GETs; such a node streams each read straight from the
 * store, one GET per read. A segment larger than the cache's ceiling is
 * discovered only by filling it once -- its size is counted as it streams
 * -- so its key is remembered in a bounded, least-recently-refused memory,
 * and later reads skip the fill: K
 * reads cost at most K + 1 GETs rather than 2K. A segment evicted between the
 * fill and the serve costs one more GET for that read -- bounded, and counted
 * by {@code CountingBinStore}.
 */
public final class SegmentReads {

    private final SegmentProxy proxy;
    private final Object gatesLock = new Object();
    private final Map<String, Gate> gates = new HashMap<>();

    /** How many refused keys are remembered; each entry is one key string. */
    static final int REFUSED_KEYS_REMEMBERED = 256;

    /** Keys the cache would not admit, guarded by {@link #gatesLock}. */
    private final Map<String, Boolean> refused =
            new java.util.LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > REFUSED_KEYS_REMEMBERED;
                }
            };

    private static final class Gate {
        private int users;

        /** The fill that failed while others waited; guarded by the gate. */
        private IOException failed;
    }

    public SegmentReads(SegmentProxy proxy) {
        this.proxy = Objects.requireNonNull(proxy, "proxy");
    }

    /**
     * Streams {@code segmentKey} to {@code sink} in {@link SegmentProxy}'s
     * chunks.
     *
     * @return whether the sink took the whole segment; false when it threw
     *     part-way, which {@link SegmentProxy} treats as a consumer gone
     * @throws IOException if the STORE fails -- before any byte reached the
     *     sink when the fill is what failed
     */
    public boolean serve(String segmentKey, SegmentSink sink) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(sink, "sink");
        SegmentCache cache = proxy.cache();
        if (cache.capacityBytes() > 0 && !wasRefused(segmentKey)
                && cache.get(segmentKey) == null) {
            fill(segmentKey, cache);
        }
        return proxy.streamTo(segmentKey, List.of(sink)) == 1;
    }

    private void fill(String segmentKey, SegmentCache cache) throws IOException {
        Gate gate = acquire(segmentKey);
        try {
            synchronized (gate) {
                // ⚠️ A FAILED FILL IS SHARED WITH ITS WAITERS. Each waiter
                // re-filling in turn would make N requests for an object the
                // store cannot serve cost N serial GETs, the last answered
                // after N store timeouts. The gate lives while anyone waits
                // on it, so the next arrival after they leave tries afresh.
                if (gate.failed != null) {
                    throw new IOException("the segment could not be read from the store",
                            gate.failed);
                }
                // Re-checked under the gate: the read that held it before us
                // has usually just admitted the segment.
                if (!wasRefused(segmentKey) && cache.get(segmentKey) == null) {
                    long[] size = {0};
                    try {
                        proxy.streamTo(segmentKey,
                                List.of((buffer, offset, length) -> size[0] += length));
                    } catch (IOException storeFailed) {
                        gate.failed = storeFailed;
                        throw storeFailed;
                    }
                    // ⚠️ REFUSED BY SIZE, NOT BY ABSENCE. A segment another
                    // path's admission evicted between the fill and this check
                    // is cacheable, and remembering it as refused would make
                    // every later read of it a GET.
                    if (size[0] > cache.capacityBytes()) {
                        synchronized (gatesLock) {
                            refused.put(segmentKey, Boolean.TRUE);
                        }
                    }
                }
            }
        } finally {
            release(segmentKey, gate);
        }
    }

    /** Gates currently held -- zero at rest, or the map leaks one per segment. */
    int gatesHeld() {
        synchronized (gatesLock) {
            return gates.size();
        }
    }

    /** Refused keys remembered -- never above {@link #REFUSED_KEYS_REMEMBERED}. */
    int refusedKeysRemembered() {
        synchronized (gatesLock) {
            return refused.size();
        }
    }

    private boolean wasRefused(String key) {
        synchronized (gatesLock) {
            return refused.get(key) != null;
        }
    }

    private Gate acquire(String key) {
        synchronized (gatesLock) {
            Gate gate = gates.computeIfAbsent(key, ignored -> new Gate());
            gate.users++;
            return gate;
        }
    }

    private void release(String key, Gate gate) {
        synchronized (gatesLock) {
            if (--gate.users == 0) {
                gates.remove(key, gate);
            }
        }
    }
}
