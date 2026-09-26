// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One fetch per (node, segment), shared by every run this node holds
 * (M5.45h, FR-6, cost.md R5, non-negotiable 6).
 *
 * <p>⚠️ THE SHARING UNIT IS THE NODE, AND NO TYPE IN {@code client}
 * REPRESENTS ONE. A node gets one {@code Delivery} per RUN, each carrying the
 * same key-scoped grant, so a {@link SegmentSource} owned by a
 * {@code ConsumerClient} fetches once per run: a catch-up node holding ~400
 * runs of one 8 MiB segment issues ~400 whole-object GETs. That is
 * shards-per-node scaling, which non-negotiable 6 forbids by name and which
 * nothing in the ingester can see, because a consumer-side GET is invisible to
 * its {@code CountingBinStore}. {@link NodeSubscriptions} is what owns a node's
 * clients, so this is what it hands all of them.
 *
 * <p>⚠️ THE COUNT IS OF INVOCATIONS, NOT OF DISTINCT URLS, and the difference
 * was measured on M5.45d: every delivery of one segment carries THE SAME
 * grant, and {@link Grant} is a record with value equality, so a set of
 * requested urls has size 1 under the per-run implementation as well as under
 * this one. {@code fetches()} counts calls that reached the delegate.
 *
 * <p>⚠️ IT HOLDS BYTES, AND THE HOLD IS BOUNDED, because 64 deliveries arrive
 * at 64 independent {@code readNext} calls: unioning them needs either a
 * barrier or a per-node hold of the segment, and a hold is a NEW memory bound
 * rather than a free one. M5's criterion 6 asks that service memory be flat in
 * K, and a hold of C is flat in K at every C -- so the bound is a byte ceiling
 * with eviction, asserted, rather than a number in a javadoc.
 *
 * <p>⚠️ {@code fetch} IS SERIALISED PER KEY, not per node. A slow GET for one
 * segment must not hold a different segment's fetch hostage, while concurrent
 * deliveries for the same key still share one in-flight fetch. The cache state
 * has its own short lock, never held across the delegate's network round trip.
 */
public final class NodeSegmentSource implements SegmentSource,
        io.github.huyz0.os.biningester.client.ProxySource {

    private final SegmentSource delegate;
    private final io.github.huyz0.os.biningester.client.ProxySource proxy;
    private final long capacityBytes;
    private final Object cacheLock = new Object();
    private long fetches;
    private long bytesHeld;
    private final Map<String, KeyGate> keyGates = new HashMap<>();

    /**
     * ⚠️ ACCESS-ORDERED, so what falls out is the segment nobody has asked for
     * in a while rather than the one this node is catching up on. Eviction is
     * by BYTES rather than by entry count: segments are not one size, and a
     * ceiling in entries bounds nothing an operator can size a heap against.
     */
    private final Map<String, byte[]> held = new LinkedHashMap<>(16, 0.75f, true);

    private static final class KeyGate {
        private int users;
    }

    public NodeSegmentSource(SegmentSource delegate, long capacityBytes) {
        this(delegate, null, capacityBytes);
    }

    /**
     * Also coalescing {@code proxy} fetches (M10.2): K runs of one segment on
     * this node cost one request to the ingester's route, held under the same
     * byte ceiling as {@code direct} fetches.
     */
    public NodeSegmentSource(SegmentSource delegate,
            io.github.huyz0.os.biningester.client.ProxySource proxy, long capacityBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.proxy = proxy;
        if (capacityBytes < 0) {
            throw new IllegalArgumentException(
                    "a negative hold is not a bound; got " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
    }

    /**
     * The segment's bytes, fetched at most once per (node, segment) while the
     * hold keeps them.
     *
     * <p>⚠️ A FAILURE IS NOT HELD AND NOT SWALLOWED. Nothing is put in the map
     * on a throw, so the next run's read tries again rather than inheriting an
     * empty array -- and the throw reaches every one of the K runs, which is
     * M5.40a's all-or-none rule arriving where an operator sees it. Returning
     * an empty array instead would advance K offsets past records nobody read:
     * silent data loss, and the failure mode M5.45g's criterion 5 names one
     * layer down.
     */
    @Override
    public byte[] fetch(Grant grant) throws IOException {
        Objects.requireNonNull(grant, "grant");
        return fetchOnce(grant.url(), () -> delegate.fetch(grant));
    }

    /**
     * A {@code proxy} segment's bytes, fetched at most once per (node, segment)
     * while the hold keeps them -- the same rules as {@link #fetch(Grant)}.
     *
     * <p>⚠️ KEYED APART FROM GRANT URLS, so a segment key can never collide
     * with a signed URL in the hold.
     */
    @Override
    public byte[] fetch(String segmentKey) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        if (proxy == null) {
            throw new IllegalStateException("this node holds no proxy source");
        }
        return fetchOnce("proxy:" + segmentKey, () -> proxy.fetch(segmentKey));
    }

    @FunctionalInterface
    private interface Fetch {
        byte[] get() throws IOException;
    }

    private byte[] fetchOnce(String key, Fetch fetch) throws IOException {
        KeyGate gate = acquire(key);
        try {
            synchronized (gate) {
                byte[] hit;
                synchronized (cacheLock) {
                    hit = held.get(key);
                }
                if (hit != null) {
                    return hit;
                }
                synchronized (cacheLock) {
                    fetches++;
                }
                byte[] bytes = fetch.get();
                synchronized (cacheLock) {
                    admit(key, bytes);
                }
                return bytes;
            }
        } finally {
            release(key, gate);
        }
    }

    private KeyGate acquire(String key) {
        synchronized (cacheLock) {
            KeyGate gate = keyGates.computeIfAbsent(key, ignored -> new KeyGate());
            gate.users++;
            return gate;
        }
    }

    private void release(String key, KeyGate gate) {
        synchronized (cacheLock) {
            if (--gate.users == 0) {
                keyGates.remove(key, gate);
            }
        }
    }

    private void admit(String url, byte[] bytes) {
        if (bytes == null || bytes.length > capacityBytes) {
            // ⚠️ TOO LARGE TO HOLD IS STILL SERVED -- the same array the
            // delegate returned, since nothing here copies. Holding it would
            // break the ceiling this class exists to keep; refusing to return
            // it would fail a read for a segment the node fetched
            // successfully. The cost is that this node's other runs of THAT
            // segment fetch again, which is the honest consequence of a hold
            // smaller than a segment -- and it is why the guard is here rather
            // than left to the eviction loop, which would admit the oversize
            // entry and evict EVERY held segment before dropping it again.
            return;
        }
        held.put(url, bytes);
        bytesHeld += bytes.length;
        var it = held.entrySet().iterator();
        while (bytesHeld > capacityBytes && it.hasNext()) {
            // ⚠️ THE ITERATOR IS IN ACCESS ORDER, so this removes the LEAST
            // recently used first -- the entry just admitted is the most
            // recent and is the last thing that can go.
            bytesHeld -= it.next().getValue().length;
            it.remove();
        }
    }

    /** Calls that reached the delegate -- criterion 1's count. */
    public long fetches() {
        synchronized (cacheLock) {
            return fetches;
        }
    }

    /** Bytes this node currently holds, never above the ceiling. */
    public long bytesHeld() {
        synchronized (cacheLock) {
            return bytesHeld;
        }
    }

    /** The ceiling this node's hold is bounded by. */
    public long capacityBytes() {
        return capacityBytes;
    }
}
