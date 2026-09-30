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
 * (M5.45h, FR-6, cost.md R5, non-negotiable 6) -- under a grant for
 * {@code direct}, and by segment key from the ingester's route for
 * {@code proxy} (M10.3).
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
public final class NodeSegmentSource implements SegmentSource {

    private final SegmentSource delegate;
    private final long capacityBytes;
    private final Object cacheLock = new Object();
    private long fetches;
    private long bytesHeld;
    private long oversizeFetches;
    private long refetchesAfterEviction;
    private volatile io.github.huyz0.os.biningester.client.SubscriptionMetrics metrics;

    /**
     * How many evicted keys are remembered, to tell a re-fetch from a first
     * fetch. ⚠️ BOUNDED, like the hold: key strings, not bytes, dropped oldest
     * first, so a re-fetch of a segment evicted more than this many evictions
     * ago goes uncounted -- an undercount, never a memory leak.
     */
    static final int EVICTED_KEYS_REMEMBERED = 4_096;

    private final Map<String, Boolean> evicted = new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > EVICTED_KEYS_REMEMBERED;
        }
    };

    /**
     * The first hold of a failed fetch: the consumer's own retry floor, so the
     * node fetches a failing segment about as often as ONE run's client would
     * retry it (M10.28).
     */
    static final java.time.Duration FAILURE_HOLD_FLOOR =
            io.github.huyz0.os.biningester.client.HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR;

    /** How long the doubling hold may grow: the consumer's retry ceiling. */
    static final java.time.Duration FAILURE_HOLD_CEILING =
            io.github.huyz0.os.biningester.client.HttpSubscriptionTransport.DEFAULT_RETRY_CEILING;

    /** A failed fetch this node answers without the delegate until {@code untilMillis}. */
    private record Failed(IOException cause, long untilMillis, int consecutive) {
    }

    /** Held failures, bounded as the evicted keys are; cleared by the key's next success. */
    private final Map<String, Failed> failed = new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Failed> eldest) {
            return size() > EVICTED_KEYS_REMEMBERED;
        }
    };

    /** The host's relative clock in millis, or {@code null}: then no failure is held. */
    private volatile java.util.function.LongSupplier failureClock;
    /** What a failure's backoff becomes when held (M12.11): jittered in production. */
    private volatile java.util.function.LongUnaryOperator failureJitter =
            java.util.function.LongUnaryOperator.identity();
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
        this.delegate = Objects.requireNonNull(delegate, "delegate");
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
     * <p>⚠️ A FAILURE IS NEVER HELD AS BYTES AND NEVER SWALLOWED. Nothing is
     * put in the byte map on a throw, so no run inherits an empty array -- and
     * the throw reaches every one of the K runs, which is M5.40a's all-or-none
     * rule arriving where an operator sees it. With the host's clock installed
     * (M10.28, {@link #holdFailures}) the failure itself is held for a
     * backoff and answered as a {@code SegmentFetchHeldException}; without it
     * the next run's read fetches again. Returning
     * an empty array instead would advance K offsets past records nobody read:
     * silent data loss, and the failure mode M5.45g's criterion 5 names one
     * layer down.
     */
    @Override
    public byte[] fetch(Grant grant) throws IOException {
        Objects.requireNonNull(grant, "grant");
        return held(GRANT + grant.url(), () -> delegate.fetch(grant));
    }

    /**
     * A {@code proxy} segment's bytes, fetched from the ingester's segment
     * route at most once per (node, segment KEY) while the hold keeps them
     * (M10.3, ADR-0073).
     *
     * <p>⚠️ THE SAME HOLD AND THE SAME PER-KEY GATE AS {@link #fetch}, which is
     * the point: a {@code proxy} event over HTTP carries coordinates only, and
     * each run of a segment gets its own, so K shard subscriptions reading one
     * segment would otherwise be K route fetches -- the ingester's request
     * rate scaling with shards, which non-negotiable 6 forbids by name. The
     * byte ceiling and the failure rule are {@link #fetch}'s unchanged.
     */
    @Override
    public byte[] fetchSegment(String segmentKey) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        return held(SEGMENT + segmentKey, () -> delegate.fetchSegment(segmentKey));
    }

    /**
     * ⚠️ TWO NAMESPACES IN ONE HOLD. A grant's url and a segment key are
     * different sources that could in principle be spelled alike; prefixing
     * each keeps one from answering for the other, while both share the one
     * byte ceiling an operator sizes the heap against.
     */
    private static final String GRANT = "grant ";
    private static final String SEGMENT = "segment ";

    @FunctionalInterface
    private interface Load {
        byte[] bytes() throws IOException;
    }

    private byte[] held(String key, Load load) throws IOException {
        KeyGate gate = acquire(key);
        try {
            synchronized (gate) {
                byte[] hit;
                Failed failure;
                synchronized (cacheLock) {
                    hit = held.get(key);
                    failure = failed.get(key);
                }
                if (hit != null) {
                    return hit;
                }
                java.util.function.LongSupplier clock = failureClock;
                long now = clock == null ? 0 : clock.getAsLong();
                if (failure != null && clock != null && now < failure.untilMillis()) {
                    // ⚠️ STILL A FAILURE, never an empty array (the rule above),
                    // but a HELD one, which a run's retry does not count as its
                    // attempt (review F1): it waits out the rest of the hold,
                    // and surfaces once the node's own fetches reach its budget.
                    throw new io.github.huyz0.os.biningester.client.SegmentFetchHeldException(
                            "segment " + key + " failed on this node and is held for its "
                                    + "backoff: " + failure.cause().getMessage(),
                            failure.cause(),
                            java.time.Duration.ofMillis(failure.untilMillis() - now),
                            failure.consecutive());
                }
                boolean refetch;
                synchronized (cacheLock) {
                    fetches++;
                    refetch = evicted.remove(key) != null;
                    if (refetch) {
                        refetchesAfterEviction++;
                    }
                }
                if (refetch) {
                    count(io.github.huyz0.os.biningester.client.SubscriptionMetrics.Counter
                            .SEGMENT_HOLD_REFETCHES_AFTER_EVICTION);
                }
                byte[] bytes;
                try {
                    bytes = load.bytes();
                } catch (IOException fetchFailed) {
                    if (clock != null) {
                        // ⚠️ A FAILURE LONG PAST STARTS A NEW RUN OF THEM (review
                        // F3): one that expired more than a ceiling ago is not
                        // this outage's, and continuing its count would start
                        // the next at the 30 s ceiling rather than the floor.
                        boolean continuing = failure != null && now
                                < failure.untilMillis() + FAILURE_HOLD_CEILING.toMillis();
                        int consecutive = continuing ? failure.consecutive() + 1 : 1;
                        synchronized (cacheLock) {
                            failed.put(key, new Failed(fetchFailed, clock.getAsLong()
                                    + failureJitter.applyAsLong(holdMillis(consecutive)),
                                    consecutive));
                        }
                    }
                    throw fetchFailed;
                }
                boolean oversize;
                synchronized (cacheLock) {
                    failed.remove(key);
                    oversize = !admit(key, bytes);
                    if (oversize) {
                        oversizeFetches++;
                    }
                }
                if (oversize) {
                    count(io.github.huyz0.os.biningester.client.SubscriptionMetrics.Counter
                            .SEGMENT_HOLD_OVERSIZE_FETCHES);
                }
                return bytes;
            }
        } finally {
            release(key, gate);
        }
    }

    /** floor x 2^(n-1), at most the ceiling. */
    static long holdMillis(int consecutive) {
        long floor = FAILURE_HOLD_FLOOR.toMillis();
        long ceiling = FAILURE_HOLD_CEILING.toMillis();
        int doublings = Math.min(consecutive - 1, 30);
        return Math.min(ceiling, floor << doublings);
    }

    /**
     * Holds each failed fetch for a backoff, per node and key (M10.28),
     * reading {@code relativeMillis} -- the host's clock, injected because this
     * module may not read one (non-negotiable 7).
     *
     * <p>⚠️ WITHOUT IT A FAILED SEGMENT IS FETCHED ONCE PER RUN: the hold keeps
     * only successes, and each run's client retries on its own backoff, so K
     * runs of one failing segment were K x {@code maxAttempts} requests -- a
     * rate scaling with shards, which non-negotiable 6 forbids by name. With
     * it, the node fetches the key once per hold, the hold doubling from the
     * consumer's retry floor to its ceiling, as one client's retries would.
     *
     * <p>⚠️ BUT JITTERED ONLY LONGER (M13.18, M12.11 review P1): each hold is
     * its backoff plus up to half again ({@link #upJitter}), so at the 30 s
     * ceiling a node holds a failed key for (30, 45] s -- where before M12.11
     * it held exactly 30 s, so recovery once the store is back can take up to
     * 15 s longer than it did. A client's own wait is jittered both ways,
     * [15, 45) s at the ceiling ({@code HttpSubscriptionTransport.jitteredMillis}):
     * the node's raises the shortest and the mean hold, not the longest, and
     * never cuts a client's attempts shorter than its policy assumes.
     */
    void holdFailures(java.util.function.LongSupplier relativeMillis,
            java.util.function.LongUnaryOperator jitter) {
        this.failureJitter = Objects.requireNonNull(jitter, "jitter");
        this.failureClock = Objects.requireNonNull(relativeMillis, "relativeMillis");
    }

    /**
     * Jitter that only LENGTHENS (M12.11, M10.28b P1): a backoff {@code b} held
     * for a uniform draw from {@code (b, 1.5b]}, so a blip that fails one segment
     * on every node does not bring every node back for it in the same
     * millisecond.
     *
     * <p>⚠️ NEVER SHORTER THAN THE BACKOFF (M12.11 review P1): a hold drawn below
     * it spends the consumer's attempts faster than its policy assumes, and
     * {@code FailureHoldWithRetryTest}'s 75 s outage then paused shards for six
     * seeds in eight. Longer holds mean at most as many re-fetches, not more.
     *
     * <p>⚠️ THE GENERATOR IS LOCKED: one node's failures come from many threads,
     * and {@code SplittableRandom} is not safe to share.
     */
    static java.util.function.LongUnaryOperator upJitter(
            java.util.random.RandomGenerator random) {
        Objects.requireNonNull(random, "random");
        return base -> {
            synchronized (random) {
                return base + 1 + random.nextLong(Math.max(1, base / 2));
            }
        };
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

    /** @return whether the bytes were held; {@code false} only when too large to */
    private boolean admit(String url, byte[] bytes) {
        if (bytes == null) {
            return true;
        }
        if (bytes.length > capacityBytes) {
            // ⚠️ TOO LARGE TO HOLD IS STILL SERVED -- the same array the
            // delegate returned, since nothing here copies. Holding it would
            // break the ceiling this class exists to keep; refusing to return
            // it would fail a read for a segment the node fetched
            // successfully. The cost is that this node's other runs of THAT
            // segment fetch again, which is the honest consequence of a hold
            // smaller than a segment -- and it is why the guard is here rather
            // than left to the eviction loop, which would admit the oversize
            // entry and evict EVERY held segment before dropping it again.
            // ⚠️ COUNTED, not remembered as evicted: every one of its fetches
            // is the fall-back, and M10.25 reports each (oversizeFetches).
            return false;
        }
        held.put(url, bytes);
        bytesHeld += bytes.length;
        var it = held.entrySet().iterator();
        while (bytesHeld > capacityBytes && it.hasNext()) {
            // ⚠️ THE ITERATOR IS IN ACCESS ORDER, so this removes the LEAST
            // recently used first -- the entry just admitted is the most
            // recent and is the last thing that can go.
            Map.Entry<String, byte[]> dropped = it.next();
            bytesHeld -= dropped.getValue().length;
            evicted.put(dropped.getKey(), Boolean.TRUE);
            it.remove();
        }
        return true;
    }

    private void count(io.github.huyz0.os.biningester.client.SubscriptionMetrics.Counter c) {
        var sink = metrics;
        if (sink != null) {
            sink.increment(c);
        }
    }

    /**
     * Reports this hold's re-fetch fall-backs into {@code metrics} from now on
     * (M10.25): the node's own metrics, which the plugin exports.
     */
    void countInto(io.github.huyz0.os.biningester.client.SubscriptionMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    /**
     * Fetches of a segment too large for the hold (M10.25): each reached the
     * delegate, and every other run on this node reading it will again.
     */
    public long oversizeFetches() {
        synchronized (cacheLock) {
            return oversizeFetches;
        }
    }

    /**
     * Fetches of a segment this node held and evicted before a later run asked
     * for it (M10.25): a hold smaller than the working set, as a number.
     */
    public long refetchesAfterEviction() {
        synchronized (cacheLock) {
            return refetchesAfterEviction;
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
