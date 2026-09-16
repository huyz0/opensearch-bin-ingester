// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Warms one AZ's copy of a segment when it becomes DURABLE (M5.16, NFR-4).
 *
 * <p>⚠️ ON DURABILITY, NOT ON COMMIT, and the difference is the point.
 * Prefetching is cache warming with no visibility implication, so it may start
 * the moment the bytes exist -- 20-350 ms before the commit that makes them
 * visible. The PUSH still waits for the commit, which is invariant I4: a
 * consumer is told about a segment only once its offsets are durable.
 *
 * <p>⚠️ ONE FETCH PER SEGMENT PER AZ, INDEPENDENT OF NODE COUNT. The ring owner
 * in each AZ fetches; every other pod in that AZ is served from it through
 * ADR-0012's miss ladder. Three AZs is two GETs per segment whether the AZ
 * holds three pods or thirty, which is what NFR-4 means by scaling with AZs and
 * not with nodes. Fetch-on-demand per consumer is the rejected alternative: at
 * ~400 streams a node it makes the read rate scale with consumers.
 *
 * <p>⚠️ THE WRITING AZ FETCHES NOTHING, because the writer still holds the
 * bytes. That is an AZ-level rule rather than a pod-level one, and it is
 * deliberately the cheaper side of the trade: a non-writing pod in the writing
 * AZ reaches the writer through the same ladder, so warming a second copy there
 * would buy a GET to avoid an intra-AZ hop.
 *
 * <p>⚠️ IT HOLDS NO CLOCK AND NO SOCKET, and the store reaches it only through
 * {@link SegmentProxy} -- non-negotiable 7.
 *
 * <p>⚠️ NOTHING CONSTRUCTS IT YET, and an earlier draft of this paragraph said
 * the writer's own flush did. It does not, and could not usefully: the pod that
 * wrote a segment is in the WRITING AZ, so every branch below declines for it.
 * What this class needs is a broadcast -- every pod told that a segment is
 * durable, with the ring deciding which one fetches -- and that is the
 * pod-to-pod transport M8 owns (M5.6e). So NO branch here is reachable in
 * production today, including the writing-AZ suppression, which exists for the
 * pods in that AZ that did not write and will hear the same broadcast.
 */
public final class SegmentPrefetcher {

    /** ⚠️ Takes the bytes and drops them: the point is the CACHE, not the sink. */
    private static final SegmentSink DISCARDING = (buffer, offset, length) -> { };

    /**
     * How many recently-prefetched keys are remembered, so a repeated
     * durability signal costs no repeated GET.
     *
     * <p>⚠️ IT IS NOT THE CACHE, and that is the whole reason it exists. An
     * earlier version relied on {@link SegmentCache} to make a repeat free --
     * true for a segment the cache ADMITS, and false for one it cannot hold.
     * {@code SegmentCache.forSegmentsOf}'s own javadoc says an oversized
     * segment is normal, because {@code maxSegmentBytes} is a flush TRIGGER
     * rather than a ceiling: one large {@code _bulk} body makes one oversized
     * segment. MEASURED at capacity 4096 with an 8192-byte segment: five
     * signals, five GETs, nothing held. The read rate then follows the SIGNAL
     * rate -- retried broadcasts -- which is neither segments nor AZs nor
     * nodes.
     */
    static final int KEYS_REMEMBERED = 1024;

    private final Membership membership;
    private final SegmentProxy proxy;

    /**
     * ⚠️ BOUNDED AND ACCESS-ORDERED, because an unbounded set of every segment
     * key this pod ever heard about is a leak that grows with uptime. What
     * falling out of it costs is one repeated GET for a segment nobody has
     * mentioned in a thousand others, which is the cheap direction.
     *
     * <p>⚠️ A REMEMBERED KEY DOES NOT MEAN A HELD COPY. This map outlives the
     * cache entry, so a segment the cache has since evicted is not warmed
     * again by a repeated signal. That is deliberate: the read path falls back
     * through ADR-0012's ladder, and re-warming on every signal is the read
     * rate this class exists to flatten.
     */
    private final java.util.Map<String, Boolean> prefetched =
            java.util.Collections.synchronizedMap(
                    new java.util.LinkedHashMap<>(16, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                java.util.Map.Entry<String, Boolean> eldest) {
                            return size() > KEYS_REMEMBERED;
                        }
                    });

    public SegmentPrefetcher(Membership membership, SegmentProxy proxy) {
        this.membership = Objects.requireNonNull(membership, "membership");
        this.proxy = Objects.requireNonNull(proxy, "proxy");
    }

    /**
     * Fetches {@code segmentKey} into this pod's cache if this pod is the one
     * that should.
     *
     * <p>⚠️ FIVE REASONS NOT TO FETCH, and each is a normal state rather than
     * an error: this pod's AZ wrote the segment, this AZ has no ring at all
     * (ADR-0012's ladder then ends at the object store, which costs one extra
     * GET and is correct), this pod is not the owner, the proxy holds no cache
     * so a fetch would read bytes nobody can serve from, or this pod has
     * already prefetched this segment.
     *
     * <p>⚠️ A SEGMENT THIS POD CANNOT CACHE IS STILL FETCHED ONCE, and that is
     * a deliberate cost rather than an oversight: the read warms nothing, so
     * the AZ is served from the object store afterwards. What it must not do is
     * happen AGAIN per signal, which is what {@link #KEYS_REMEMBERED} is for.
     * Declining to read it at all would need the size, and asking for the size
     * is a {@code stat} per segment -- the request this whole path exists to
     * avoid.
     *
     * <p>⚠️ M5.64 IS REACHABLE FROM HERE WITH NO CONSUMER PRESENT: a read that
     * ends early WITHOUT throwing is admitted to the cache as if whole, and
     * this path admits on the very pod its AZ is about to be served from. That
     * row owns it; this one makes it reachable sooner.
     *
     * <p>⚠️ OWNERSHIP IS BY {@code podId}, NEVER BY {@code Peer} EQUALITY.
     * After a restart the fleet still lists this pod's OLD address and that
     * entry is what every pod in the AZ sees, including this one --
     * {@link Membership#localAz()} says so, and review MEASURED the equality form
     * matching 0 of 200 segments for a restarted pod.
     *
     * @return whether this call fetched
     */
    public boolean onDurable(String segmentKey, String writerAz) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(writerAz, "writerAz");
        if (membership.self().az().equals(writerAz)) {
            return false;
        }
        if (proxy.cache().capacityBytes() == 0) {
            return false;
        }
        Optional<Peer> owner = PeerRing.ownerOf(segmentKey, membership.localAz());
        if (owner.isEmpty() || !owner.get().podId().equals(membership.self().podId())) {
            return false;
        }
        // ⚠️ A SECOND CALL COSTS NO GET, AND THE CACHE IS NOT WHAT BUYS THAT.
        // A cache hit makes a repeat free only for a segment the cache can
        // ADMIT; for one it cannot, every repeated signal was a fresh read.
        // Remembering the key instead makes the property unconditional.
        //
        // ⚠️ RECORDED BEFORE THE READ, so a signal arriving while this one is
        // still reading does not start a second. The cost of recording a fetch
        // that then FAILS is that this pod does not retry it -- correct here,
        // because a prefetch is an optimisation and the read path falls back to
        // the object store, which is ADR-0012's last rung.
        if (prefetched.putIfAbsent(segmentKey, Boolean.TRUE) != null) {
            return false;
        }
        proxy.streamTo(segmentKey, List.of(DISCARDING));
        return true;
    }
}
