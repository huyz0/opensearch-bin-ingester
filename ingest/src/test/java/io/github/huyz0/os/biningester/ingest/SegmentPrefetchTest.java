// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Prefetch on durability costs one GET per AZ, independent of node count
 * (M5.16, NFR-4).
 *
 * <p>⚠️ THE NUMBER IS THE PROPERTY. SPEC.md prices three AZs at two GETs per
 * segment -- the writing AZ fetches nothing because the writer holds the bytes
 * -- and says so "independent of node count". The rejected alternative is
 * fetch-on-demand per consumer, which at ~400 streams a node makes the read
 * rate scale with consumers, exactly what NFR-4 forbids. So every case here
 * counts requests rather than observing that a fetch happened.
 */
class SegmentPrefetchTest {

    private static final String SEGMENT = "bins/seg-durable";

    /** One pod's view: its own identity plus a prefetcher over a shared store. */
    private record Pod(Peer self, SegmentPrefetcher prefetcher) {
    }

    private static List<Pod> fleet(CountingBinStore store, List<String> azs, int podsPerAz) {
        List<Peer> all = new ArrayList<>();
        for (String az : azs) {
            for (int i = 0; i < podsPerAz; i++) {
                all.add(new Peer(az + "-pod" + i, "http://" + az + "-" + i + ":9000", az));
            }
        }
        List<Pod> pods = new ArrayList<>();
        for (Peer self : all) {
            // ⚠️ A CACHE PER POD, because that is what a prefetch warms. One
            // shared cache would make the second AZ's fetch a hit and the case
            // would count one GET while claiming to have measured two.
            SegmentProxy proxy = new SegmentProxy(store, 64 * 1024,
                    SegmentCache.forSegmentsOf(1L << 20), new IndexCostLedger());
            pods.add(new Pod(self,
                    new SegmentPrefetcher(new StaticMembership(self, all), proxy)));
        }
        return pods;
    }

    @Test
    void threeAZsCostTWOGetsWhateverTheNodeCount() throws Exception {
        for (int podsPerAz : new int[] {1, 3, 30}) {
            CountingBinStore store = new CountingBinStore(new MemoryBinStore());
            store.put(SEGMENT, Body.ofBytes(new byte[8192]));
            List<Pod> pods = fleet(store, List.of("az-a", "az-b", "az-c"), podsPerAz);
            long before = store.counts().gets();

            // ⚠️ EVERY POD IS TOLD, which is the shape a broadcast signal has.
            // The ring is what turns "everyone knows" into "one fetches".
            for (Pod pod : pods) {
                pod.prefetcher().onDurable(SEGMENT, "az-a");
            }

            assertThat(store.counts().gets() - before)
                    .as("two AZs did not write it and each warms ONE copy, at %d pods per AZ "
                            + "-- a rate that moved with the node count would be the "
                            + "fetch-on-demand design NFR-4 rules out", podsPerAz)
                    .isEqualTo(2);
        }
    }

    @Test
    void theWRITINGAZFetchesNOTHING() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        List<Pod> pods = fleet(store, List.of("az-a"), 5);
        long before = store.counts().gets();

        for (Pod pod : pods) {
            pod.prefetcher().onDurable(SEGMENT, "az-a");
        }

        assertThat(store.counts().gets() - before)
                .as("the writer still holds the bytes, and every other pod in its AZ reaches "
                        + "it through ADR-0012's ladder -- warming a second copy here buys a "
                        + "GET to avoid an intra-AZ hop")
                .isZero();
    }

    @Test
    void exactlyTheRINGOWNERFetchesAndTheOthersDoNot() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        List<Pod> pods = fleet(store, List.of("az-a", "az-b"), 4);

        List<String> fetched = new ArrayList<>();
        for (Pod pod : pods) {
            if (pod.prefetcher().onDurable(SEGMENT, "az-a")) {
                fetched.add(pod.self().podId());
            }
        }

        List<Peer> inB = new ArrayList<>();
        for (Pod pod : pods) {
            if ("az-b".equals(pod.self().az())) {
                inB.add(pod.self());
            }
        }
        String owner = PeerRing.ownerOf(SEGMENT, new AzPeers("az-b", inB)).orElseThrow().podId();

        assertThat(fetched)
                .as("the ONE that fetched is the ring's owner for this segment in the "
                        + "non-writing AZ -- any other pod fetching is a second copy of the "
                        + "same bytes, and no pod fetching is an AZ served from the store")
                .containsExactly(owner);
    }

    /**
     * A RESTARTED pod still owns its share (M5.16).
     *
     * <p>⚠️ THE FLEET LISTS THE OLD ADDRESS, which is the normal transient
     * {@code Membership.localAz()}'s javadoc describes: {@code podShortId} is
     * stable across a restart under a StatefulSet, so every pod in the AZ --
     * including this one -- sees the pre-restart entry until reconciliation
     * finishes. Ownership therefore compares {@code podId}, never {@code Peer}
     * equality, and review MEASURED the equality form matching 0 of 200
     * segments for a restarted pod.
     *
     * <p>⚠️ THE OTHER CASES CANNOT SEE THIS, because their fleet lists each
     * pod's current address, so the two forms agree everywhere. Measured: with
     * ownership compared by {@code Peer} equality, every case in this file
     * stayed green except this one.
     */
    @Test
    void aRESTARTEDPodStillOwnsItsShare() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));

        List<Peer> asTheFleetSeesThem = List.of(
                new Peer("az-b-pod0", "http://b-0-OLD:9000", "az-b"),
                new Peer("az-b-pod1", "http://b-1:9000", "az-b"));
        String ownerId = PeerRing.ownerOf(SEGMENT, new AzPeers("az-b", asTheFleetSeesThem))
                .orElseThrow().podId();
        // ⚠️ THE OWNER IS THE ONE UNDER TEST, whichever the ring picked: the
        // case is about the restarted pod recognising ITSELF, so it must be the
        // pod the ring named, and its live address must differ from the listed
        // one.
        Peer restarted = new Peer(ownerId, "http://" + ownerId + "-NEW:9000", "az-b");
        SegmentProxy proxy = new SegmentProxy(store, 64 * 1024,
                SegmentCache.forSegmentsOf(1L << 20), new IndexCostLedger());
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(restarted, asTheFleetSeesThem), proxy);

        assertThat(asTheFleetSeesThem)
                .as("PREMISE: the fleet does NOT list this pod's live address, or the two "
                        + "ownership forms agree and this case measures nothing")
                .doesNotContain(restarted);
        assertThat(prefetcher.onDurable(SEGMENT, "az-a"))
                .as("it owns its share by ID -- comparing the Peer would make a restarted "
                        + "pod own nothing, and its whole share would be served from the "
                        + "object store one consumer at a time")
                .isTrue();
    }

    @Test
    void aREPEATEDDurabilitySignalCostsNOFurtherGet() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        List<Pod> pods = fleet(store, List.of("az-a", "az-b"), 3);
        long before = store.counts().gets();

        for (int round = 0; round < 5; round++) {
            for (Pod pod : pods) {
                pod.prefetcher().onDurable(SEGMENT, "az-a");
            }
        }

        assertThat(store.counts().gets() - before)
                .as("a durability signal can arrive more than once -- a retried broadcast, a "
                        + "pod that re-reads the log -- and the pod REMEMBERING what it "
                        + "fetched is what makes that harmless, not the cache. Five rounds costing five GETs would put the read rate on "
                        + "the SIGNAL rate rather than on the segment count")
                .isEqualTo(1);
    }

    /**
     * A repeated signal costs no repeated GET even for a segment the cache
     * CANNOT hold (M5.16, round 1's major).
     *
     * <p>⚠️ THE CACHE WAS DOING THE WORK AND ONLY SOMETIMES. Relying on a
     * cache hit makes a repeat free for a segment the cache ADMITS and leaves
     * it a fresh read for one it cannot -- which `SegmentCache.forSegmentsOf`'s
     * own javadoc calls normal, because `maxSegmentBytes` is a flush TRIGGER
     * and one large `_bulk` body makes one oversized segment. MEASURED before
     * the fix: five signals, five GETs, nothing held. The read rate then
     * follows the SIGNAL rate, which is neither segments nor AZs nor nodes.
     */
    @Test
    void aSegmentTOOLARGEToCacheIsStillFetchedONLYONCE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        Peer self = new Peer("az-b-pod0", "http://b:9000", "az-b");
        SegmentProxy tiny = new SegmentProxy(store, 1024, new SegmentCache(4096), new IndexCostLedger());
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(self)), tiny);
        long before = store.counts().gets();

        for (int round = 0; round < 5; round++) {
            prefetcher.onDurable(SEGMENT, "az-a");
        }

        assertThat(tiny.cache().bytesHeld())
                .as("PREMISE: the segment really is too large for this cache, or the cache "
                        + "is what makes the repeat free and this case measures nothing")
                .isZero();
        assertThat(store.counts().gets() - before)
                .as("ONE read, however many times the signal arrives -- the pod remembers "
                        + "what it fetched rather than asking a cache that could not keep it")
                .isEqualTo(1);
    }

    @Test
    void anAZWithNORingFetchesNothingAndDoesNotThrow() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        Peer self = new Peer("lonely", "http://lonely:9000", "az-b");
        SegmentProxy proxy = new SegmentProxy(store, 64 * 1024,
                SegmentCache.forSegmentsOf(1L << 20), new IndexCostLedger());
        // ⚠️ A FLEET THIS POD IS NOT IN, which is the post-restart shape: the
        // list still holds the OLD address, so `localAz` is non-empty and the
        // owner it names is not this pod by `Peer` equality -- but ownership is
        // decided by podId, and this pod's id is not there either.
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(new Peer("az-a-pod0", "http://a:9000",
                        "az-a"))), proxy);
        long before = store.counts().gets();

        assertThat(prefetcher.onDurable(SEGMENT, "az-a"))
                .as("no ring in this AZ means ADR-0012's ladder ends at the object store, "
                        + "which costs one extra GET at read time and is correct -- it is not "
                        + "an error and must not throw")
                .isFalse();
        assertThat(store.counts().gets() - before).isZero();
    }

    @Test
    void aPROXYWithNOCacheFetchesNOTHING() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        Peer self = new Peer("az-b-pod0", "http://b:9000", "az-b");
        SegmentProxy noCache = new SegmentProxy(store, 64 * 1024, new SegmentCache(0), new IndexCostLedger());
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(self)), noCache);
        long before = store.counts().gets();

        assertThat(prefetcher.onDurable(SEGMENT, "az-a"))
                .as("warming a cache that cannot hold anything reads bytes nobody can be "
                        + "served from -- a GET bought for nothing, which is the one thing a "
                        + "prefetch must never be")
                .isFalse();
        assertThat(store.counts().gets() - before).isZero();
    }
    /**
     * The remembered keys are BOUNDED (M5.16, round 2's major).
     *
     * <p>⚠️ THE BOUND IS THE HALF NOTHING PINNED. Round 1 replaced a cache hit
     * with a remembered key, and remembering EVERY key a pod ever heard about
     * is a set that grows with uptime rather than with anything the fleet
     * holds. Measured: with {@code removeEldestEntry} returning {@code false},
     * every other case in this file stayed green, because each uses one key.
     *
     * <p>⚠️ WHAT FALLING OUT COSTS IS ONE GET, which is why the eviction is
     * ACCESS-ordered rather than insertion-ordered: the key that pays is the
     * one nobody has mentioned in {@code KEYS_REMEMBERED} others, never the
     * one that arrived first and is still being signalled.
     */
    @Test
    void theREMEMBEREDKeysAreBOUNDEDAndTheEVICTEDOneIsTheLEASTRecentlySignalled()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Peer self = new Peer("az-b-pod0", "http://b:9000", "az-b");
        SegmentProxy proxy = new SegmentProxy(store, 1024, new SegmentCache(4096), new IndexCostLedger());
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(self)), proxy);
        // ⚠️ ONE POD IN THE AZ, so the ring names it for every key and the case
        // measures the memory rather than the ownership.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i <= SegmentPrefetcher.KEYS_REMEMBERED; i++) {
            String key = "bins/seg-" + i;
            keys.add(key);
            store.put(key, Body.ofBytes(new byte[64]));
        }
        // ⚠️ OVERSIZED FOR THIS CACHE at 64 bytes against a 4096-byte capacity
        // only in aggregate -- so the cache, not the memory, could answer a
        // repeat for a recent key. The evicted key is the one this asserts on.
        for (String key : keys) {
            prefetcher.onDurable(key, "az-a");
        }
        long afterFirstPass = store.counts().gets();

        assertThat(prefetcher.onDurable(keys.get(0), "az-a"))
                .as("the OLDEST key fell out: remembering every key a pod ever heard about "
                        + "is a set bounded by uptime rather than by the fleet, and one "
                        + "repeated GET for a segment nobody has mentioned in %d others is "
                        + "the cheap direction", SegmentPrefetcher.KEYS_REMEMBERED)
                .isTrue();
        assertThat(prefetcher.onDurable(keys.get(keys.size() - 1), "az-a"))
                .as("the MOST RECENT key is still remembered -- a bound that dropped the "
                        + "keys being signalled right now would put the read rate back on "
                        + "the signal rate")
                .isFalse();
        assertThat(store.counts().gets() - afterFirstPass)
                .as("exactly the evicted key was re-read")
                .isEqualTo(1);
    }

}
