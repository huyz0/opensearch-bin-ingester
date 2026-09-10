// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Which pod in an AZ fetches a segment (M5.8).
 *
 * <p>⚠️ COMPUTED, NOT DISCOVERED, which is ADR-0012's requirement: every pod
 * given the same AZ membership reaches the same answer with no coordination,
 * no gossip and no round trip. That is why the answer is a VALUE two processes
 * must agree on, and why a golden case pins it -- review MEASURED that a
 * per-JVM seed mixed into the score passes every "these two calls agree" test
 * while making every pod choose differently, which is 4 GETs where criterion 9
 * budgets 2.
 *
 * <p>⚠️ RENDEZVOUS RATHER THAN A HASH RING WITH VIRTUAL NODES, and the choice
 * is about what can go wrong rather than speed. Both give "removing a member
 * moves only what it owned"; rendezvous gives it with no replica count to tune.
 * It runs once per segment per AZ, not per record.
 */
public final class PeerRing {

    private PeerRing() {
    }

    /**
     * The peer that fetches {@code segmentKey} for this AZ, or empty if the AZ
     * has none.
     *
     * <p>⚠️ EMPTY IS NOT AN ERROR. ADR-0012's miss ladder ends at the object
     * store: {@code local cache -> ring owner in the same AZ -> object store},
     * and no ring owner means the last rung, which is correct and costs one
     * extra GET. An earlier version threw {@code IllegalArgumentException},
     * which in this module means programmer error and would have turned a
     * routine single-pod-AZ into a crash.
     */
    public static Optional<Peer> ownerOf(String segmentKey, AzPeers inAz) {
        if (inAz.isEmpty()) {
            return Optional.empty();
        }
        Peer best = null;
        long bestScore = 0;
        for (Peer peer : inAz.peers()) {
            long score = score(segmentKey, peer.podId());
            // ⚠️ TIES BROKEN ON (podId, endpoint), A TOTAL ORDER, and both
            // halves are load-bearing. Ties are REACHABLE: `avalanche` is a
            // bijection over a 32-bit CRC, so a score carries 32 bits and a
            // collision is about n(n-1)/2 over 2^32 -- review found
            // `pod-1371838` and `pod-2000402` both scoring
            // 1977658691747965201 for "seg/2026/09/10/abc".
            // ⚠️ AND podId ALONE IS NOT A TOTAL ORDER, because two entries can
            // share one: `podShortId` is stable across a restart (ADR-0036)
            // under a StatefulSet (ADR-0031), so `ingester-0` at its old
            // address beside `ingester-0` at its new one is what every rolling
            // restart produces for the reconciliation window. On podId alone
            // review MEASURED two pods holding that list in different orders
            // disagreeing on 102 of 200 segments; on (podId, endpoint) it is 0.
            // ⚠️ REFUSING THE DUPLICATE WAS THE FIRST FIX AND WAS WORSE: it
            // threw from `inAz` on every call, taking out the whole AZ's ring
            // including peers that were not duplicated, and turned a transient
            // membership disagreement into a hard failure -- against
            // architecture.md's "membership disagreement costs an extra GET,
            // never correctness". Here the tie is settled and both pods agree,
            // and the cost lands on ADR-0012's ladder: whichever entry wins may
            // be the STALE one, so the pod addresses a dead endpoint, fails and
            // fetches from the object store -- one extra GET, no coordination.
            // ⚠️ AND IT IS NOT AN OCCASIONAL GET. `orderingKey` sorts
            // lexicographically, which is uncorrelated with which address is
            // current, so the stale entry wins ALL of that pod's share or none
            // of it: review MEASURED 1,277 of 1,277, and 2,039 of 2,039 on the
            // test file's own fixture. Bounded and self-healing -- it lasts the
            // reconciliation window -- but it is that pod's whole 1/n share for
            // about half of rolling restarts, not a rounding error.
            // ⚠️ ORDER-INDEPENDENCE ITSELF COMES FROM `score` BEING PURE in
            // (segmentKey, podId); this clause makes the tie deterministic.
            if (best == null
                    || score > bestScore
                    || (score == bestScore && orderingKey(peer).compareTo(orderingKey(best)) < 0)) {
                best = peer;
                bestScore = score;
            }
        }
        return Optional.of(best);
    }

    /** A total order over peers, used only to settle a score tie. */
    private static String orderingKey(Peer peer) {
        return peer.podId() + '\u0000' + peer.endpoint();
    }

    private static long score(String segmentKey, String podId) {
        CRC32C crc = new CRC32C();
        crc.update(segmentKey.getBytes(StandardCharsets.UTF_8));
        crc.update((byte) 0);
        crc.update(podId.getBytes(StandardCharsets.UTF_8));
        return avalanche(crc.getValue());
    }

    /**
     * ⚠️ CRC32C ALONE IS NOT ENOUGH HERE, and this is measured rather than
     * cargo-culted: it is linear, and pod ids that differ only in their last
     * character leave the scores correlated. Over 3,000 segments and 3 members
     * the raw CRC gave 760 / 740 / 1500 -- one pod owning half the AZ's
     * fetches. The finalizer below is the standard 64-bit avalanche and costs
     * three shifts and two multiplies, once per segment per AZ.
     */
    private static long avalanche(long value) {
        long h = value;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }
}
