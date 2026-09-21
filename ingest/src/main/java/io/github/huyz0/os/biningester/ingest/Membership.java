// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

/**
 * Who else is in the fleet (M5.8).
 *
 * <p>⚠️ A SEAM, AND IT DOES NO I/O. Business logic touches no socket
 * (non-negotiable 7): the Kubernetes {@code EndpointSlice} watch that will
 * implement this is M8's, and until it lands membership is configuration --
 * {@link StaticMembership}. SPEC.md states that as a consequence rather than
 * hiding it.
 *
 * <p>⚠️ THERE IS NO QUERY TAKING AN AZ, which is ADR-0012's "enforced in code,
 * not just documented" AT THIS SEAM -- narrowed rather than closed, since
 * {@code PeerRing.ownerOf} is public and whoever holds the fleet list can still
 * build an {@link AzPeers} by hand. M5.8 shipped an {@code inAz(String)} and review
 * MEASURED an az-a caller naming az-b's peers 20 of 20 times: the type stopped
 * a MIXED view and nothing stopped the WRONG one. M5.9 removed the query
 * rather than discouraging it, because no pod needs another AZ's membership --
 * the ring owner in each AZ is computed by pods in that AZ.
 */
public interface Membership {

    // ⚠️ AN IMPLEMENTATION MUST NOT REJECT A DUPLICATE podId, and this is
    // stated HERE rather than only in ADR-0040 because this file is what the
    // next implementer opens. Two entries sharing a podId are a normal
    // transient -- `podShortId` is stable across a restart (ADR-0036) under a
    // StatefulSet (ADR-0031), so a rolling restart shows the old address beside
    // the new one until reconciliation finishes. `PeerRing` settles it with a
    // total order on `(podId, endpoint)`. An earlier version refused it and
    // review MEASURED the cost: `inAz` threw on EVERY call, taking out the
    // whole AZ's ring including peers that were not duplicated, which turns a
    // disagreement ADR-0012 says costs an extra GET into a hard failure.
    // See ADR-0040.

    /** This pod. */
    Peer self();

    /**
     * The peers of THIS pod's AZ, as the fleet lists them.
     *
     * <p>⚠️ IT NEED NOT CONTAIN {@link #self}, AND A CALLER MUST NOT ASSUME IT
     * DOES. After a restart the list still holds this pod's OLD address, and
     * that entry is what every pod in the AZ sees -- including this one. So
     * "do I own this segment" is {@code owner.podId().equals(self().podId())},
     * never {@code owner.equals(self())}, which review MEASURED matching 0 of
     * 200 segments for a restarted pod.
     *
     * <p>⚠️ AND THAT RULE IS UNCONDITIONAL WHILE A DUPLICATE podId IS NORMAL,
     * so if both incarnations are briefly live BOTH answer yes and both
     * prefetch. It is bounded by the overlap window and unlikely under an
     * ordered StatefulSet restart -- but it is not a few segments:
     * {@code PeerRing} MEASURED the stale entry winning a pod's WHOLE share.
     * Which incarnation should stand down is undecided.
     *
     * <p>⚠️ THE FLEET WINS BECAUSE AGREEMENT IS THE POINT. Substituting self
     * in would make this pod's view differ from its neighbours' -- MEASURED at
     * 96 of 200 segments -- and two pods that disagree about the owner both
     * fetch, which is the arithmetic the ring exists to protect.
     *
     * <p>⚠️ THERE IS NO WAY TO ASK FOR ANOTHER AZ, and that absence IS the
     * enforcement ADR-0012 demands -- "the peer-fetch path takes an AZ-scoped
     * member list, so a cross-AZ peer is not addressable by construction rather
     * than by discipline". M5.8 shipped an {@code inAz(String)} and review
     * MEASURED an az-a caller naming az-b's peers 20 of 20 times: the type
     * stopped a MIXED view, nothing stopped the WRONG one.
     *
     * <p>⚠️ NO POD EVER NEEDS ANOTHER AZ'S MEMBERSHIP. SPEC.md's prefetch
     * paragraph says the ring owner in EACH AZ fetches for that AZ, so the
     * owner elsewhere is computed by pods elsewhere. The query was removed
     * rather than discouraged because it had no legitimate caller to keep it
     * for.
     */
    AzPeers localAz();
}
