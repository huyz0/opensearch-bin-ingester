// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * Who else is in the fleet (M5.8).
 *
 * <p>⚠️ A SEAM, AND IT DOES NO I/O. Business logic touches no socket
 * (non-negotiable 7): the Kubernetes {@code EndpointSlice} watch that will
 * implement this is M8's, and until it lands membership is configuration --
 * {@link StaticMembership}. SPEC.md states that as a consequence rather than
 * hiding it.
 *
 * <p>⚠️ THE ONLY QUERY IS AZ-SCOPED, deliberately: a seam that handed back the
 * whole fleet would leave every caller free to mix AZs. ⚠️ THAT IS NOT YET
 * ADR-0012's "enforced in code, not just documented" -- this layer has no
 * notion of the LOCAL pod's AZ, so asking for another AZ is still possible.
 * M5.9 closes that at the fetch path.
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

    /**
     * The peers in {@code az}, empty if none.
     *
     * <p>⚠️ RETURNS {@link AzPeers}, NOT A BARE LIST, so what comes back cannot
     * be widened into a cross-AZ fetch by a caller who forgets. An UNKNOWN az
     * answers empty, which is normal rather than exceptional: ADR-0012's miss
     * ladder says the response to no ring owner is to fetch from the object
     * store -- correct, one extra GET.
     *
     * <p>⚠️ A BLANK OR NULL az THROWS, and that is a different case from an
     * unknown one: it is a configuration error, not a miss, and degrading it to
     * a GET would hide an unset AZ variable behind a permanent cost. Stated
     * because the two look alike at the call site and only one is a ladder rung.
     */
    AzPeers inAz(String az);
}
