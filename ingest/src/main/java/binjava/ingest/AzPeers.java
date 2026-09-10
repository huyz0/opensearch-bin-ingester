// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.List;

/**
 * The peers of ONE availability zone (M5.8).
 *
 * <p>⚠️ THIS TYPE MAKES A MIXED VIEW UNREPRESENTABLE. A bare
 * {@code List<Peer>} carries no AZ and enforces nothing: review MEASURED that
 * {@code ownerOf("seg/0", List.of(aPeerInAzA, aPeerInAzB))} answered the az-b
 * peer for 13 of 20 segments.
 *
 * <p>⚠️ A DUPLICATE podId IS ACCEPTED, DELIBERATELY. An earlier version
 * refused it, and that was worse: it threw from {@code inAz} on every call,
 * taking out the whole AZ's ring including peers that were not duplicated, and
 * a duplicate is what every rolling restart produces for its reconciliation
 * window. {@link PeerRing} settles it instead, with a total order on
 * {@code (podId, endpoint)}.
 *
 * <p>⚠️ THIS TYPE ALONE DID NOT MAKE A CROSS-AZ FETCH UNADDRESSABLE. While
 * {@code Membership} had an {@code inAz(String)}, a caller in az-a could ask
 * for az-b and get a usable view -- review MEASURED 20 of 20. M5.9 closed that
 * by removing the query, so the seam now yields only {@code localAz()}.
 * ⚠️ WHAT REMAINS is that anyone HOLDING a fleet list can still build an
 * {@code AzPeers} for any AZ by hand; the seam no longer retains the fleet, so
 * that is narrowed rather than closed.
 */
public record AzPeers(String az, List<Peer> peers) {

    public AzPeers {
        if (az == null || az.isBlank()) {
            throw new IllegalArgumentException("an AZ view needs an az");
        }
        peers = List.copyOf(peers);
        for (Peer peer : peers) {
            if (!az.equals(peer.az())) {
                throw new IllegalArgumentException(
                        "peer " + peer.podId() + " is in az " + peer.az()
                                + ", not " + az + " -- peer fetch is intra-AZ only");
            }
        }
    }

    /** Whether this AZ holds no peers, so nothing here can fetch. */
    public boolean isEmpty() {
        return peers.isEmpty();
    }
}
