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
 * <p>⚠️ IT DOES NOT MAKE A CROSS-AZ FETCH UNADDRESSABLE, and the difference
 * matters because ADR-0012 asks for the second. Nothing here knows the local
 * pod's AZ, so a caller in az-a can still ask for {@code inAz("az-b")} and get
 * a usable view -- review MEASURED 20 of 20. Closing that is M5.9's, at the
 * fetch path, and it needs a notion of "me" that this layer does not have.
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
