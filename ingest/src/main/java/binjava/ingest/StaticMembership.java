// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Membership as configuration, until the {@code EndpointSlice} watch lands
 * (M5.8).
 *
 * <p>⚠️ A STATIC LIST NEVER REMOVES A MEMBER, which is why NFR-9 moved to M8:
 * the early-challenge path fires on a membership event this implementation
 * cannot produce.
 */
public final class StaticMembership implements Membership {

    private final Map<String, List<Peer>> byAz = new LinkedHashMap<>();

    public StaticMembership(List<Peer> peers) {
        for (Peer peer : peers) {
            byAz.computeIfAbsent(peer.az(), az -> new ArrayList<>()).add(peer);
        }
        byAz.replaceAll((az, members) -> List.copyOf(members));
    }

    @Override
    public AzPeers inAz(String az) {
        return new AzPeers(az, byAz.getOrDefault(az, List.of()));
    }
}
