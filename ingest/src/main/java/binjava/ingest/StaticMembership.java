// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.ArrayList;
import java.util.List;

/**
 * Membership as configuration, until the {@code EndpointSlice} watch lands
 * (M5.8).
 *
 * <p>⚠️ A STATIC LIST NEVER REMOVES A MEMBER, which is why NFR-9 moved to M8:
 * the early-challenge path fires on a membership event this implementation
 * cannot produce.
 *
 * <p>⚠️ THE FLEET WINS, AND NOTHING IS REFUSED. The local view is exactly the
 * configured peers of this pod's AZ -- self is not substituted in, and a
 * disagreement between {@code self} and the list is not an error. Every pod
 * therefore computes the IDENTICAL view and so the identical owner, which is
 * the property the ring exists for: review MEASURED that letting self replace
 * its own fleet entry made two pods disagree on 96 of 200 segments, against 0
 * before it.
 *
 * <p>⚠️ EARLIER VERSIONS REFUSED, TWICE, AND BOTH WERE WRONG. Refusing a
 * {@code self} absent from the fleet crash-loops a scale-up replica the list
 * has not caught up with; refusing an AZ disagreement kills a correctly-running
 * pod on the say-so of the STALE side, since membership here IS configuration
 * (this class's own title) while {@code self} comes from the platform. Both
 * contradict ADR-0012 and ADR-0040: a membership disagreement costs an extra
 * GET, never correctness. A pod the list has not caught up with simply does not
 * prefetch until it does. ⚠️ ITS CONSUMERS DO NOT FALL TO THE OBJECT STORE for
 * that reason alone: the AZ still has an owner, so they proxy through it --
 * rung 2, not rung 3. The last rung is reached only when the local view is
 * EMPTY, which is a pod in an AZ the list does not mention at all.
 */
public final class StaticMembership implements Membership {

    private final Peer self;
    private final AzPeers localAz;

    public StaticMembership(Peer self, List<Peer> fleet) {
        this.self = self;
        List<Peer> here = new ArrayList<>();
        for (Peer peer : fleet) {
            if (self.az().equals(peer.az())) {
                here.add(peer);
            }
        }
        this.localAz = new AzPeers(self.az(), here);
    }

    @Override
    public Peer self() {
        return self;
    }

    @Override
    public AzPeers localAz() {
        return localAz;
    }
}
