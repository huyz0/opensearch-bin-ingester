// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import binjava.format.Lease;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * Commits by forwarding to the pod the lease names (M5.4, M5.5, FR-11).
 *
 * <p>⚠️ THIS IS THE HALF OF MULTI-POD CORRECTNESS THAT WAS MISSING, and an
 * earlier draft of this sentence said it WAS multi-pod correctness. It is not,
 * and the distinction cost a review round to notice: M4 introduced a lease so
 * that exactly one sequencer writes the chain and shipped only the local
 * implementation, so a pod that is not the leaseholder could not commit at all.
 * This class removes that. But the only {@link SequencerTransport} in the tree
 * is a test fixture (M5.6e) and no production {@code main()} assembles any of
 * it — M2.1 merely OBSERVES that absence in a row marked done, and no row owns
 * creating one — so a real follower pod still cannot forward. Everything else in M5
 * is the read path; this one is a correctness hole, and it is not shut yet.
 *
 * <p>⚠️ IT HOLDS NO COORDINATION STATE, AND THAT IS THE DESIGN. It reads the
 * lease to learn where to send, sends, and on a refusal re-reads. ADR-0012:
 * "the lease object is the truth; a peer hint may accelerate, never decide."
 * A cached endpoint would be a second source of that truth, and a stale one
 * would send commits to a fenced leader.
 *
 * <p>⚠️ IT ADDS NO OBJECT-STORE WRITE. The forwarded commit is the same single
 * PUT the leaseholder already makes; forwarding costs one GET of the lease per
 * commit and one pod-to-pod RPC. The GET is the price of not caching
 * leadership, and it is bounded by commits, never by records or streams.
 */
public final class RemoteSequencer implements Sequencer {

    private final BinStore store;
    private final LeaseConfig leaseConfig;
    private final SequencerTransport transport;

    public RemoteSequencer(BinStore store, LeaseConfig leaseConfig,
            SequencerTransport transport) {
        this.store = Objects.requireNonNull(store, "store");
        this.leaseConfig = Objects.requireNonNull(leaseConfig, "leaseConfig");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /**
     * The lease as the store currently holds it.
     *
     * <p>⚠️ READ EVERY TIME, NOT CACHED. A lease that moved between two commits
     * must move this pod's target with it, and the only thing that knows is the
     * store.
     */
    private Lease currentLease() throws IOException {
        try (InputStream in = store.get(leaseConfig.leaseKey())) {
            return Lease.decode(in.readAllBytes());
        }
    }

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        Lease lease = currentLease();
        try {
            return transport.send(lease.holderEndpoint(), requests.size() == 1
                    ? requests.get(0) : batched(requests));
        } catch (SequencerTransport.NotTheLeaseholderException refused) {
            // ⚠️ FOLLOW THE LEASE, AND ONLY ON A REFUSAL (M5.5). A refusal says
            // plainly that nothing was applied, so resending the SAME request
            // to the new holder is safe. An IOException does not say that -- it
            // may have landed and lost its reply.
            // ⚠️ M5.23 MAKES THAT RESEND ANSWERABLE ONLY AT THE SEQUENCER
            // INSTANCE THAT MADE THE APPEND. A leaseholder reconciles its own
            // ambiguous append against the slot it named, so a resend of the
            // same triple TO THAT INSTANCE gets the offsets that already apply
            // -- and not even the same pod's NEXT term qualifies, because the
            // mark that does it is a field on the instance. ⚠️ IT IS NOT ANSWERABLE
            // ACROSS A TAKEOVER: the reconciliation seeds that pod's in-memory
            // window and never its checkpoint, so a SUCCESSOR does not inherit
            // the flush once a later checkpoint bounds past its delta, and the
            // resend appends the same records again -- which is I2 and which is
            // M5.25. And a takeover is exactly the case this arm is in.
            // ⚠️ SO PROPAGATING IS STILL A CORRECTNESS BAR, not yet a policy
            // choice. It is also the conservative behaviour on its own terms:
            // nothing bounds how long the peer stays unreachable, and the
            // caller is the one that must decide.
            Lease moved = currentLease();
            if (moved.epoch() == lease.epoch()
                    && moved.holderEndpoint().equals(lease.holderEndpoint())) {
                // ⚠️ THE LEASE DID NOT MOVE, so the peer refusing is the pod the
                // store still names. Resending would loop against a peer that
                // has already said no. Propagating tells the caller the truth:
                // there is no reachable sequencer right now.
                throw new IOException("the pod the lease names ("
                        + lease.holderEndpoint() + ", epoch " + lease.epoch()
                        + ") refused the commit and the lease has not moved", refused);
            }
            return transport.send(moved.holderEndpoint(), requests.size() == 1
                    ? requests.get(0) : batched(requests));
        }
    }

    /**
     * ⚠️ ONE REQUEST PER SEND, DELIBERATELY, UNTIL THE WIRE CARRIES A BATCH.
     * {@code commitAll} exists so a leaseholder can fold many pods' flushes into
     * one delta -- that is M4.7's cost property, and it belongs to the pod that
     * WRITES the chain. A forwarding pod has one flush of its own, so batching
     * here would be inventing a shape the transport has no encoding for. A
     * caller handing several is refused rather than silently sending the first.
     */
    private static CommitRequest batched(List<CommitRequest> requests) throws IOException {
        throw new IOException("forwarding carries one request per send; got "
                + requests.size() + ". Batching belongs to the pod that writes the chain "
                + "(M4.7), not to the pod forwarding to it");
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }
}
