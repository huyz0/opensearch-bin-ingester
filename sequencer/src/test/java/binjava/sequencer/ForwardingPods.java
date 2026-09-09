// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/**
 * The follower half of a simulated fleet: pods that FORWARD rather than lead
 * (M5.7).
 *
 * <p>⚠️ SPLIT OUT OF {@link CommitProtocolSimulation} when it crossed 700 lines
 * — code-structure rule 1, split rather than raise, the same way
 * {@code DedupFixtures} and {@code InvariantFixtures} were. The seam is real
 * rather than convenient: leading is about acquiring a lease and writing a
 * chain, and forwarding is about reaching whoever holds them, with no
 * coordination state of its own.
 *
 * <p>⚠️ IT COUNTS ATTEMPTS AND SUCCESSES SEPARATELY, for the reason the
 * simulation's zombie counters state: attempts are the anti-vacuity signal that
 * the mechanism RAN, successes are what it achieved, and a sweep asserting only
 * one of them cannot tell "no pod ever forwarded" from "every forward was
 * refused".
 */
final class ForwardingPods {

    private static final String PREFIX = "bins/cluster-a";
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);

    private final BinStore store;
    private final FaultInjectingStore faulty;
    private final InProcessTransport peers = new InProcessTransport();
    /**
     * What is behind each endpoint, kept alongside {@link InProcessTransport}'s
     * own map.
     *
     * <p>⚠️ SO A LANDED FORWARD CAN NAME THE CHAIN IT LANDED IN. The delta a
     * peer returns carries a sequence and no epoch, and the driver's `leader`
     * field is not the answer -- a forward may be issued while that field is
     * null. The only thing that knows is the sequencer the transport actually
     * reached.
     */
    private final java.util.Map<String, Sequencer> behind = new java.util.HashMap<>();
    private final SequencerTransport hop;

    private int commits;
    private int attempts;
    private int refusals;
    private long epochThatLanded = -1;

    ForwardingPods(BinStore store, FaultInjectingStore faulty) {
        this.store = store;
        this.faulty = faulty;
        // ⚠️ THE HOP CHANGES WHO IS ACTING, which is what makes a partition mean
        // anything to a forwarded commit. A forwarding pod reads the LEASE as
        // itself -- so a partitioned forwarder cannot even learn where to send
        // -- and the leaseholder writes the CHAIN as itself, so a partitioned
        // leader still refuses the append it was handed. Routing the whole call
        // under one pod's name would make a partition block both halves or
        // neither, and that asymmetry is the entire shape of the failure.
        this.hop = new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
                String from = faulty.actingPod();
                faulty.actingAs(podOf(endpoint));
                try {
                    CommitDelta delta = peers.send(endpoint, request);
                    if (behind.get(endpoint) instanceof LocalSequencer local) {
                        epochThatLanded = local.epoch();
                    }
                    return delta;
                } catch (NotTheLeaseholderException refused) {
                    // ⚠️ COUNTED HERE AND NOT AT THE CALLER, because
                    // `RemoteSequencer` does not propagate this type: when the
                    // lease has not moved it WRAPS the refusal in a plain
                    // IOException, so a caller counting by type sees none and
                    // the refusal path reads as unreached. Measured: 0 refusals
                    // counted at the caller across 1,000 seeds that issued
                    // thousands of them.
                    refusals++;
                    throw refused;
                } finally {
                    faulty.actingAs(from);
                }
            }

            @Override
            public void close() throws IOException {
                peers.close();
            }
        };
    }

    /**
     * Where a pod answers its peers.
     *
     * <p>⚠️ DISTINCT PER POD, and that is what makes forwarding falsifiable at
     * all. Every pod used to write {@code ""} into its lease, so a transport
     * routing by endpoint could not tell one holder from another and a
     * forwarder with a STALE endpoint would still have reached the right
     * sequencer — the fake would have been answering the question under test.
     */
    static String endpointOf(String pod) {
        return pod + ":9100";
    }

    private static String podOf(String endpoint) {
        return endpoint.substring(0, endpoint.indexOf(':'));
    }

    /**
     * Puts {@code sequencer} behind {@code pod}'s endpoint.
     *
     * <p>⚠️ UNDER THE ENDPOINT THE LEASE NAMES, never under "whichever sequencer
     * we have". {@link InProcessTransport} refuses an endpoint it does not
     * know, so a forwarder that stopped re-reading the lease reaches a pod that
     * has gone and is REFUSED — the property under test, which a transport
     * routing to the only leader present would answer for free.
     */
    void leads(String pod, Sequencer sequencer) {
        peers.at(endpointOf(pod), sequencer);
        behind.put(endpointOf(pod), sequencer);
    }

    /**
     * Stops {@code pod} answering.
     *
     * <p>⚠️ CALLED EVEN WHEN THE SEQUENCER OBJECT SURVIVES. A zombie is a pod
     * that stopped ANSWERING, so a peer's send to it must fail; keeping it
     * routable would model a leader unreachable to the store and reachable to
     * its peers, which is not a failure anything injects.
     */
    void gone(String pod) {
        peers.gone(endpointOf(pod));
        behind.remove(endpointOf(pod));
    }

    /**
     * Commits {@code request} by forwarding it to whoever the lease names.
     *
     * <p>⚠️ THE SEQUENCER IS BUILT PER COMMIT AND HOLDS NOTHING.
     * {@link RemoteSequencer} re-reads the lease on every call by design
     * (ADR-0012), so a long-lived instance would carry no state a fresh one
     * does not — and a fresh one makes it impossible for the driver to hand a
     * follower a cached endpoint the production class refuses to keep.
     *
     * @return where the records landed, or empty if the forward was refused or
     *     its outcome lost — neither of which is a failure of the run
     */
    Optional<CommitDelta> forward(CommitRequest request) {
        attempts++;
        faulty.actingAs(request.podId());
        RemoteSequencer remote = new RemoteSequencer(store,
                new LeaseConfig(PREFIX, request.podId(), endpointOf(request.podId()), TTL, RENEW),
                hop);
        try {
            CommitDelta delta = remote.commit(request);
            commits++;
            return Optional.of(delta);
        } catch (IOException refusedOrLost) {
            // ⚠️ NOT RE-THROWN, and not counted as a commit. The lease may have
            // moved, the peer may be partitioned, or the store may be down; a
            // follower that cannot reach the leaseholder simply does not commit
            // this round.
            return Optional.empty();
        }
    }

    /**
     * The epoch of the chain the last landed forward went into.
     *
     * <p>⚠️ READ ONLY AFTER {@link #forward} RETURNS A DELTA, and it is the
     * epoch of the sequencer the transport reached rather than of whoever the
     * driver believes is leading.
     */
    long epochThatLanded() {
        return epochThatLanded;
    }

    /** How many forwarded commits landed. */
    int commits() {
        return commits;
    }

    /** How many were attempted, landed or not. */
    int attempts() {
        return attempts;
    }

    /**
     * How many sends were REFUSED because the endpoint the lease named answers
     * nothing.
     *
     * <p>⚠️ THE ANTI-VACUITY SIGNAL FOR THE ENDPOINT MACHINERY. A run with none
     * of these never had the lease and the routing table disagree, so neither
     * per-pod endpoints nor {@link #gone} constrained anything in it.
     *
     * <p>⚠️ THEY REACH THE RE-READ, NEVER THE RESEND, and the difference is
     * stated rather than left to look like coverage. MEASURED over 1,000 seeds:
     * {@code RemoteSequencer}'s refusal catch is entered 22,824 times, the
     * lease re-read succeeds 22,136 times, and the resend to a MOVED holder
     * executes ZERO times. It is unreachable by construction here -- this
     * driver is single-threaded and a round is atomic, so nothing can move the
     * lease between the send and the re-read. The resend arm is covered by
     * {@code RemoteSequencerTest}, which moves the lease by hand.
     */
    int refusals() {
        return refusals;
    }
}
