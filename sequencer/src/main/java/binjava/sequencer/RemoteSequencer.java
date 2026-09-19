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

    /**
     * ⚠️ VOLATILE, and written on the commit path while the push path reads it.
     * A stale read costs a consumer one push labelled with the previous epoch,
     * which is the same answer it would have got a millisecond earlier; a torn
     * one is impossible for a {@code long} only because this is volatile.
     */
    private volatile long lastEpoch = EPOCH_UNKNOWN;
    private final SequencerTransport transport;

    public RemoteSequencer(BinStore store, LeaseConfig leaseConfig,
            SequencerTransport transport) {
        this(store, leaseConfig, transport, LeaseChallenge.NEVER);
    }

    /**
     * The same, cutting a forward short when {@code challenge} reports the
     * holder it was sent to gone (M8.57).
     *
     * <p>⚠️ **M8.55 MEASURED WHY**: a frozen holder accepts the connection and
     * says nothing, so the forward waited out the peer-commit timeout -- which
     * IS the TTL -- and the {@code EndpointSlice} evidence was read only by the
     * election after it. A GC-paused leader cost a TTL even with the watch fed.
     */
    public RemoteSequencer(BinStore store, LeaseConfig leaseConfig,
            SequencerTransport transport, LeaseChallenge challenge) {
        this.store = Objects.requireNonNull(store, "store");
        this.leaseConfig = Objects.requireNonNull(leaseConfig, "leaseConfig");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.challenge = Objects.requireNonNull(challenge, "challenge");
    }

    /**
     * How often a waiting forward re-reads the evidence.
     *
     * <p>⚠️ **NO REQUEST**: the evidence is the watch's in-memory view, asked
     * about the lease already in hand, so this adds nothing to any rate.
     */
    static final long EVIDENCE_POLL_MILLIS = 100;

    private final LeaseChallenge challenge;

    /**
     * Sends, and gives up early if the watch reports {@code lease}'s holder gone.
     *
     * <p>⚠️ **THE CUT IS THE FAILURE THE TIMEOUT ALREADY GIVES**: a plain
     * {@code IOException}, which {@code Sequencer.commit} defines as "may have
     * landed" and which is reconciled exactly as a timeout is (M5.23, M5.25).
     * It is never a {@code NotTheLeaseholderException}: nothing says the holder
     * refused, and a caller that re-sent on that belief could double-commit.
     *
     * <p>⚠️ **THE SENDER IS INTERRUPTED, NOT AWAITED**: a transport that ignores
     * the interrupt keeps one virtual thread until its own timeout, which is the
     * bound that applied before this method existed.
     */
    private CommitDelta sendWatching(Lease lease, CommitRequest request) throws IOException {
        if (challenge == LeaseChallenge.NEVER) {
            return transport.send(lease.holderEndpoint(), request);
        }
        java.util.concurrent.CompletableFuture<CommitDelta> answer =
                new java.util.concurrent.CompletableFuture<>();
        Thread sender = Thread.ofVirtual().name("forward-" + lease.holderPodId()).start(() -> {
            try {
                answer.complete(transport.send(lease.holderEndpoint(), request));
            } catch (Throwable failed) {
                answer.completeExceptionally(failed);
            }
        });
        while (true) {
            try {
                return answer.get(EVIDENCE_POLL_MILLIS,
                        java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException stillWaiting) {
                if (challenge.holderGone(lease)) {
                    sender.interrupt();
                    throw new IOException("the watch reports the lease holder "
                            + lease.holderPodId() + " (epoch " + lease.epoch() + ") gone while"
                            + " a commit forwarded to it was in flight; the commit may have"
                            + " landed, and the next attempt elects on that evidence");
                }
            } catch (java.util.concurrent.ExecutionException failed) {
                Throwable cause = failed.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new IOException(cause);
            } catch (InterruptedException interrupted) {
                sender.interrupt();
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while forwarding to "
                        + lease.holderPodId() + "; the commit may have landed", interrupted);
            }
        }
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
            Lease lease = Lease.decode(in.readAllBytes());
            // ⚠️ REMEMBERED SO `epoch()` COSTS NO SECOND READ (M5.15d). The
            // push site asks for the chain epoch once per flush, and answering
            // it with its own GET doubled this follower's lease read rate --
            // the same object, decoded twice in one flush, which cost.md R4
            // calls a read to coalesce rather than to repeat. Every commit
            // passes through here, so what `epoch()` returns is what THIS
            // pod's last commit was routed by.
            lastEpoch = lease.epoch();
            return lease;
        }
    }

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        Lease lease = currentLease();
        refuseSelfAddress(lease, null);
        try {
            return sendWatching(lease, requests.size() == 1
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
            // ACROSS A POD RESTART: the mark that answers it is one
            // sequencer instance's field, and M5.25 carries the flush into the
            // checkpoint for a pod's CURRENT incarnation only -- a checkpoint
            // remembers one per pod. A resend from an incarnation since
            // superseded still appends the records again, which is I2.
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
            refuseSelfAddress(moved, refused);
            return sendWatching(moved, requests.size() == 1
                    ? requests.get(0) : batched(requests));
        }
    }

    /**
     * Refuses to address this pod's own endpoint (M5.6c, ADR-0027).
     *
     * <p>⚠️ REACHABLE THROUGH THE SELF-FENCE, which ADR-0027 accepts as a cost
     * rather than a defect: an ambiguous renew makes a healthy leader treat
     * itself as fenced and stand down for the remainder of the TTL, while the
     * LEASE OBJECT goes on naming it for that whole remainder. A pod in that
     * state falls back to forwarding, reads the lease, and finds itself.
     *
     * <p>⚠️ AND SENDING THERE IS NOT MERELY POINTLESS. The transport routes by
     * endpoint, so the request arrives back at this pod — at the fenced
     * sequencer that just refused it, or, where a pod publishes its
     * {@link FleetSequencer} rather than its local one, at the very object that
     * is forwarding. What a caller needs instead is the truth: no reachable
     * sequencer right now, and the lease will not move until it expires.
     *
     * <p>⚠️ COMPARED ON {@code podId}, NEVER ON THE ENDPOINT, and an earlier
     * draft got that wrong in a way that opened the hole it was closing.
     * {@code holderEndpoint} is EMPTY for every pod configured without one —
     * {@code Lease} says so in as many words, and most of this tree's fixtures
     * still do it — so an endpoint comparison had to carve out the empty case,
     * and the carve-out skipped exactly those pods. {@code podId} needs no
     * carve-out: {@link LeaseConfig} and {@link Lease} both refuse it blank, so
     * the comparison can never be vacuous. It is also the identity
     * {@code LeaseManager.isOwnTerm} already uses.
     *
     * <p>⚠️ IT SAYS WHAT IS TRUE, NOT WHAT IS LIKELY. The self-fence is the
     * case this exists for, but it is not the only way to arrive: during a
     * PROMOTION a pod acquires the lease before it has finished sealing and
     * recovering, so a concurrent commit on that same pod loses the election
     * lock, forwards, and finds itself — with a perfectly healthy sequencer
     * moments away. Refusing is right either way; asserting a fencing would be
     * false at every failover.
     */
    private void refuseSelfAddress(Lease lease, IOException because) throws IOException {
        if (!lease.holderPodId().equals(leaseConfig.podId())) {
            return;
        }
        IOException refused = new IOException("the lease names this pod itself ("
                + leaseConfig.podId() + " at " + lease.holderEndpoint() + ", epoch "
                + lease.epoch() + ") while this pod is forwarding, so this commit has no"
                + " reachable sequencer: either this pod's own term is fenced or closed and"
                + " the lease cannot move until it expires, or it is mid-promotion and has"
                + " not published its sequencer yet");
        if (because != null) {
            // ⚠️ CHAINED, like the sibling refusal two branches up: the peer's
            // "not the leaseholder" is why we re-read at all, and dropping it
            // leaves an operator with the conclusion and none of the evidence.
            refused.initCause(because);
        }
        throw refused;
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

    /**
     * The epoch of the lease this pod last forwarded to (M5.15d).
     *
     * <p>⚠️ IT ISSUES NO REQUEST, which is the whole of its cost story. Every
     * commit reads the lease already, so the epoch is in hand; an earlier draft
     * read it again here and review measured the consequence -- this follower's
     * lease GET rate doubled, one per flush becoming two, the same object
     * decoded twice. cost.md R4 calls that a read to coalesce, not to repeat.
     *
     * <p>⚠️ SO IT IS THE LAST OBSERVED EPOCH, NOT A FRESH ONE, and a pod that
     * has never committed answers {@link Sequencer#EPOCH_UNKNOWN}. That is
     * honest rather than convenient: what a consumer is told is the epoch this
     * pod's commits are actually being routed by, and before the first commit
     * there is no such thing.
     */
    @Override
    public long epoch() {
        return lastEpoch;
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }
}
