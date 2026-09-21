// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link SequencerTransport} fake: routes by endpoint, in one process.
 *
 * <p>⚠️ A SEAM SHIPS WITH ITS FAKE IN THE SAME COMMIT (code-structure.md rule
 * 5). The behaviours M5 must prove — following a moved lease, refusing a fenced
 * holder, not duplicating a forwarded commit — are properties of the CALLER,
 * and a socket would only make them slower to assert and flakier to trust.
 *
 * <p>⚠️ IT REFUSES RATHER THAN GUESSING. An endpoint it does not know is a
 * {@link SequencerTransport.NotTheLeaseholderException}, which is what a real
 * peer returns when it does not hold the lease. Routing to "whichever sequencer
 * we have" would make every test pass regardless of whether the caller read the
 * lease at all — the fake would be answering the question under test.
 */
public final class InProcessTransport implements SequencerTransport {

    private final Map<String, Sequencer> peers = new ConcurrentHashMap<>();
    private final java.util.Set<String> unreachable =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final List<String> sentTo = new ArrayList<>();
    private final List<CommitRequest> sent = new ArrayList<>();

    /** Puts {@code sequencer} behind {@code endpoint}. */
    public InProcessTransport at(String endpoint, Sequencer sequencer) {
        peers.put(endpoint, sequencer);
        // ⚠️ PUTTING A PEER BACK MAKES IT ANSWER AGAIN. A separate `reachable`
        // was written and removed: it had no caller, and it did not undo what
        // `unreachable` did, so using it would have produced a REFUSAL -- the
        // exact confusion this pair exists to remove.
        unreachable.remove(endpoint);
        return this;
    }

    /**
     * Removes the peer at {@code endpoint}, as a pod going away does.
     *
     * <p>⚠️ THIS MODELS A POD THAT ANSWERS, NOT ONE THAT IS DEAD. A removed
     * endpoint is answered with {@link SequencerTransport.NotTheLeaseholderException} below —
     * a REFUSAL, which is what a live pod says when the term is not its own.
     * Use {@link #unreachable} for a pod that says nothing.
     */
    public void gone(String endpoint) {
        peers.remove(endpoint);
    }

    /**
     * Makes {@code endpoint} answer NOTHING, as a dead node does (M5.6j).
     *
     * <p>⚠️ WITHOUT THIS THE FIXTURE COULD ONLY MODEL A DEAD LEADER AS A
     * REFUSING ONE, and that gap let a liveness regression pass a full green
     * suite: a design that promoted a follower on
     * {@link SequencerTransport.NotTheLeaseholderException} alone elected no
     * successor when a leader was lost to node failure, because a dead pod
     * raises no such thing — {@code SequencerTransport.send}'s own javadoc says
     * that type means "the peer ANSWERS that it does not hold the lease", and
     * everything else is a plain ambiguous {@code IOException}. ADR-0039
     * records the design and why it was withdrawn; this is the guard that makes
     * writing it a second time fail a test instead of a cluster.
     */
    public void unreachable(String endpoint) {
        // ⚠️ THE PEER IS LEFT REGISTERED, deliberately. `send` checks this set
        // BEFORE the lookup, so removing it would change nothing observable --
        // and leaving it is what makes the fixture model the real thing: a dead
        // pod still holds its lease and is still the pod the lease names. Put
        // it back with {@link #at}, which clears this.
        this.unreachable.add(endpoint);
    }

    /** Every endpoint a send was addressed to, in order. */
    public List<String> sentTo() {
        synchronized (sentTo) {
            return List.copyOf(sentTo);
        }
    }

    /** Every request handed to this transport, in order. */
    public List<CommitRequest> sent() {
        synchronized (sent) {
            return List.copyOf(sent);
        }
    }

    private volatile io.github.huyz0.os.biningester.binstore.BinStore inboxStore;
    private volatile String inboxPrefix;

    /**
     * Lets a peer drain the inbox in {@code store} under {@code prefix}, as the
     * real route does (M8.14a). ⚠️ Without it a drain is refused, the seam's
     * safe default, and a pod that deferred keeps deferring.
     */
    public InProcessTransport withInbox(io.github.huyz0.os.biningester.binstore.BinStore store, String prefix) {
        this.inboxStore = store;
        this.inboxPrefix = prefix;
        return this;
    }

    @Override
    public void drain(String endpoint, String requester) throws IOException {
        if (unreachable.contains(endpoint)) {
            throw new IOException("no answer from " + endpoint + "; the pod is not reachable");
        }
        Sequencer peer = peers.get(endpoint);
        if (peer == null) {
            throw new NotTheLeaseholderException(
                    "no sequencer at " + endpoint + " -- it does not hold the lease");
        }
        if (inboxStore == null) {
            SequencerTransport.super.drain(endpoint, requester);
            return;
        }
        InboxDrain.drain(inboxStore, inboxPrefix, peer, requester);
    }

    @Override
    public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
        synchronized (sentTo) {
            sentTo.add(endpoint);
        }
        synchronized (sent) {
            sent.add(request);
        }
        if (unreachable.contains(endpoint)) {
            // ⚠️ A PLAIN IOException, and the distinction is the whole point of
            // this branch: nothing answered, so the outcome of the request is
            // UNKNOWN. A refusal would be a claim about the lease that no dead
            // pod is in a position to make.
            throw new IOException("no answer from " + endpoint + "; the pod is not reachable");
        }
        Sequencer peer = peers.get(endpoint);
        if (peer == null) {
            throw new NotTheLeaseholderException(
                    "no sequencer at " + endpoint + " -- it does not hold the lease");
        }
        return peer.commit(request);
    }

    @Override
    public void close() {
        peers.clear();
    }
}
