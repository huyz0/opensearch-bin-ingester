// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Leads when it can, forwards when it cannot (M5.6, FR-11).
 *
 * <p>⚠️ THIS IS THE SEQUENCER A POD ACTUALLY GETS, and until now no production
 * code chose one at all: {@code DefaultIngest} takes a {@link Sequencer} and
 * every construction site in the tree was a test. So "wire forwarding into the
 * ingest path" means building the thing that decides — and the decision is not
 * a startup flag, it is "do I hold the lease right now".
 *
 * <p>⚠️ RESOLVED PER COMMIT, NOT ONCE AT STARTUP. A pod that lost a lease
 * election at boot must still be able to lead when the holder dies, and a pod
 * that held one must stop writing the chain the moment it is fenced. Choosing
 * once would make the first case a permanent follower and the second a split
 * brain. {@link Leadership} owns that decision and its sharp edges; this class
 * owns only what to do with the answer.
 *
 * <p>⚠️ A FENCED LEADER FALLS BACK, IT DOES NOT FAIL — and it is told apart by
 * {@link FencedException} rather than by its message. A fenced commit appended
 * nothing, so re-sending it is not a duplicate; any other {@link IOException}
 * may have landed and lost its reply, and re-sending THAT ELSEWHERE commits
 * the same records twice — M5.23 answers such a resend only at the sequencer
 * instance that made the append, never after the lease moves (M5.25). So the pod keeps its
 * term through an ambiguous failure: a transient 503 on a delta PUT is not a
 * takeover.
 *
 * <p>⚠️ NOT SYNCHRONIZED, AND THE FIRST DRAFT WAS. A monitor held for the
 * length of a commit admits one caller at a time, so a leader wrapped in
 * {@code BatchingSequencer} — whose purpose is folding a WINDOW of flushes into
 * one delta PUT — could never batch: six pods forwarding would pay six
 * sequential windows and six PUTs where the design demands one, the per-pod PUT
 * rate non-negotiable 6 forbids by name. It would also span the election, the
 * commit PUT and the forwarding RPC, so one hung store call parks every caller
 * with no timeout (java-style.md rule 6).
 *
 * <p>⚠️ WHAT THIS CLASS DOES NOT YET REFUSE, stated here rather than
 * discovered: a commit after {@link #close}, and a commit by a pod the lease
 * still NAMES while its own sequencer is fenced — ADR-0027's self-fence, where
 * forwarding would address this pod's own endpoint. It also elects on every
 * commit while it does not lead, rather than only into an expired lease, which
 * costs a {@code stat} and a {@code get} the lease read has already answered.
 * Those are M5.6c; nothing in production constructs this class yet, so the gap
 * is a gap in the class rather than in a running pod.
 */
public final class FleetSequencer implements Sequencer {

    private final Leadership leadership;
    private final RemoteSequencer remote;

    public FleetSequencer(BinStore store, LeaseConfig config,
            SequencerTransport transport, Leadership.Election election) {
        // ⚠️ EVERY ARGUMENT IS CHECKED BEFORE A TERM IS TAKEN, and the order is
        // the whole point. `new Leadership(...)` ELECTS in its constructor: it
        // acquires the lease, starts a renewer, seals the ancestor and replays
        // the chain. A `requireNonNull` after that throws out of a constructor
        // whose object is then discarded -- and with it the only reference that
        // could ever call `close()` and release what was just taken. The lease
        // would name a dead pod and be renewed every renew interval forever, so
        // no pod could lead and, the constructor having thrown, none could
        // forward here either: the fleet stops committing for the life of the
        // JVM, from one null argument. `LocalSequencer.start` carries two
        // comments about this exact failure, in the layer below.
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(transport, "transport");
        this.remote = new RemoteSequencer(store, config, transport);
        this.leadership = new Leadership(election);
    }

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        Sequencer mine = leadership.sequencer();
        FencedException fence = null;
        if (mine != null) {
            try {
                return mine.commitAll(requests);
            } catch (FencedException fenced) {
                // ⚠️ OUR TERM WENT, and this is the ONLY failure that says so
                // without ambiguity, so it is the only one that falls through
                // to forwarding. Everything else propagates from here.
                leadership.retire(mine, fenced);
                fence = fenced;
            }
        }
        try {
            return remote.commitAll(requests);
        } catch (IOException forwardFailed) {
            if (fence != null) {
                // ⚠️ THE FENCE SURVIVES THE FORWARD'S FAILURE. Forwarding starts
                // with a lease GET, so a store hiccup here reports a plain
                // IOException -- and `Sequencer.commit` defines that as "may
                // have landed and lost its reply", which is the one thing that
                // is FALSE about this failure: a fence proves nothing was
                // appended and the forward never reached a peer. Dropping the
                // fence destroys the evidence that authorises a safe retry,
                // and leaves an operator debugging a stalled producer with no
                // sign a takeover happened. ⚠️ THE FENCE IS THE STRONGER
                // EVIDENCE, and stays so after M5.23: reconciling an ambiguous
                // append makes a resend safe to THAT SEQUENCER only -- the mark
                // is one instance's field, so not even the same pod's next term
                // holds it -- while a fence proves nothing was appended at all
                // and so authorises a resend anywhere, including to a
                // successor, which does not inherit an ambiguously-landed
                // flush (M5.25).
                forwardFailed.addSuppressed(fence);
            }
            throw forwardFailed;
        }
    }

    /** Whether this pod is currently the one writing the chain. */
    public boolean leading() {
        return leadership.isHeld();
    }

    @Override
    public void close() throws IOException {
        try {
            leadership.close();
        } finally {
            remote.close();
        }
    }
}
