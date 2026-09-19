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
 * the same records twice unless the new holder inherited it — M5.23 answers
 * such a resend at the sequencer instance that made the append, and M5.25
 * carries it into the checkpoint a successor reads, for a pod's CURRENT
 * incarnation only. So the pod keeps its
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
 * <p>⚠️ IT REFUSES A COMMIT AFTER {@link #close}, and it refuses to forward to
 * ITSELF — the pod the lease still names while its own sequencer is fenced,
 * which is ADR-0027's self-fence (M5.6c). Both used to fall through to
 * forwarding, and the second of them routed the request straight back here.
 *
 * <p>⚠️ WHAT IT STILL DOES, stated here rather than discovered: it elects on
 * every commit while it does not lead, rather than only into an EXPIRED lease,
 * costing a {@code stat} and a {@code get} the lease read has already answered
 * — measured at ~13% of the write-path bill. That is M5.6g. Nothing in
 * production constructs this class yet (M5.6e), so the gap is a gap in the
 * class rather than in a running pod.
 */
public final class FleetSequencer implements Sequencer {

    private final Leadership leadership;
    private final RemoteSequencer remote;
    private final BinStore store;
    private final String prefix;
    private final String podId;

    /**
     * ⚠️ VOLATILE, because {@link #close} and a commit reach it from different
     * threads by construction: a SIGTERM handler closes while an in-flight
     * flush is still inside {@code commitAll}. {@link Leadership} guards its own
     * closed flag the same way and for the same reason.
     */
    private volatile boolean closed;

    public FleetSequencer(BinStore store, LeaseConfig config,
            SequencerTransport transport, Leadership.Election election) {
        this(store, config, transport, election, LeaseChallenge.NEVER);
    }

    /**
     * ⚠️ THE FORWARD READS THE SAME EVIDENCE THE ELECTION DOES (M8.57), so a
     * holder the watch reports gone costs the probe window rather than the
     * peer-commit timeout a frozen holder would otherwise hold a flush for.
     */
    public FleetSequencer(BinStore store, LeaseConfig config,
            SequencerTransport transport, Leadership.Election election,
            LeaseChallenge challenge) {
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
        Objects.requireNonNull(challenge, "challenge");
        this.remote = new RemoteSequencer(store, config, transport, challenge);
        this.store = store;
        this.prefix = config.prefix();
        this.podId = config.podId();
        this.leadership = new Leadership(election);
    }

    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        if (closed) {
            // ⚠️ REFUSED HERE, BECAUSE THE FALLBACK WOULD SUCCEED. `Leadership`
            // already refuses to ELECT after close, so `sequencer()` returns
            // null -- and without this the commit fell straight through to
            // forwarding, which reads the lease and sends. A pod that has
            // released its lease and reported a clean shutdown then goes on
            // committing through a peer for as long as anything holds a
            // reference to it: an in-flight flush, a queued batch.
            // ⚠️ NOT A `FencedException`. Nothing was appended, but this is a
            // deliberate shutdown rather than a lost term, and a caller
            // re-sending on the strength of a fence would be re-sending because
            // THIS pod stopped -- which says nothing about who holds the lease.
            throw new IOException("this pod's sequencer was closed and must not commit again");
        }
        Sequencer mine = leadership.sequencer();
        FencedException fence = null;
        if (mine != null) {
            if (deferring) {
                // ⚠️ THIS POD's OWN INTENTS FIRST: committing past them here
                // would raise its high mark over them, and the drain would then
                // refuse each as a replay -- acked writes lost (M8.14a).
                InboxDrain.drain(store, prefix, mine, podId);
                deferring = false;
            }
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
        if (closed) {
            // ⚠️ RE-CHECKED BEFORE FORWARDING, because the entry check alone
            // leaves the exact caller the flag exists for: an in-flight flush
            // that entered before `close` and reaches here after it. The window
            // is the local commit above, which is a store round trip wide.
            throw new IOException("this pod's sequencer was closed while this commit was in"
                    + " flight, and must not forward it");
        }
        if (deferring) {
            // ⚠️ STILL DEFERRING: this pod has intents in the inbox, so it may
            // not forward until the leaseholder has applied them -- the drain
            // it asks for here IS the heal (M8.14a). Cut off still, it defers.
            try {
                remote.drain();
                deferring = false;
            } catch (IOException stillCutOff) {
                throw defer(requests, stillCutOff, fence);
            }
        }
        try {
            return remote.commitAll(requests);
        } catch (SequencerTransport.NotTheLeaseholderException refused) {
            // ⚠️ A DEFINITE "NOT APPLIED" IS NOT A PARTITION: it is answered,
            // not deferred, and the caller retries where the lease now is.
            if (fence != null) {
                refused.addSuppressed(fence);
            }
            throw refused;
        } catch (IOException forwardFailed) {
            // ⚠️ A REFUSAL REWRAPPED IS STILL A REFUSAL: `RemoteSequencer`
            // reports "refused, and the lease has not moved" as a plain
            // IOException over the refusal. Nothing was applied, so it is
            // answered, not deferred.
            boolean refused = forwardFailed.getCause()
                    instanceof SequencerTransport.NotTheLeaseholderException;
            if (requests.size() == 1 && !refused) {
                throw defer(requests, forwardFailed, fence);
            }
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
                // and so authorises a resend anywhere -- including to a
                // successor, which inherits an ambiguously-landed flush since
                // M5.25 but only for a pod's current incarnation.
                forwardFailed.addSuppressed(fence);
            }
            throw forwardFailed;
        }
    }

    /**
     * Makes the one request's intent durable and returns the deferral to throw
     * (M8.14a, ADR-0058) -- or, if the store refuses the intent, the original
     * failure: ⚠️ nothing durable to ack on, so criterion 12's acks stop.
     */
    private IOException defer(List<CommitRequest> requests, IOException cause,
            FencedException fence) {
        if (fence != null) {
            cause.addSuppressed(fence);
        }
        if (requests.size() != 1) {
            return cause;
        }
        try {
            String key = Inbox.write(store, prefix, requests.get(0));
            deferring = true;
            return new CommitDeferredException(key, cause);
        } catch (IOException intentFailed) {
            cause.addSuppressed(intentFailed);
            return cause;
        }
    }

    /**
     * ⚠️ WHETHER THIS POD HAS INTENTS THE LEASEHOLDER HAS NOT CONFIRMED
     * APPLYING. While true it forwards nothing: its next flush asks for a drain
     * first. Volatile because a close and a flush reach it from two threads.
     */
    private volatile boolean deferring;

    /** Whether this pod is currently the one writing the chain. */
    public boolean leading() {
        return leadership.isHeld();
    }

    /**
     * The term this pod holds, or {@code null} where it holds none (M8.4).
     *
     * <p>⚠️ **THIS AND NOT {@code this} IS WHAT ANSWERS A FORWARDED COMMIT.**
     * A peer forwards because it is not the leaseholder; a service that applied
     * the request through the enclosing {@link FleetSequencer} would consult
     * the lease again and forward it onward — two pods bouncing one commit
     * between them, and if the lease moved mid-flight, back to where it came
     * from. The receiving side must commit LOCALLY or refuse, which is the 409
     * {@code SequencerTransport} defines.
     *
     * <p>⚠️ **IT DOES NOT ELECT**, for {@link #chain()}'s reason: answering a
     * question about the current term must not take one.
     */
    public Sequencer heldTerm() {
        return leadership.heldWithoutElecting();
    }

    /**
     * The chain this pod's own term has written, or empty where it does not
     * lead (M8.3).
     *
     * <p>⚠️ **EMPTY IS THE ANSWER FOR A FOLLOWER, AND IT IS NOT AN ERROR.** A
     * follower writes no chain and runs no retention pass: GC is the
     * leaseholder's work (M7.10's {@code LeasedGc}), and a follower that
     * produced a chain here would be handing a pass the deltas of a term it
     * does not hold — condemning segments it cannot fence.
     *
     * <p>⚠️ **IT DOES NOT ELECT.** Asking {@code Leadership.sequencer()} would
     * take a TERM to answer a question about the current one, which is the same
     * reason {@link #epoch()} reads the held term first.
     */
    public java.util.Optional<ChainMemory> chain() {
        return LocalSequencer.underneath(leadership.heldWithoutElecting())
                .map(LocalSequencer::chain);
    }

    /**
     * The epoch this pod's commits land under, whichever way they travel
     * (M5.15d).
     *
     * <p>⚠️ THE LEADER'S OWN, WHEN THIS POD LEADS, and the leaseholder's when
     * it forwards -- which is the same number by construction, since a follower
     * forwards to the holder of the lease whose epoch it is reading. Asking the
     * leadership first avoids the store read {@link RemoteSequencer#epoch()}
     * pays, on the pod where the answer is already in hand.
     *
     * <p>⚠️ IT DOES NOT ELECT. Asking {@code Leadership.sequencer()} would take
     * a term to answer a question -- acquiring a lease, sealing an ancestor and
     * replaying a chain -- so this reads the term already held and forwards
     * otherwise.
     */
    @Override
    public long epoch() {
        Sequencer mine = leadership.heldWithoutElecting();
        return mine != null ? mine.epoch() : remote.epoch();
    }

    @Override
    public void close() throws IOException {
        // ⚠️ THE FLAG GOES UP FIRST, so a commit racing this shutdown is refused
        // rather than forwarded, exactly as `Leadership.close` raises its own
        // before taking the term away.
        closed = true;
        IOException failed = null;
        try {
            try {
                leadership.close();
            } catch (IOException releaseFailed) {
                failed = releaseFailed;
            }
        } finally {
            // ⚠️ A `finally`, NOT A SECOND `catch`. An earlier draft caught only
            // IOException, so an UNCHECKED throwable out of the lease release --
            // `BatchingSequencer.close`'s join, a `BoundedLock` timeout, an SDK
            // RuntimeException -- skipped this and leaked the transport. The
            // original code got that right with `finally` and lost it while
            // fixing the exception it swallowed; both properties are wanted.
            closeTransport(failed);
        }
        if (failed != null) {
            throw failed;
        }
    }

    /**
     * Closes the transport, attaching its failure to {@code failed} if there is
     * one.
     *
     * @param failed the lease-release failure, or null
     */
    private void closeTransport(IOException failed) throws IOException {
        try {
            remote.close();
        } catch (IOException transportFailed) {
            // ⚠️ ATTACHED, NEVER SUBSTITUTED, and a `finally` did substitute it.
            // The lease release is the failure that matters to the fleet -- it
            // says the lease was NOT handed back, so every other pod waits out
            // the TTL instead of taking over in milliseconds (ADR-0007 puts
            // that at ~10 s). Letting the transport's failure replace it threw
            // away the one fact a successor's timing depends on.
            if (failed == null) {
                // ⚠️ THROWN FROM HERE when the release succeeded, because there
                // is nothing to attach it to and losing it would report a clean
                // shutdown over a transport still holding its peers.
                throw transportFailed;
            }
            failed.addSuppressed(transportFailed);
        }
    }
}
