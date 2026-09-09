// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;

/**
 * Assigns offsets and appends to the write-once commit log. One of the four
 * declared I/O seams, so it ships with a fake kept in step in the same commit.
 *
 * <p>⚠️ ORDERING IS ASSIGNED HERE, NOT AT WRITE TIME (ADR-0001). The segment is
 * already durable when {@link #commit} is called; what this returns is where its
 * records land in each stream's total order. That is why an abandoned commit
 * leaves no hole — nothing was reserved.
 *
 * <p>⚠️ THE STORE IS THE ONLY COORDINATOR (ADR-0002). There is no consensus
 * cluster, no lock and no quorum round: I1 ("no commit-log sequence number is
 * ever written twice") is held by the object store's own conditional write, and
 * that is the entire reason this system can be cheap.
 *
 * <h2>The contract</h2>
 *
 * <p><b>What a caller may assume once {@link #commit} returns normally:</b> the
 * offsets in the returned {@link CommitDelta} are final and will never be
 * reassigned (I2, and NFR-11 across a failover), and the delta is durable in the
 * commit log — so a caller may acknowledge the write (FR-4). ⚠️ This is the
 * whole point of the seam being blocking: returning before durability would make
 * FR-4 unprovable, and virtual threads make a blocking call the concurrent API.
 *
 * <p><b>What a caller may NOT assume:</b> that its records are visible to a
 * consumer yet — fan-out happens after the commit is durable, never before,
 * because notifying about an uncommitted offset lets a consumer read a record a
 * failover could un-assign (I4). Nor that {@code flushSeq} values arrive in
 * order.
 * ⚠️ THE KEY IS NOT {@code (podId, flushSeq)}: ADR-0036 measured that pair
 * unable to tell a RESTART from a REPLAY, because {@code flushSeq} restarts at
 * 0 while {@code podId} is stable. It is
 * {@code (podId, incarnationId, flushSeq)}. An earlier draft of this very sentence promised it, four lines above
 * the paragraph retracting it.
 *
 * <p><b>Failure and retry.</b>
 * {@link IOException} means the store was unreachable
 * ({@code BinStore}'s own contract distinguishes that from losing a CAS race,
 * which is an empty {@code Optional}). ⚠️ That is the AMBIGUOUS case, not a
 * clean failure: a conditional PUT whose response was lost has still landed.
 * <b>A resubmission of a triple this sequencer has already APPLIED is
 * answered</b> (M4.10d): it gets the offsets that already apply and appends
 * nothing. ⚠️ AND SINCE M5.23 THAT INCLUDES THE AMBIGUOUS CASE. A PUT whose
 * response was lost is never recorded as applied -- {@code commitAll} records
 * only after the write RETURNS -- so the failure names the SLOT it wrote to
 * ({@link AmbiguousAppendException}), and the next commit reads that slot and
 * learns from the chain what happened. A retry of anything the landed batch
 * carried is then answered rather than appended.
 * ⚠️ THE RETRY MUST REUSE THE TRIPLE. A caller that mints a
 * fresh {@code flushSeq} for the retry is submitting a different commit, and
 * the same records are committed twice.
 * ⚠️ AND NO PRODUCTION CALLER RESENDS ONE YET, which is stated rather than
 * implied: {@code DefaultIngest} builds its request once (M5.2) but has no
 * retry loop, and {@code RemoteSequencer} resends only a REFUSED forward, never
 * an ambiguous one. What M5.23 changes is that such a resend is now SAFE, not
 * that anything makes it.
 *
 * <p>⚠️ A SUCCESSOR INHERITS THE WINDOW FROM THE CHAIN (M5.1), rebuilt from the
 * replay that crosses from the predecessor, so a retry that crosses a takeover
 * is answered too — with two stated limits. A checkpoint remembers a pod's
 * LATEST incarnation only; and ⚠️ <b>AN AMBIGUOUSLY-LANDED FLUSH IS NOT IN THE
 * CHECKPOINT AT ALL</b>. {@code CheckpointWriter} learns only from a commit
 * that RETURNED, so the flush M5.23 reconciles is known to the running window
 * and to the chain, but not to the map a successor treats as authoritative
 * below the checkpoint bound. While that delta is still in the uncheckpointed
 * tail the successor's replay finds it; once a later checkpoint bounds past it,
 * a retry crossing the takeover is applied twice. Recorded as M5.25, and stated
 * here rather than left to be discovered.
 *
 * <p>⚠️ THE TRIPLE IS NOT THE WHOLE KEY OF AN ANSWER. A replay is DETECTED on
 * {@code (podId, incarnationId, flushSeq)}, but it is ANSWERED only by a
 * segment carrying that triple AND the same {@code segmentKey} — one flush may
 * submit several segments under one {@code flushSeq}, and the key alone would
 * answer a different flush that reused it. So resending a triple with a
 * DIFFERENT segment key is neither answered nor committed: it is refused, and
 * that refusal is permanent for that {@code flushSeq}. Deliberate — answering
 * with offsets that belong to other records is worse than failing.
 * ⚠️ AN EARLIER DRAFT SAID THE PAIR WAS CARRIED "so M4.10 can make the retry
 * safe WITHOUT CHANGING THE RECORD SHAPE". M4.10c changed it: ADR-0036 added
 * {@code incarnationId} to this record, because the pair could not discriminate
 * a restart. {@link binjava.sequencer.FakeSequencer} models the refusal.
 *
 * <p>⚠️ Losing a CAS race is NOT a failure and never surfaces here: an
 * implementation redrives internally, re-reading and rebuilding rather than
 * resubmitting the same bytes (ADR-0022 makes a lost race an empty
 * {@code Optional}, not an exception, so the two are distinguishable).
 *
 * <p>⚠️ M4 ships one implementation — the node holding the lease commits
 * directly. Commit forwarding, the remote implementation by which a
 * non-leaseholder reaches that node, is M5's and rides on the peer mesh built
 * there. Until it exists a multi-node deployment is not correct, which the M4
 * SPEC states plainly rather than leaving to be discovered.
 */
public interface Sequencer extends AutoCloseable {

    /**
     * Assigns offsets for one segment's runs and appends the delta to the log.
     *
     * @param request what to commit, and which node's flush it came from
     * @return the durable delta, carrying each run's assigned {@code firstOffset}.
     *     ⚠️ M4.7 batches many requests into ONE delta, after which a returned
     *     delta may carry runs this caller did not submit, so a caller must not
     *     treat the whole delta as its own. ⚠️ Selecting by {@link
     *     binjava.format.RunKey} is NOT sufficient either: two nodes may commit
     *     the same stream in one batch window. ⚠️ {@code RunCommit} still
     *     carries no pod attribution; since M4.10c {@code SegmentCommit} DOES,
     *     as an optional {@code Attribution}, which is what the replay-answer
     *     path matches a retry against, what M5.1 rebuilds a successor's window
     *     FROM, and what M5.23 seeds it from after an ambiguous append. It is
     *     not a selector for a caller's own offsets.
     *     ⚠️ SETTLED BY M4.7b: <b>a caller finds its offsets by the SEGMENT KEY
     *     it submitted</b> — {@code delta.segments()}, matched on
     *     {@code request.segmentKey()}. That works because each request brings
     *     its own segment, and {@code CommitLog.commitAll} REFUSES a batch whose
     *     submissions share a key, so the match is unique. This paragraph used
     *     to end "is M4.7's to settle", and the settlement is recorded here
     *     rather than only on the implementation, because this interface is
     *     what a caller reads and the obvious re-derivation — select by
     *     {@code RunKey} — is the one the sentence above warns against
     * @throws IOException the store was unreachable — ⚠️ AMBIGUOUS, so the
     *     commit may or may not have landed. Retrying with the same
     *     {@code (podId, incarnationId, flushSeq)} IS SAFE AGAINST THE SAME
     *     SEQUENCER (M5.23): the failure names the slot the append was
     *     attempted at, and the next commit reconciles it against the chain, so
     *     a retry of a commit that landed is answered with the offsets that
     *     already apply and appends nothing. ⚠️ IT IS NOT YET SAFE ACROSS A
     *     TAKEOVER -- a successor does not inherit an ambiguously-landed flush
     *     once a checkpoint bounds past its delta (M5.25). ⚠️ And a DIFFERENT
     *     triple is a different commit and duplicates the records; see the
     *     failure-and-retry note
     * @throws FencedException the ONE exception to the paragraph above: this
     *     sequencer's term ended, the append was refused and NOTHING landed, so
     *     the same request may be re-sent to whoever holds the term now. ⚠️ THE
     *     PROMISE IS NOT UNIFORM ACROSS IMPLEMENTATIONS, and a caller has to
     *     know that: a sequencer that writes the chain itself raises it, and one
     *     that FORWARDS to a peer does not — a peer's refusal reaches it as a
     *     transport-level failure it reports as a plain {@link IOException}.
     *     So {@code instanceof FencedException} means "safe to re-send
     *     ELSEWHERE"; its absence does not mean "unsafe from every
     *     implementation", nor — since M5.23 — "unsafe to re-send to THIS
     *     sequencer"
     */
    default CommitDelta commit(CommitRequest request) throws IOException {
        return commitAll(List.of(request));
    }

    /**
     * Assigns offsets for MANY flushes and appends them as ONE delta.
     *
     * <p>⚠️ THE PRIMITIVE, with {@link #commit} as the one-request case, and
     * that direction is deliberate: M4.7's cost argument is that the commit rate
     * scales with WINDOWS rather than pods, and an interface whose primitive is
     * one-at-a-time makes the batched path the exception rather than the rule.
     * A default that looped over {@code commit} would satisfy the compiler and
     * silently restore the per-pod PUT rate the milestone exists to remove.
     *
     * @param requests the flushes to commit together. ⚠️ Their segment keys must
     *     be DISTINCT — attribution depends on it, and an implementation refuses
     *     a batch that collides rather than picking a winner
     * @return the durable delta carrying every submitted segment
     * @throws IOException as {@link #commit}
     */
    CommitDelta commitAll(List<CommitRequest> requests) throws IOException;

    /**
     * Releases whatever this instance holds. ⚠️ For an implementation holding a
     * lease that means RELEASING it voluntarily rather than waiting out the
     * TTL — the difference between a failover in milliseconds and one bounded
     * by the lease TTL (ADR-0007 puts the TTL path at ~10 s worst case). After
     * {@code close} a {@link #commit} must fail rather than silently succeed.
     * ⚠️ {@link FakeSequencer}'s close is a no-op and it stays usable
     * afterwards; that is a fake's licence, not the contract.
     */
    @Override
    void close() throws IOException;
}
