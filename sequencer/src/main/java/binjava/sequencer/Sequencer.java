// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;

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
 * order. ⚠️ And NOT that a given {@code (podId, flushSeq)} is applied at most
 * once — that is M4.10 and does not hold yet; see the failure-and-retry note
 * below. An earlier draft of this very sentence promised it, four lines above
 * the paragraph retracting it.
 *
 * <p><b>Failure and retry — and what is NOT yet guaranteed.</b>
 * {@link IOException} means the store was unreachable
 * ({@code BinStore}'s own contract distinguishes that from losing a CAS race,
 * which is an empty {@code Optional}). ⚠️ That is the AMBIGUOUS case, not a
 * clean failure: a conditional PUT whose response was lost has still landed.
 * So a retry is safe only once the sequencer ignores a replay of a
 * {@code (podId, flushSeq)} it has already applied — and <b>it does not yet</b>.
 * That is M4.10, and until it lands <b>a retry after an ambiguous failure may
 * assign a second set of offsets to the same records</b>. The pair is carried
 * from this first commit precisely so M4.10 can make the retry safe without
 * changing the record shape; it does not make it safe today, and
 * {@link binjava.sequencer.FakeSequencer} does not model it either.
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
     *     the same stream in one batch window, and neither {@code CommitDelta}
     *     nor {@code RunCommit} carries pod attribution.
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
     *     {@code (podId, flushSeq)} is safe only from M4.10; see the
     *     failure-and-retry note above
     */
    CommitDelta commit(CommitRequest request) throws IOException;

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
