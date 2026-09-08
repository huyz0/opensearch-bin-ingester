// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;

/**
 * How a pod's commit reaches the pod holding the lease (M5.3, FR-11).
 *
 * <p>⚠️ THE SEAM EXISTS SO FORWARDING IS TESTABLE WITHOUT A SOCKET. M4 shipped
 * only the LOCAL {@code Sequencer}, so today every pod that is not the
 * leaseholder has no way to commit at all — {@code M4/SPEC.md} states that
 * twice on purpose and calls it "the one deferral that changes what 'done'
 * means". The transport is what M5 adds under it, and every behaviour that
 * matters — following a moved lease, refusing a fenced holder, not duplicating
 * — is a property of the CALLER, not of the wire.
 *
 * <p>⚠️ IT CARRIES A REQUEST, NOT A CONNECTION. `CommitRequest` is
 * {@code (podId, incarnationId, flushSeq, segmentKey, recordCounts)} — all
 * values, meaningful when they arrive from another pod. M4 shaped it that way
 * deliberately "so that the record shape does not have to change when the
 * remote implementation lands", and it has not.
 *
 * <p>⚠️ THE ENDPOINT IS AN ARGUMENT, NOT STATE. The lease object is the truth
 * about who holds it (ADR-0012: "a peer hint may accelerate, never decide"), so
 * a forwarding pod re-reads it rather than caching a peer. A transport that
 * remembered an endpoint would be a second, weaker source of that truth.
 */
public interface SequencerTransport extends AutoCloseable {

    /**
     * Sends {@code request} to the sequencer at {@code endpoint}.
     *
     * <p>⚠️ AN {@link IOException} HERE IS AMBIGUOUS and must be treated as
     * such: the request may have been applied and only the reply lost. Until
     * M5.23 reconciles an ambiguous commit from the chain, a caller may resend
     * a request that was REFUSED, never one whose outcome it does not know —
     * {@code Sequencer.commit}'s own contract says resending an unknown outcome
     * "commits the same records again".
     *
     * @throws NotTheLeaseholderException when the peer answers that it does not
     *     hold the lease. ⚠️ A REFUSAL, NOT A FAILURE: nothing was applied, so
     *     re-reading the lease and resending is safe and is what M5.5 does.
     */
    CommitDelta send(String endpoint, CommitRequest request) throws IOException;

    @Override
    void close() throws IOException;

    /**
     * The peer declined because it is not the leaseholder.
     *
     * <p>⚠️ A DISTINCT TYPE BECAUSE THE TWO OUTCOMES DIFFER IN WHAT A CALLER
     * MAY DO. A plain {@link IOException} leaves the commit's fate unknown and
     * must not be resent; this one says plainly that nothing was applied, which
     * is the only case where following the lease to a new holder is safe.
     */
    final class NotTheLeaseholderException extends IOException {
        private static final long serialVersionUID = 1L;

        public NotTheLeaseholderException(String message) {
            super(message);
        }
    }
}
