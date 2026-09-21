// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;

/**
 * How a pod's commit reaches the pod holding the lease (M5.3, FR-11).
 *
 * <p>⚠️ THE SEAM EXISTS SO FORWARDING IS TESTABLE WITHOUT A SOCKET. M4 shipped
 * only the LOCAL {@code Sequencer}, so today every pod that is not the
 * leaseholder has no way to commit at all — {@code M4/SPEC.md} states that
 * twice on purpose and calls it "the one deferral that changes what 'done'
 * means". The transport interface is what M5 adds under it -- ⚠️ THE INTERFACE
 * AND A TEST IMPLEMENTATION, never a production one, which is M5.6e (M8) -- and every behaviour that
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
     * such: the request may have been applied and only the reply lost. A caller
     * may resend a request that was REFUSED, never one whose outcome it does
     * not know — and every resend from here is to another pod, which is the
     * case that stays unsafe.
     *
     * <p>⚠️ M5.23 DOES NOT LIFT THAT, and this sentence used to date the rule
     * to M5.23's absence. What M5.23 gives is a leaseholder that reconciles its
     * own ambiguous append against the slot it named, so a resend to THE SAME
     * SEQUENCER is answered. ⚠️ THE SAME SEQUENCER, NOT THE SAME POD: the mark
     * that makes it answerable is one instance's field, so a pod that loses and
     * re-acquires its lease is the same pod holding a fresh, empty one.
     * ⚠️ A RESEND TO A SUCCESSOR IS ANSWERED SINCE M5.25 -- the reconciliation
     * writes the flush into the checkpoint too -- but only within a pod's
     * CURRENT incarnation, because a checkpoint remembers one per pod. This
     * transport cannot tell which case a resend is in, so it still does not
     * resend one whose outcome it does not know.
     *
     * @throws NotTheLeaseholderException when the peer answers that it does not
     *     hold the lease. ⚠️ A REFUSAL, NOT A FAILURE: nothing was applied, so
     *     re-reading the lease and resending is safe and is what M5.5 does.
     */
    CommitDelta send(String endpoint, CommitRequest request) throws IOException;

    /**
     * Asks the leaseholder at {@code endpoint} to drain the inbox (M8.14a,
     * ADR-0058), returning once it has applied every intent it could.
     *
     * <p>⚠️ **THE DEFAULT REFUSES**, and refusing is safe: the pod that asked
     * stays deferring, writing intents, rather than forwarding past its own.
     */
    default void drain(String endpoint, String requester) throws IOException {
        throw new IOException("this transport cannot ask a peer to drain the inbox");
    }

    @Override
    void close() throws IOException;

    /**
     * The peer declined because it is not the leaseholder.
     *
     * <p>⚠️ A DISTINCT TYPE BECAUSE THE TWO OUTCOMES DIFFER IN WHAT A CALLER
     * MAY DO. A plain {@link IOException} leaves the commit's fate unknown, so
     * it must not be resent TO A DIFFERENT SEQUENCER; this one says plainly
     * that nothing was applied, which is what makes following the lease to a
     * new holder safe. ⚠️ A {@code FencedException} raised by a sequencer that
     * writes the chain itself says the same thing; between them they are the
     * only two failures that PROVE an append did not happen, and proving that
     * is what authorises a resend elsewhere.
     */
    final class NotTheLeaseholderException extends IOException {
        private static final long serialVersionUID = 1L;

        public NotTheLeaseholderException(String message) {
            super(message);
        }
    }
}
