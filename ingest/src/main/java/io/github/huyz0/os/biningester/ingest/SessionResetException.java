// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;

/**
 * What the ingester says when it cannot honour a resume: re-send FULL STATE
 * (M5.15c, SPEC criterion 11's second half).
 *
 * <p>⚠️ IT IS THE SERVER'S ANSWER, NOT THE CLIENT'S GUESS. A consumer whose
 * session the ingester no longer holds cannot tell that from a slow reply, and
 * a consumer that guessed would either re-send full state it did not need to
 * (harmless, and it makes every reconnect expensive) or resume against a
 * session that is gone (a skip, and silent). So the server says it.
 *
 * <p>⚠️ "FULL STATE" IS THE WHOLE KEY SET, NOT A DELTA. A reset means the
 * ingester has nothing to apply a delta to; a client that answered with
 * {@code add}/{@code remove} would be describing a difference from state
 * neither side holds.
 *
 * <p>⚠️ IT IS NOT A SEQUENCER FAILOVER, and that is the pairing SPEC criterion
 * 12 asks for in both directions. This exception names a SESSION and carries no
 * epoch of the chain; nothing about it says the leaseholder moved, and nothing
 * about the leaseholder moving produces it. An operator whose failover metric
 * counted these would see a failover every time a consumer reconnected after a
 * restart -- which is the reading the two counters exist to keep apart.
 *
 * <p>⚠️ AND IT IS NOT A PROTOCOL ERROR. A resume carrying an epoch behind what
 * the session has served, or different deltas at an epoch already served, is a
 * client defect: the session is intact and the request is wrong, so it is
 * refused with {@code IllegalStateException} and re-sending full state would
 * hide the defect by making it look like ordinary state loss.
 */
public final class SessionResetException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Why a session must be re-established — a CLOSED SET, which is what makes
     * the "nothing about the sequencer" property structural rather than a
     * claim about today's message text.
     *
     * <p>⚠️ A FREE-FORM REASON WAS THE FIRST DRAFT and review was right to call
     * it asserted rather than enforced: any caller could have concatenated
     * "the sequencer epoch moved" into a session signal, and the only thing
     * standing against it was a test matching the one string in the tree.
     * Adding a reason here is a deliberate act with a diff someone reads.
     */
    public enum Reason {
        /** The ingester no longer holds this session — a restart, or an operator drop. */
        NOT_HELD("the ingester no longer holds it");

        private final String text;

        Reason(String text) {
            this.text = text;
        }
    }

    private final String session;
    private final Reason reason;

    public SessionResetException(String session, Reason reason) {
        super("session " + session + " must be re-established: " + reason.text
                + ". Re-send FULL state -- every stream this consumer holds -- rather than a "
                + "delta; this is a session reset and says nothing about the sequencer");
        this.session = session;
        this.reason = reason;
    }

    /** Why, as a value rather than as text a caller has to parse. */
    public Reason reason() {
        return reason;
    }

    /** The session that must be re-established. */
    public String session() {
        return session;
    }
}
