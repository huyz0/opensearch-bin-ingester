// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;

/**
 * A commit the leaseholder REFUSED before writing anything, and will refuse
 * again unchanged (M13.34g review round 1, P1): a default-path run of a stream
 * the leader has in fast mode (ADR-0082 §7).
 *
 * <p>⚠️ DEFINITE, NEVER AMBIGUOUS, so it is answered and never deferred: a
 * forward that defers it writes an inbox intent every drain refuses again,
 * which stalls that pod's whole inbox. It travels between pods as its own
 * HTTP status, so a peer's refusal is this type on the forwarding side too.
 */
public final class CommitRefusedException extends IOException {

    private static final long serialVersionUID = 1L;

    public CommitRefusedException(String message) {
        super(message);
    }
}
