// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.io.IOException;

/**
 * This sequencer's term ended, so its commit appended nothing (M5.6).
 *
 * <p>⚠️ A TYPE, BECAUSE THE MESSAGE WAS NOT SAFE TO MATCH ON. The caller this
 * exists for — the pod-level sequencer that forwards a commit it can no longer
 * write itself, M5.6 — first decided with the predicate {@code
 * message.contains("lease") || message.contains("fenced")}, over three refusals
 * that a fleet has to tell apart: {@link LocalSequencer} says {@code "this
 * sequencer RELEASED its lease"} after close and {@code "this sequencer LOST
 * its lease"} when fenced, and {@link CommitLog} says {@code "this writer is
 * fenced and must stop"} on discovering a successor's seal. ⚠️ It took the
 * FIRST of those — a deliberate shutdown — for a fencing, and the pod
 * re-elected itself on the way out. Note which half of the predicate carried
 * which: {@code "lease"} matched only the two LocalSequencer refusals, one of
 * them wrongly, and CommitLog's seal fence was reached solely by the word
 * {@code "fenced"} — a word no caller controls and any of the three could gain
 * with an edit to its wording.
 *
 * <p>⚠️ IT IS THE ONE FAILURE THAT IS SAFE TO RESEND, and that is the whole
 * reason it has a type. A fenced commit appended nothing — the conditional
 * write lost — so forwarding it to the new holder cannot duplicate anything.
 * Every other {@link IOException} from a commit is ambiguous: it may have landed
 * and lost its reply, and {@link Sequencer#commit} says resending one "commits
 * the same records again".
 */
public final class FencedException extends IOException {

    private static final long serialVersionUID = 1L;

    public FencedException(String message) {
        super(message);
    }
}
