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
 * <p>⚠️ IT PROVES AN APPEND DID NOT HAPPEN, which is what makes it safe to
 * resend anywhere, and that is the whole reason it has a type. A fenced commit
 * appended nothing — the conditional write lost — so forwarding it to the new
 * holder cannot duplicate anything. Every other {@link IOException} from a
 * commit is ambiguous: it may have landed and lost its reply. ⚠️ IT IS NOT THE
 * ONLY SUCH PROOF, and an earlier wording of this sentence said it was: a peer
 * declining because it does not hold the lease
 * ({@code SequencerTransport.NotTheLeaseholderException}) says the same thing
 * across a transport, and it is what M5.5's follow-the-lease resend runs on.
 * What is true of both, and of nothing else, is that they are REFUSALS rather
 * than lost outcomes.
 *
 * <p>⚠️ AND THAT DISTINCTION SURVIVES M5.23, which narrowed rather than removed
 * it. {@link AmbiguousAppendException} lets a sequencer answer a resend of its
 * OWN lost-response append, by reading the slot it named — so a resend to THAT
 * SEQUENCER is now safe too. ⚠️ TO THAT SEQUENCER, NOT TO THAT POD: the mark
 * is one instance's field, so a pod that loses and re-acquires its lease holds
 * a fresh, empty one.
 *
 * <p>⚠️ WHAT STAYS TRUE, and what this type is for, is that a resend ELSEWHERE
 * is safe only on one of those two refusals. ⚠️ M5.25 carries an
 * ambiguously-landed flush into the checkpoint, so a successor DOES inherit it
 * — but for a pod's current incarnation only, and a caller here cannot tell
 * which incarnation a resend belongs to, so a lost outcome resent to a new
 * holder is still a duplicate waiting to happen.
 */
public final class FencedException extends IOException {

    private static final long serialVersionUID = 1L;

    public FencedException(String message) {
        super(message);
    }
}
