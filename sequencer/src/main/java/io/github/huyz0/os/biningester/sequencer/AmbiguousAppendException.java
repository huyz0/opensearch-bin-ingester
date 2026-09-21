// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;

/**
 * A chain append whose outcome is unknown, and WHERE it was attempted (M5.23).
 *
 * <p>⚠️ AMBIGUITY IS NOT FAILURE. A conditional PUT whose response was lost has
 * still landed; one the store never saw has not; and {@code BinStore} cannot
 * tell a caller which happened -- an {@link IOException} means "unreachable",
 * and only a lost CAS race is reported definitely, as an empty {@code
 * Optional}. So the writer is left holding a commit it can neither acknowledge
 * nor safely repeat.
 *
 * <p>⚠️ THE SLOT IS THE WHOLE POINT OF THE TYPE. The chain is the only witness
 * to what happened, and a witness can only be questioned if the question has an
 * address: {@code (epoch, sequence)} names the one object that either holds
 * this append or does not. Without it a caller can only guess, and guessing is
 * what committed the same records twice before this row.
 *
 * <p>⚠️ IT IS NOT {@link FencedException}, and the difference is the opposite
 * of a detail. A fenced append PROVABLY appended nothing, so it may be re-sent
 * to whoever holds the term now. This one may have landed, so re-sending it
 * elsewhere is exactly the duplicate {@code Sequencer#commit} warns about --
 * it must be reconciled against the slot it names first.
 */
public final class AmbiguousAppendException extends IOException {

    private static final long serialVersionUID = 1L;

    private final long epoch;
    private final long sequence;

    /**
     * @param epoch the chain the append was attempted in
     * @param sequence the slot it was attempted at
     * @param cause the store failure that lost the outcome, kept because it is
     *     the only thing that says WHY -- a timeout, a reset connection and a
     *     503 are different operational stories and the message here says none
     *     of them
     */
    public AmbiguousAppendException(long epoch, long sequence, IOException cause) {
        super("the append to sequence " + sequence + " of epoch " + epoch
                + " may or may not have landed; its response was lost", cause);
        this.epoch = epoch;
        this.sequence = sequence;
    }

    /** The chain the append was attempted in. */
    public long epoch() {
        return epoch;
    }

    /** The slot that either holds the append or does not. */
    public long sequence() {
        return sequence;
    }
}
