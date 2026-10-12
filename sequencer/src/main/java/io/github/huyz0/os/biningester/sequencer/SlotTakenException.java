// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

/**
 * A recovery entry's slot holds another entry, now folded into the log
 * (M13.33o review round 2, T3): a commit whose answer was lost landed there.
 * The writer re-checks its offsets and builds the entry again at the next
 * slot.
 */
final class SlotTakenException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    SlotTakenException(long slot) {
        super("slot " + slot + " holds another entry");
    }
}
