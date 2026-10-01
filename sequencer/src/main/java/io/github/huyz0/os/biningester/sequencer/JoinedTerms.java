// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.util.TreeSet;

/**
 * The terms a pod has joined (ADR-0081 §1; M13.26d): it holds no entry and
 * sends no batch of a term it has not been told, by JOINED, that it joined --
 * so the incarnations that can hold a term's copies are its roster's members.
 *
 * <p>⚠️ A CLOSED TERM IS HELD BY NOBODY: every JOINED carries
 * {@code closedThrough}, and a term at or below it is forgotten here as its
 * entries are dropped from the journal.
 */
public final class JoinedTerms {

    private final TreeSet<Long> joined = new TreeSet<>();
    private long closedThrough;

    /** A JOINED for {@code epoch} arrived, naming the highest closed epoch. */
    public synchronized void joined(long epoch, long closedThrough) {
        if (epoch < 1 || closedThrough < 0) {
            throw new IllegalArgumentException("a joined term is at least 1, a closed one "
                    + "never negative");
        }
        this.closedThrough = Math.max(this.closedThrough, closedThrough);
        if (epoch > this.closedThrough) {
            joined.add(epoch);
        }
        joined.headSet(this.closedThrough, true).clear();
    }

    /** Whether this pod may journal, or send, an entry of {@code epoch}. */
    public synchronized boolean mayHold(long epoch) {
        return epoch > closedThrough && joined.contains(epoch);
    }
}
