// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.util.Optional;
import java.util.TreeMap;

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

    /** Each joined term, by epoch, and its leader's UID: empty where it was not recorded. */
    private final TreeMap<Long, String> joined = new TreeMap<>();
    private long closedThrough;

    /** A JOINED for {@code epoch} arrived, naming the highest closed epoch. */
    public synchronized void joined(long epoch, long closedThrough) {
        joined(epoch, closedThrough, "");
    }

    /**
     * The same, from {@code leaderUid}, the term's leader (M13.28a): a holder
     * journals a REPLICA only from it.
     */
    public synchronized void joined(long epoch, long closedThrough, String leaderUid) {
        if (epoch < 1 || closedThrough < 0) {
            throw new IllegalArgumentException("a joined term is at least 1, a closed one "
                    + "never negative");
        }
        java.util.Objects.requireNonNull(leaderUid, "leaderUid");
        this.closedThrough = Math.max(this.closedThrough, closedThrough);
        if (epoch > this.closedThrough) {
            joined.put(epoch, leaderUid);
        }
        joined.headMap(this.closedThrough, true).clear();
    }

    /** Whether this pod may journal, or send, an entry of {@code epoch}. */
    public synchronized boolean mayHold(long epoch) {
        return epoch > closedThrough && joined.containsKey(epoch);
    }

    /** The leader of joined term {@code epoch}, where its join recorded one (M13.28a). */
    public synchronized Optional<String> leaderOf(long epoch) {
        String leader = mayHold(epoch) ? joined.get(epoch) : null;
        return leader == null || leader.isEmpty() ? Optional.empty() : Optional.of(leader);
    }
}
