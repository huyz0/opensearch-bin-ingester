// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import java.util.Objects;
import java.util.Set;

/**
 * Admission before assignment (ADR-0081 §2.3; M13.26d): the leader assigns a
 * batch only while AVAILABLE rostered pods -- itself included -- lie in at
 * least {@code q} distinct AZs; otherwise the batch is held, unassigned
 * (backpressure, never a refusal: cost.md rule 14).
 *
 * <p>⚠️ SO A DISCARD FOLLOWS ONLY A LOSS AFTER ASSIGNMENT: at most one
 * decision per stream per pod lost, never one per retry while an AZ stays
 * lost. Which pods are available -- ready, answering within the replica
 * timeout, not refusing -- is the replica set's to say (M13.27); this answers
 * whether those pods are enough.
 */
public final class FastAdmission {

    private FastAdmission() {
    }

    /**
     * Whether a batch of an index at {@code q} may be assigned now.
     *
     * @param roster this term's roster
     * @param availableUids the pod UIDs available to the leader, its own among them
     * @param q the index's {@code wal_quorum}, 1 to 3
     */
    public static boolean admits(Roster roster, Set<String> availableUids, int q) {
        Objects.requireNonNull(roster, "roster");
        Objects.requireNonNull(availableUids, "availableUids");
        if (q < 1 || q > 3) {
            throw new IllegalArgumentException("wal_quorum is 1 to 3: " + q);
        }
        long zones = roster.members().stream()
                .filter(m -> m.state() == Roster.State.ROSTERED)
                .map(Roster.Member::incarnation)
                .filter(i -> availableUids.contains(i.podUid()))
                .map(Roster.Incarnation::az)
                .distinct()
                .count();
        return zones >= q;
    }
}
