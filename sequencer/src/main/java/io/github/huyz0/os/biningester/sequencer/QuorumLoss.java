// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

/**
 * The takeover's two predicates over an index (ADR-0081 §5 step 5; M13.33e):
 * whether it has lost quorum, and whether a stream of it may decide early.
 * Each reads only what survivors can know -- the rosters (members,
 * departures), pod-UID deletion and the recorded values -- never which pods
 * held copies, under which {@code q} an entry was assigned, or a value
 * received but not recorded.
 *
 * <p>⚠️ LOST IN T iff T's leader is GONE and not DEPARTED, and T's members
 * GONE and not DEPARTED, other than that leader, lie in at least
 * {@code q_min(T, I) - 1} distinct zones other than the leader's: an exposed
 * entry had copies on the leader and in {@code q - 1 >= q_min - 1} other
 * zones, all undeparted members, so it is lost only if all of those are. At
 * {@code q = 1} the leader's loss alone is quorum loss.
 *
 * <p>⚠️ A DEPARTED POD IS NEVER A LOSS: a departure waits for its copies to
 * commit, and a departing leader commits every exposed entry first (§9).
 */
public final class QuorumLoss {

    private QuorumLoss() {
    }

    /**
     * The least {@code wal_quorum} term {@code term} recorded for
     * {@code index}; empty where it recorded none, so assigned nothing of it.
     */
    static OptionalInt qMin(Roster term, UUID index) {
        return term.termRecord().stream()
                .filter(r -> r.walQuorum().containsKey(index))
                .mapToInt(r -> r.walQuorum().get(index))
                .min();
    }

    /** Whether {@code index} lost quorum in {@code term}, {@code gone} the deleted pod UIDs. */
    public static boolean lostIn(Roster term, UUID index, Set<String> gone) {
        Objects.requireNonNull(term, "term");
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(gone, "gone");
        OptionalInt q = qMin(term, index);
        if (q.isEmpty()) {
            return false;
        }
        Roster.Incarnation leader = term.leader();
        Roster.Member leading = term.member(leader.podUid()).orElseThrow();
        if (leading.state() == Roster.State.DEPARTED || !gone.contains(leader.podUid())) {
            return false;
        }
        Set<String> zones = new HashSet<>();
        for (Roster.Member m : term.members()) {
            String uid = m.incarnation().podUid();
            if (m.state() == Roster.State.ROSTERED && !uid.equals(leader.podUid())
                    && gone.contains(uid) && !m.incarnation().az().equals(leader.az())) {
                zones.add(m.incarnation().az());
            }
        }
        return zones.size() >= q.getAsInt() - 1;
    }

    /** Whether {@code index} lost quorum in some term of {@code unclosed}. */
    public static boolean lost(List<Roster> unclosed, UUID index, Set<String> gone) {
        for (Roster term : unclosed) {
            if (lostIn(term, index, gone)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a stream of {@code index}, which has not lost quorum, may
     * decide without waiting for its unanswered members: in every unclosed
     * term, the zones of its undeparted members that are unanswered or gone
     * number fewer than {@code q_min} -- so every exposed entry has a copy on
     * an answered member.
     */
    public static boolean mayDecideEarly(List<Roster> unclosed, UUID index,
            Set<String> unansweredOrGone) {
        Objects.requireNonNull(unansweredOrGone, "unansweredOrGone");
        for (Roster term : unclosed) {
            OptionalInt q = qMin(term, index);
            if (q.isEmpty()) {
                continue;
            }
            Set<String> zones = new HashSet<>();
            for (Roster.Member m : term.members()) {
                if (m.state() == Roster.State.ROSTERED
                        && unansweredOrGone.contains(m.incarnation().podUid())) {
                    zones.add(m.incarnation().az());
                }
            }
            if (zones.size() >= q.getAsInt()) {
                return false;
            }
        }
        return true;
    }
}
