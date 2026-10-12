// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Roster;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The quorum-loss predicate and the early decision's zone coverage (ADR-0081
 * §5 step 5; M13.33e): from what survivors can know -- the rosters, pod-UID
 * deletion and the recorded values -- never which pods held copies.
 */
class QuorumLossTest {

    private static final UUID I = new UUID(1, 1);
    private static final UUID OTHER = new UUID(2, 2);
    private static final Roster.Incarnation L = pod("l", "az-a");
    private static final Roster.Incarnation A2 = pod("a2", "az-a");
    private static final Roster.Incarnation B = pod("b", "az-b");
    private static final Roster.Incarnation C = pod("c", "az-c");

    private static Roster.Incarnation pod(String name, String az) {
        return new Roster.Incarnation(name, "uid-" + name, az, "");
    }

    private static Roster.Member rostered(Roster.Incarnation pod) {
        return new Roster.Member(pod, Roster.State.ROSTERED);
    }

    private static Roster.Member departed(Roster.Incarnation pod) {
        return new Roster.Member(pod, Roster.State.DEPARTED);
    }

    /** Term 7 led by {@code L}, its term record holding {@code values} for I in turn. */
    private static Roster term(List<Roster.Member> members, int... values) {
        List<Roster.TermRecord> record = new java.util.ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            record.add(new Roster.TermRecord(i, new TreeMap<>(Map.of(I, values[i]))));
        }
        return new Roster(7, 6, L, members, record, List.of(), 0, 0, false);
    }

    private static List<Roster.Member> all() {
        return List.of(rostered(L), rostered(A2), rostered(B), rostered(C));
    }

    @Test
    void atQ1TheLeadersLossAloneIsQuorumLoss() {
        assertThat(QuorumLoss.lostIn(term(all(), 1), I, Set.of("uid-l"))).isTrue();
    }

    @Test
    void atQ2TheLeaderAndAMemberInAnotherZoneMustBothBeGone() {
        Roster t = term(all(), 2);

        assertThat(QuorumLoss.lostIn(t, I, Set.of("uid-l"))).as("the leader alone").isFalse();
        assertThat(QuorumLoss.lostIn(t, I, Set.of("uid-l", "uid-a2")))
                .as("a member in the leader's own zone").isFalse();
        assertThat(QuorumLoss.lostIn(t, I, Set.of("uid-l", "uid-b"))).isTrue();
    }

    @Test
    void atQ3GoneMembersMustLieInTwoOtherZones() {
        Roster t = term(all(), 3);

        assertThat(QuorumLoss.lostIn(t, I, Set.of("uid-l", "uid-b"))).isFalse();
        assertThat(QuorumLoss.lostIn(t, I, Set.of("uid-l", "uid-b", "uid-c"))).isTrue();
    }

    @Test
    void aLIVELeaderMeansNoLossWhoeverElseIsGone() {
        assertThat(QuorumLoss.lostIn(term(all(), 2), I, Set.of("uid-b", "uid-c"))).isFalse();
    }

    @Test
    void aDEPARTEDLeaderOrMemberIsNoLoss() {
        // a departure waits for its copies to commit (§9)
        Roster leaderLeft = term(List.of(departed(L), rostered(B)), 1);
        assertThat(QuorumLoss.lostIn(leaderLeft, I, Set.of("uid-l"))).isFalse();

        Roster memberLeft = term(List.of(rostered(L), departed(B), rostered(C)), 2);
        assertThat(QuorumLoss.lostIn(memberLeft, I, Set.of("uid-l", "uid-b"))).isFalse();
    }

    @Test
    void theQUORUMIsTheLeastValueTheTermRecorded() {
        // recorded at 3, then lowered to 2: q_min is 2
        assertThat(QuorumLoss.lostIn(term(all(), 3, 2), I, Set.of("uid-l", "uid-b"))).isTrue();
    }

    @Test
    void anINDEXTheTermNeverRecordedLostNothingInIt() {
        assertThat(QuorumLoss.lostIn(term(all(), 1), OTHER, Set.of("uid-l"))).isFalse();
    }

    @Test
    void anINDEXHasLostQuorumIfItHasInSomeUnclosedTerm() {
        Roster healthy = new Roster(8, 7, B, List.of(rostered(B), rostered(C)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(I, 2)))), List.of(), 0, 0,
                false);

        assertThat(QuorumLoss.lost(List.of(healthy, term(all(), 1)), I, Set.of("uid-l"))).isTrue();
        assertThat(QuorumLoss.lost(List.of(healthy), I, Set.of("uid-l"))).isFalse();
    }

    @Test
    void anEARLYDecisionNeedsTheUnansweredInFewerZonesThanTheQuorum() {
        Roster t = term(all(), 2);

        assertThat(QuorumLoss.mayDecideEarly(List.of(t), I, Set.of("uid-l", "uid-a2")))
                .as("one zone unanswered at q = 2").isTrue();
        assertThat(QuorumLoss.mayDecideEarly(List.of(t), I, Set.of("uid-l", "uid-b")))
                .as("two zones").isFalse();
        assertThat(QuorumLoss.mayDecideEarly(List.of(t), OTHER, Set.of("uid-l", "uid-b")))
                .as("no entry of an unrecorded index").isTrue();
    }

    @Test
    void aDEPARTEDMemberIsInNoCoverageCount() {
        Roster t = term(List.of(rostered(L), departed(B), rostered(C)), 2);

        assertThat(QuorumLoss.mayDecideEarly(List.of(t), I, Set.of("uid-l", "uid-b"))).isTrue();
    }
}
