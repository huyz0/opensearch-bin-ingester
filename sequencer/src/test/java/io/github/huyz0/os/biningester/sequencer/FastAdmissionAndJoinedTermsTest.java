// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.Roster;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Admission before assignment (ADR-0081 §2.3) and a pod's joined terms
 * (§1; M13.26d).
 */
class FastAdmissionAndJoinedTermsTest {

    private static final Roster.Incarnation L = new Roster.Incarnation("l", "uid-l", "az-a", "");
    private static final Roster.Incarnation A2 = new Roster.Incarnation("a2", "uid-a2", "az-a", "");
    private static final Roster.Incarnation B = new Roster.Incarnation("b", "uid-b", "az-b", "");
    private static final Roster.Incarnation C = new Roster.Incarnation("c", "uid-c", "az-c", "");

    private static Roster roster(Roster.State cState) {
        return new Roster(7, -1, L, List.of(new Roster.Member(L, Roster.State.ROSTERED),
                new Roster.Member(A2, Roster.State.ROSTERED),
                new Roster.Member(B, Roster.State.ROSTERED),
                new Roster.Member(C, cState)),
                FastTermStartTest.roster(7, -1, "x", false, 0, 0, false).termRecord(),
                List.of(), 0, 0, false);
    }

    @Test
    void ADMISSIONCountsDistinctZonesOfAvailableRosteredPods() {
        Roster r = roster(Roster.State.ROSTERED);

        assertThat(FastAdmission.admits(r, Set.of("uid-l", "uid-a2"), 2))
                .as("two pods, one zone").isFalse();
        assertThat(FastAdmission.admits(r, Set.of("uid-l", "uid-b"), 2)).isTrue();
        assertThat(FastAdmission.admits(r, Set.of("uid-l", "uid-b"), 3)).isFalse();
        assertThat(FastAdmission.admits(r, Set.of("uid-l", "uid-b", "uid-c"), 3)).isTrue();
    }

    @Test
    void aDEPARTEDOrUnrosteredPodIsNotCounted() {
        assertThat(FastAdmission.admits(roster(Roster.State.DEPARTED),
                Set.of("uid-l", "uid-b", "uid-c"), 3)).as("c departed").isFalse();
        assertThat(FastAdmission.admits(roster(Roster.State.ROSTERED),
                Set.of("uid-l", "uid-b", "uid-stranger"), 3)).as("a pod outside the roster")
                .isFalse();
        assertThatThrownBy(() -> FastAdmission.admits(roster(Roster.State.ROSTERED),
                Set.of(), 4)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPODHoldsOnlyTermsItJoinedAndNotYetClosed() {
        JoinedTerms terms = new JoinedTerms();
        assertThat(terms.mayHold(7)).as("never joined").isFalse();

        terms.joined(7, 3);
        assertThat(terms.mayHold(7)).isTrue();
        assertThat(terms.mayHold(8)).isFalse();

        terms.joined(9, 7);
        assertThat(terms.mayHold(7)).as("term 7 closed").isFalse();
        assertThat(terms.mayHold(9)).isTrue();
    }
}
