// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The roster's invariants, refused at construction (ADR-0081 §1, ADR-0082 §3).
 */
class RosterValidationTest {

    private static final Roster.Incarnation L =
            new Roster.Incarnation("ingester-1", "uid-1", "az-a", "");
    private static final Roster.Incarnation M =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "");
    private static final List<Roster.TermRecord> RECORD =
            List.of(new Roster.TermRecord(0, new TreeMap<>()));

    private static Roster roster(List<Roster.Member> members, List<Roster.TermRecord> record,
            List<Roster.Decision> decisions, long predecessor, long fencedBy) {
        return new Roster(7, predecessor, L, members, record, decisions, 0, fencedBy, false);
    }

    private static List<Roster.Member> leaderOnly() {
        return List.of(new Roster.Member(L, Roster.State.ROSTERED));
    }

    @Test
    void theLEADERMustBeAMember() {
        assertThatThrownBy(() -> roster(List.of(new Roster.Member(M, Roster.State.ROSTERED)),
                RECORD, List.of(), -1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anINCARNATIONIsListedOnce() {
        assertThatThrownBy(() -> roster(List.of(new Roster.Member(L, Roster.State.ROSTERED),
                new Roster.Member(L, Roster.State.DEPARTED)), RECORD, List.of(), -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPREDECESSORIsAnEarlierTerm() {
        assertThatThrownBy(() -> roster(leaderOnly(), RECORD, List.of(), 7, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> roster(leaderOnly(), RECORD, List.of(), 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void onlyALATERTermFences() {
        assertThatThrownBy(() -> roster(leaderOnly(), RECORD, List.of(), -1, 7))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theTERMRecordIsNumberedWithoutGaps() {
        assertThatThrownBy(() -> roster(leaderOnly(),
                List.of(new Roster.TermRecord(0, new TreeMap<>()),
                        new Roster.TermRecord(2, new TreeMap<>())), List.of(), -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aWALQuorumIsOneToThree() {
        assertThatThrownBy(() -> new Roster.TermRecord(0,
                new TreeMap<>(Map.of(RosterTest.I1, 4)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Roster.TermRecord(0,
                new TreeMap<>(Map.of(RosterTest.I1, 0)))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void DECISIONSAscendByNumber() {
        assertThatThrownBy(() -> roster(leaderOnly(), RECORD,
                List.of(new Roster.Decision(3, new RunKey(RosterTest.I1, 0), 10),
                        new Roster.Decision(3, new RunKey(RosterTest.I1, 1), 10)), -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anINCARNATIONNamesItsPodUidAndAz() {
        assertThatThrownBy(() -> new Roster.Incarnation("p", "", "az", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Roster.Incarnation("p", "u\"", "az", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
