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
 * The early decision holds in EVERY unclosed term (M13.33e review round 1,
 * T1): a later term covered never excuses an earlier one uncovered, and a
 * term that recorded nothing of the index is skipped, not decisive.
 */
class QuorumLossTermsTest {

    private static final UUID I = new UUID(1, 1);
    private static final Roster.Incarnation L = pod("l", "az-a");
    private static final Roster.Incarnation B = pod("b", "az-b");
    private static final Roster.Incarnation C = pod("c", "az-c");

    private static Roster.Incarnation pod(String name, String az) {
        return new Roster.Incarnation(name, "uid-" + name, az, "");
    }

    private static Roster term(long epoch, boolean records) {
        return new Roster(epoch, epoch - 1, L, List.of(
                new Roster.Member(L, Roster.State.ROSTERED),
                new Roster.Member(B, Roster.State.ROSTERED),
                new Roster.Member(C, Roster.State.ROSTERED)),
                records ? List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(I, 2))))
                        : List.of(),
                List.of(), 0, 0, false);
    }

    @Test
    void aCOVEREDTermNeverExcusesAnUncoveredOne() {
        // in term 8 alone only b is unanswered (one zone at q = 2: covered);
        // term 7 also lists c, unanswered too: two zones, uncovered
        Roster eight = new Roster(8, 7, L, List.of(new Roster.Member(L, Roster.State.ROSTERED),
                new Roster.Member(B, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(I, 2)))), List.of(), 0, 0,
                false);

        assertThat(QuorumLoss.mayDecideEarly(List.of(eight, term(7, true)), I,
                Set.of("uid-b", "uid-c"))).isFalse();
    }

    @Test
    void aTERMThatRecordedNothingIsSkippedNotDecisive() {
        assertThat(QuorumLoss.mayDecideEarly(List.of(term(8, false), term(7, true)), I,
                Set.of("uid-b", "uid-c"))).as("term 7 is uncovered").isFalse();
    }
}
