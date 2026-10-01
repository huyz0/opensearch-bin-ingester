// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The leader's {@code DEPARTED} writes (ADR-0081 §9; M13.26f): one per
 * unclosed roster listing the pod, this term's included.
 */
class RosterDeparturesTest {

    private static final Roster.Incarnation POD = FastTermStartTest.incarnation("pod");

    private static Roster with(long epoch, long predecessor, long fencedBy,
            Roster.State podState) {
        Roster base = FastTermStartTest.roster(epoch, predecessor, "l" + epoch, false, 0, 0,
                false);
        List<Roster.Member> members = new ArrayList<>(base.members());
        if (podState != null) {
            members.add(new Roster.Member(POD, podState));
        }
        return new Roster(epoch, predecessor, base.leader(), members, base.termRecord(),
                List.of(), 0, fencedBy, false);
    }

    @Test
    void thePODIsMarkedDepartedInEveryRosterListingIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, with(3, -1, 7, Roster.State.ROSTERED));
        FastTermStartTest.put(store, with(5, 3, 7, null));
        FastTermStartTest.put(store, with(7, 5, 0, Roster.State.ROSTERED));

        RosterDepartures.Outcome outcome = new RosterDepartures(store, "p", 7)
                .depart("uid-pod", List.of(5L, 3L));

        assertThat(outcome).isEqualTo(new RosterDepartures.Departed(List.of(7L, 3L)));
        assertThat(FastTermStartTest.read(store, 7).member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
        assertThat(FastTermStartTest.read(store, 3).member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
        assertThat(FastTermStartTest.read(store, 5).member("uid-pod"))
                .as("a roster that never listed it is not written").isEmpty();
    }

    @Test
    void anALREADYDepartedPodCostsNoWrite() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, with(7, -1, 0, Roster.State.DEPARTED));
        CountingBinStore store = new CountingBinStore(backing);

        RosterDepartures.Outcome outcome = new RosterDepartures(store, "p", 7)
                .depart("uid-pod", List.of());

        assertThat(outcome).isEqualTo(new RosterDepartures.Departed(List.of()));
        assertThat(store.counts().puts()).isZero();
    }

    @Test
    void aROSTERFencedByANewerTermDeposesBeforeAnyLaterWrite() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, with(5, -1, 9, Roster.State.ROSTERED));
        FastTermStartTest.put(store, with(7, 5, 9, Roster.State.ROSTERED));

        RosterDepartures.Outcome outcome = new RosterDepartures(store, "p", 7)
                .depart("uid-pod", List.of(5L));

        assertThat(outcome).isEqualTo(new RosterDepartures.Deposed(9));
        assertThat(FastTermStartTest.read(store, 7).member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.ROSTERED);
        assertThat(FastTermStartTest.read(store, 5).member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.ROSTERED);
    }

    @Test
    void aWRITERefusedIsReReadAndKeepsWhatLandedFirst() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, with(7, -1, 0, Roster.State.ROSTERED));
        Roster.Incarnation joiner = FastTermStartTest.incarnation("joiner");
        HookedStore store = new HookedStore(backing, Roster.key("p", 7), b -> {
            Roster r = FastTermStartTest.read(b, 7);
            List<Roster.Member> members = new ArrayList<>(r.members());
            members.add(new Roster.Member(joiner, Roster.State.ROSTERED));
            FastTermStartTest.put(b, new Roster(7, r.predecessor(), r.leader(), members,
                    r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));
        });

        new RosterDepartures(store, "p", 7).depart("uid-pod", List.of());

        Roster stored = FastTermStartTest.read(backing, 7);
        assertThat(stored.member("uid-joiner")).isPresent();
        assertThat(stored.member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
    }
}
