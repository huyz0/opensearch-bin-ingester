// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A DEPARTED write keeps an earlier roster's fence and closure, an earlier
 * roster fenced by a newer term deposes, and one pod leading the next term is
 * asked again (M13.26h review round 1, T16-T18).
 */
class RosterDeparturesKeepTest {

    private static final Roster.Incarnation POD = FastTermStartTest.incarnation("pod");

    private static Roster listing(long epoch, long predecessor, long fencedBy, boolean closed) {
        Roster.Incarnation leader = FastTermStartTest.incarnation("l" + epoch);
        return new Roster(epoch, predecessor, leader,
                List.of(new Roster.Member(leader, Roster.State.ROSTERED),
                        new Roster.Member(POD, Roster.State.ROSTERED)),
                FastTermStartTest.roster(epoch, -1, "x", false, 0, 0, false).termRecord(),
                List.of(), 0, fencedBy, closed);
    }

    @Test
    void anEARLIERRostersFenceAndClosureSurviveTheDepartedWrite() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, listing(5, -1, 7, true));
        FastTermStartTest.put(store, listing(7, 5, 0, false));

        new RosterDepartures(store, "p", 7).depart("uid-pod", List.of(5L));

        Roster earlier = FastTermStartTest.read(store, 5);
        assertThat(earlier.fencedBy()).as("its old leader must still learn it was deposed")
                .isEqualTo(7);
        assertThat(earlier.closed()).isTrue();
        assertThat(earlier.member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
    }

    @Test
    void anEARLIERRosterFencedByANewerTermDeposes() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, listing(5, -1, 9, false));
        FastTermStartTest.put(store, listing(7, 5, 0, false));

        RosterDepartures.Outcome outcome = new RosterDepartures(store, "p", 7)
                .depart("uid-pod", List.of(5L));

        assertThat(outcome).isEqualTo(new RosterDepartures.Deposed(9));
        assertThat(FastTermStartTest.read(store, 5).member("uid-pod").orElseThrow().state())
                .isEqualTo(Roster.State.ROSTERED);
    }

    @Test
    void theSAMEPodLeadingTheNextTermIsAskedForTheUploadAgain() throws Exception {
        MemoryBinStore leaseStore = new MemoryBinStore();
        leaseStore.put("p/ctl/lease/0.json",
                Body.ofBytes(new Lease(7, "leader", "", 1_000).encode()));
        EpochFence fence = EpochFence.start(leaseStore, "p/ctl/lease/0.json", Optional.empty());
        List<FastFrame.Body> sent = new ArrayList<>();
        Departure departure = new Departure(
                new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2"),
                (endpoint, frame) -> {
                    FastFrame.Frame f = FastFrame.decode(frame);
                    sent.add(f.body());
                    return FastFrame.encode(f.header().epoch(), "uid-l", "uid-2",
                            new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
                }, fence, HeldReportsTest.journal());

        departure.report(7, "uid-l", "e");
        departure.report(8, "uid-l", "e");

        assertThat(sent.get(1)).as("term 8 is a new term, though its leader is the same pod")
                .isInstanceOf(FastFrame.Depart.class);
    }
}
