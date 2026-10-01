// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A pod still holding an entry never departs, a closed term above the
 * committed offset is dropped, and a DEPARTED write keeps the rest of the
 * roster (M13.26f review round 3, P6, T13-T15; carried by M13.26h).
 */
class DepartureHoldsTest {

    private static final RunKey A = HeldReportsTest.A;
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    @Test
    void anENTRYAppendedDuringTheReportKeepsThePodFromLeaving() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(A, 7, 0, 0, 2));
        List<FastFrame.Body> sent = new ArrayList<>();
        Departure departure = new Departure(POD, (endpoint, frame) -> {
            FastFrame.Body body = FastFrame.decode(frame).body();
            sent.add(body);
            if (body instanceof FastFrame.Depart d && d.phase() == 2) {
                throw new AssertionError("phase 2 is never sent while anything is held");
            }
            try {
                journal.append(HeldReportsTest.entry(A, 7, 0, 2, 2));
                journal.sync();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return FastFrame.encode(7, "uid-l", "uid-2", new FastFrame.HeldStatusReport(
                    new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(A, 2, List.of(
                            new FastFrame.GroupStatus(7, 0, Long.MAX_VALUE,
                                    FastFrame.Status.COMMITTED)))))));
        }, fenceAt(7), journal);

        assertThat(departure.report(7, "uid-l", "e"))
                .as("offset 2 was acked after the report and is in no answer").isTrue();
        assertThatThrownBy(() -> departure.leave(7, "uid-l", "e"))
                .isInstanceOf(IOException.class);
        assertThat(journal.held()).extracting(e -> e.firstOffset()).containsExactly(2L);
    }

    @Test
    void aCLOSEDTermAboveTheCommittedOffsetIsDroppedWhole() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(A, 5, 0, 10, 2));

        boolean pending = HeldReports.apply(journal, new FastFrame.HeldStatus(List.of(
                new FastFrame.StreamStatus(A, 0, List.of(new FastFrame.GroupStatus(5, 0,
                        Long.MAX_VALUE, FastFrame.Status.CLOSED_TERM))))));

        assertThat(pending).isFalse();
        assertThat(journal.held()).isEmpty();
    }

    @Test
    void aDEPARTEDWriteKeepsTheRostersDecisionsAndEverythingElse() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster.Incarnation leader = FastTermStartTest.incarnation("l");
        Roster.Incarnation pod = FastTermStartTest.incarnation("pod");
        Roster before = new Roster(7, 5, leader,
                List.of(new Roster.Member(leader, Roster.State.ROSTERED),
                        new Roster.Member(pod, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(java.util.Map.of(
                        new UUID(1, 1), 2)))),
                List.of(new Roster.Decision(0, A, 4242)), 99, 0, false);
        FastTermStartTest.put(store, before);

        new RosterDepartures(store, "p", 7).depart("uid-pod", List.of());

        Roster after = FastTermStartTest.read(store, 7);
        assertThat(after).isEqualTo(new Roster(7, 5, leader,
                List.of(new Roster.Member(leader, Roster.State.ROSTERED),
                        new Roster.Member(pod, Roster.State.DEPARTED)),
                before.termRecord(), before.decisions(), 99, 0, false));
    }
}
