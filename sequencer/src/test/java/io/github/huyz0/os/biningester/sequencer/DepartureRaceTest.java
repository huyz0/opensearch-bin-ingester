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
 * An entry appended between a report and its answer, a leader change during
 * a departure, and a leave with no report first (M13.26f review round 2, P4,
 * P5, T11, T12).
 */
class DepartureRaceTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    @Test
    void anENTRYAppendedAfterTheReportSurvivesItsGroupsCommittedAnswer() throws Exception {
        FastJournal journal = HeldReportsTest.journal(
                HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 0, 2));
        FastFrame.Held reported = HeldReports.of(journal.held());
        journal.append(HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 2, 2));
        journal.sync();

        HeldReports.apply(journal, new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(
                HeldReportsTest.A, 2, List.of(new FastFrame.GroupStatus(7, 0, Long.MAX_VALUE,
                        FastFrame.Status.COMMITTED))))));

        assertThat(reported.streams().get(0).groups().get(0).highest()).isEqualTo(1);
        assertThat(journal.held()).extracting(e -> e.firstOffset())
                .as("acked after the report, uncommitted: kept").containsExactly(2L);
    }

    @Test
    void aNEWLeaderIsAskedForTheUploadAgain() throws Exception {
        FastJournal journal = HeldReportsTest.journal(
                HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 0, 2));
        List<FastFrame.Body> sent = new ArrayList<>();
        EpochFence fence = fenceAt(7);
        Departure departure = new Departure(POD, (endpoint, frame) -> {
            FastFrame.Frame f = FastFrame.decode(frame);
            sent.add(f.body());
            return FastFrame.encode(f.header().epoch(), f.header().targetUid(), "uid-2",
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        }, fence, journal);

        departure.report(7, "uid-l1", "e1");
        departure.report(8, "uid-l2", "e2");

        assertThat(sent.get(1)).as("term 8's leader never heard the ask")
                .isInstanceOf(FastFrame.Depart.class);
    }

    @Test
    void LEAVINGWithoutAReportStillStopsAccepting() throws Exception {
        Departure departure = new Departure(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                "uid-2", new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE)), fenceAt(7),
                HeldReportsTest.journal());

        departure.leave(7, "uid-l", "e");

        assertThat(departure.accepting()).isFalse();
    }
}
