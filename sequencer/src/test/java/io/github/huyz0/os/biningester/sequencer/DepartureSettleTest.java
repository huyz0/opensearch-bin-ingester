// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Split groups, other streams, the assignedAfter boundary, the committed
 * prefix, durability, and a failed first DEPART (M13.26f review round 1,
 * P1, P2, T1-T10).
 */
class DepartureSettleTest {

    private static final RunKey A = HeldReportsTest.A;
    private static final RunKey B = HeldReportsTest.B;
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    private static Roster decided(long epoch, Roster.Decision... decisions) {
        Roster base = FastTermStartTest.roster(epoch, -1, "l", false, 0, 0, false);
        return new Roster(epoch, -1, base.leader(), base.members(), base.termRecord(),
                List.of(decisions), 0, 0, false);
    }

    private static FastFrame.GroupStatus answer(long epoch, long assignedAfter, long lowest,
            long highest, long next, Roster... rosters) {
        return HeldStatusAnswers.answer(new FastFrame.Held(List.of(new FastFrame.HeldStream(A,
                List.of(new FastFrame.HeldGroup(epoch, assignedAfter, lowest, highest))))),
                Map.of(A, next), List.of(rosters), 0).streams().get(0).groups().get(0);
    }

    @Test
    void aSPLITGroupWhoseLowerPartIsCommittedIsCommittedNotPending() {
        Roster takeover = decided(8, new Roster.Decision(0, A, 15));

        FastFrame.GroupStatus committed = answer(7, 0, 10, 20, 15, takeover);
        FastFrame.GroupStatus pending = answer(7, 0, 10, 20, 12, takeover);

        assertThat(committed).isEqualTo(new FastFrame.GroupStatus(7, 0, 15,
                FastFrame.Status.COMMITTED));
        assertThat(pending.status()).as("offsets 12-14 are neither committed nor superseded")
                .isEqualTo(FastFrame.Status.PENDING);
    }

    @Test
    void anOTHERStreamsDecisionNeverSupersedes() {
        FastFrame.GroupStatus status = answer(7, 0, 10, 20, 0,
                decided(8, new Roster.Decision(0, B, 0)));

        assertThat(status).isEqualTo(new FastFrame.GroupStatus(7, 0, Long.MAX_VALUE,
                FastFrame.Status.PENDING));
    }

    @Test
    void aDECISIONNumberedExactlyTheGroupsAssignedAfterSupersedesIt() {
        assertThat(answer(7, 3, 10, 20, 0, decided(7, new Roster.Decision(3, A, 12)))
                .supersededFrom()).isEqualTo(12);
    }

    @Test
    void aPENDINGGroupsCommittedPrefixIsReleasedAndTheResultDurable() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, 1 << 20);
        journal.append(HeldReportsTest.entry(A, 7, 0, 0, 2));
        journal.append(HeldReportsTest.entry(A, 7, 0, 2, 2));
        journal.sync();

        boolean pending = HeldReports.apply(journal, new FastFrame.HeldStatus(List.of(
                new FastFrame.StreamStatus(A, 2, List.of(new FastFrame.GroupStatus(7, 0,
                        Long.MAX_VALUE, FastFrame.Status.PENDING))))));
        file.crash();

        assertThat(pending).isTrue();
        assertThat(FastJournal.recover(file, 1 << 20).held())
                .extracting(e -> e.firstOffset())
                .as("the release below 2 survived the crash; the pending entry is kept")
                .containsExactly(2L);
    }

    @Test
    void aRETRYAfterAFailedFirstDepartAsksForTheUploadAgain() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(A, 7, 0, 0, 2));
        List<FastFrame.Body> sent = new ArrayList<>();
        int[] calls = {0};
        Departure departure = new Departure(POD, (endpoint, frame) -> {
            sent.add(FastFrame.decode(frame).body());
            if (calls[0]++ == 0) {
                throw new IOException("connection reset");
            }
            return FastFrame.encode(7, "uid-l", "uid-2",
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        }, fenceAt(7), journal);
        assertThatThrownBy(() -> departure.report(7, "uid-l", "e"))
                .isInstanceOf(IOException.class);
        assertThat(departure.accepting()).as("stopped accepting at once").isFalse();

        departure.report(7, "uid-l", "e");
        departure.report(7, "uid-l", "e");

        assertThat(sent.get(1)).isInstanceOf(FastFrame.Depart.class);
        assertThat(sent.get(2)).as("after an answered phase 1, a HELD carrying the journal")
                .isEqualTo(new FastFrame.HeldReport(HeldReports.of(journal.held())));
    }

    @Test
    void anANSWERFromAnotherPodOrATermBelowTheFenceIsRefused() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(A, 7, 0, 0, 2));
        Departure impostor = new Departure(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-x",
                "uid-2", new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE)), fenceAt(7),
                journal);
        Departure stale = new Departure(POD, (endpoint, frame) -> {
            throw new AssertionError("nothing is sent to a deposed leader");
        }, fenceAt(7), journal);

        assertThatThrownBy(() -> impostor.leave(7, "uid-l", "e")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> stale.report(6, "uid-l", "e"))
                .isInstanceOf(TermJoiner.Fenced.class);
        assertThatThrownBy(() -> new RosterDepartures(new MemoryBinStore(), "p", 7)
                .depart("uid-2", List.of(7L))).isInstanceOf(IllegalArgumentException.class);
    }
}
