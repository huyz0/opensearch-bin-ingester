// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A non-leader's graceful departure (ADR-0081 §9; M13.26f): stop accepting,
 * report and drop what the leader settles, report again while anything is
 * pending, then ask to be marked departed.
 */
class DepartureTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    private static byte[] status(long epoch, FastFrame.HeldStatus status) {
        return FastFrame.encode(epoch, "uid-l", "uid-2", new FastFrame.HeldStatusReport(status));
    }

    private static FastFrame.HeldStatus pendingA() {
        return new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(HeldReportsTest.A, 0,
                List.of(new FastFrame.GroupStatus(7, 0, Long.MAX_VALUE,
                        FastFrame.Status.PENDING)))));
    }

    private static FastFrame.HeldStatus committedA() {
        return new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(HeldReportsTest.A, 2,
                List.of(new FastFrame.GroupStatus(7, 0, Long.MAX_VALUE,
                        FastFrame.Status.COMMITTED)))));
    }

    @Test
    void theFIRSTReportIsADepartThenHeldWhileAnythingIsPending() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(HeldReportsTest.A,
                7, 0, 0, 2));
        List<FastFrame.Body> sent = new ArrayList<>();
        List<byte[]> answers = new ArrayList<>(List.of(status(7, pendingA()),
                status(7, committedA())));
        Departure departure = new Departure(POD, (endpoint, frame) -> {
            sent.add(FastFrame.decode(frame).body());
            return answers.remove(0);
        }, fenceAt(7), journal);
        assertThat(departure.accepting()).isTrue();

        assertThat(departure.report(7, "uid-l", "e")).as("the entry is still pending").isTrue();
        assertThat(departure.accepting()).as("stops accepting at the first report").isFalse();
        assertThat(journal.held()).as("a pending copy is never dropped").hasSize(1);
        assertThat(departure.report(7, "uid-l", "e")).isFalse();

        assertThat(sent.get(0)).isEqualTo(new FastFrame.Depart(POD, 1,
                HeldReports.of(List.of(HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 0, 2)))));
        assertThat(sent.get(1)).isInstanceOf(FastFrame.HeldReport.class);
        assertThat(journal.held()).isEmpty();
    }

    @Test
    void LEAVINGAsksForPhaseTwoAndNeedsAnEmptyAnswer() throws Exception {
        FastJournal journal = HeldReportsTest.journal();
        List<FastFrame.Body> sent = new ArrayList<>();
        Departure departure = new Departure(POD, (endpoint, frame) -> {
            sent.add(FastFrame.decode(frame).body());
            return status(7, FastFrame.HeldStatus.NONE);
        }, fenceAt(7), journal);

        departure.leave(7, "uid-l", "e");

        assertThat(sent).containsExactly(new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));
        Departure unsettled = new Departure(POD, (endpoint, frame) -> status(7, pendingA()),
                fenceAt(7), journal);
        assertThatThrownBy(() -> unsettled.leave(7, "uid-l", "e"))
                .as("entries still to settle: not departed").isInstanceOf(IOException.class);
    }

    @Test
    void anANSWERNotFromTheLeaderToThisPodOrBelowTheFenceIsRefused() throws Exception {
        FastJournal journal = HeldReportsTest.journal(HeldReportsTest.entry(HeldReportsTest.A,
                7, 0, 0, 2));
        Departure misaddressed = new Departure(POD, (endpoint, frame) -> FastFrame.encode(7,
                "uid-l", "uid-9", new FastFrame.HeldStatusReport(committedA())), fenceAt(7),
                journal);
        EpochFence fence = fenceAt(7);
        Departure raced = new Departure(POD, (endpoint, frame) -> {
            fence.raise(8);
            return status(7, committedA());
        }, fence, journal);

        assertThatThrownBy(() -> misaddressed.report(7, "uid-l", "e"))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> raced.report(7, "uid-l", "e"))
                .isInstanceOf(TermJoiner.Fenced.class);
        assertThat(journal.held()).as("nothing dropped on an answer refused").hasSize(1);
    }

    @Test
    void aREFUSEDOrOtherTermsAnswerIsAnIOException() throws Exception {
        FastJournal journal = HeldReportsTest.journal();
        Departure refused = new Departure(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                "uid-2", new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                        "no")), fenceAt(7), journal);
        Departure otherTerm = new Departure(POD, (endpoint, frame) -> status(8,
                FastFrame.HeldStatus.NONE), fenceAt(7), journal);

        assertThatThrownBy(() -> refused.leave(7, "uid-l", "e")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> otherTerm.leave(7, "uid-l", "e"))
                .isInstanceOf(IOException.class);
    }
}
