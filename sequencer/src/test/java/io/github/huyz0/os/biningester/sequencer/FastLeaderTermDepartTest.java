// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The leader answers a departing pod (ADR-0081 §9; ADR-0082 §2; M13.27k):
 * DEPART phase 1 and every HELD with each reported group's status, judged on
 * its roster as it is NOW; DEPART phase 2 by marking the pod departed in its
 * roster and every earlier open one listing it, answered by an empty
 * HELD_STATUS once those writes landed.
 */
class FastLeaderTermDepartTest {

    static final RunKey STREAM = new RunKey(INDEX, 0);
    static final Roster.Incarnation POD = FastTermStartTest.incarnation("a");

    /** Term 3 over an open term 2 (it recorded a fast index) that lists the pod. */
    static FastLeaderTerm term3(MemoryBinStore store, Map<RunKey, Long> committed)
            throws Exception {
        put(store, roster(1, -1, "x", false, 0, 0, true));
        Roster two = roster(2, 1, "y", false, 0, 0, false);
        TreeMap<java.util.UUID, Integer> q = new TreeMap<>();
        q.put(INDEX, 2);
        put(store, new Roster(2, 1, two.leader(),
                List.of(two.members().get(0), new Roster.Member(POD, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, false));
        latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> committed.getOrDefault(key, 0L), store, "p");
    }

    static FastFrame.Header header(int kind, long epoch) {
        return new FastFrame.Header(kind, epoch, POD.podUid(), "uid-self");
    }

    private static FastFrame.Held held(long epoch, long lowest, long highest) {
        return new FastFrame.Held(List.of(new FastFrame.HeldStream(STREAM,
                List.of(new FastFrame.HeldGroup(epoch, 0, lowest, highest)))));
    }

    @Test
    void aPHASE1DepartIsAnsweredWithEachGroupsStatus() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of(STREAM, 10L));

        FastFrame.Body answer = term.answerDepart(header(FastFrame.KIND_DEPART, 3),
                new FastFrame.Depart(POD, 1, held(3, 0, 4)));

        FastFrame.HeldStatus status = ((FastFrame.HeldStatusReport) answer).status();
        assertThat(status.streams().get(0).releaseBelow()).isEqualTo(10);
        assertThat(status.streams().get(0).groups().get(0).status())
                .isEqualTo(FastFrame.Status.COMMITTED);
        assertThat(read(store, 3).member(POD.podUid())).as("phase 1 marks nothing").isEmpty();
    }

    @Test
    void aHELDIsJudgedOnTheRosterAsItIsNow() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        Roster own = read(store, 3);
        put(store, new Roster(3, own.predecessor(), own.leader(), own.members(),
                own.termRecord(), List.of(new Roster.Decision(0, STREAM, 5)), own.notBefore(),
                own.fencedBy(), own.closed()));

        FastFrame.Body answer = term.answerHeld(header(FastFrame.KIND_HELD, 3),
                new FastFrame.HeldReport(held(2, 6, 8)));

        assertThat(((FastFrame.HeldStatusReport) answer).status().streams().get(0).groups()
                .get(0).status()).as("a decision written after the term started")
                .isEqualTo(FastFrame.Status.SUPERSEDED);
    }

    @Test
    void aPHASE2DepartMarksThePodDepartedHereAndInEveryOpenEarlierRoster() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        term.answerJoin(header(FastFrame.KIND_JOIN, 3),
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        FastFrame.Body answer = term.answerDepart(header(FastFrame.KIND_DEPART, 3),
                new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));

        assertThat(answer).as("every DEPARTED write landed")
                .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(read(store, 3).member(POD.podUid()).orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
        assertThat(read(store, 2).member(POD.podUid()).orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
    }

    @Test
    void aDEPOSEDTermRefusesADepartureLowerEpoch() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        put(store, read(store, 3).fencedBy(5));

        FastFrame.Body answer = term.answerDepart(header(FastFrame.KIND_DEPART, 3),
                new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }

    @Test
    void aDEPARTUREOrHeldOfAnotherTermIsRefusedNotRostered() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore(), Map.of());

        assertThat(((FastFrame.Refused) term.answerDepart(header(FastFrame.KIND_DEPART, 2),
                new FastFrame.Depart(POD, 2, FastFrame.Held.NONE))).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(((FastFrame.Refused) term.answerHeld(header(FastFrame.KIND_HELD, 4),
                new FastFrame.HeldReport(FastFrame.Held.NONE))).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }
}
