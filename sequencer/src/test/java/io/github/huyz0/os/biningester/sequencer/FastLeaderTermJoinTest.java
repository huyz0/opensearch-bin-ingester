// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

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
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader answers a JOIN (ADR-0081 §1, §5 step 7; ADR-0082 §2; M13.27j):
 * JOINED with {@code closedThrough} and the status of every group the pod
 * reported, once the roster lists it; REFUSED when it is not this term's,
 * when the pod departed, or when the term is deposed.
 */
class FastLeaderTermJoinTest {

    private static final RunKey STREAM = new RunKey(new UUID(1, 1), 0);
    private static final Roster.Incarnation POD = FastTermStartTest.incarnation("a");

    /** Term 3, its predecessors 1 and 2 empty and closed at its start. */
    private static FastLeaderTerm term3(MemoryBinStore store, Map<RunKey, Long> committed)
            throws Exception {
        put(store, roster(1, -1, "x", false, 0, 0, false));
        put(store, roster(2, 1, "y", false, 0, 0, false));
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

    private static FastFrame.Header header(long epoch) {
        return new FastFrame.Header(FastFrame.KIND_JOIN, epoch, POD.podUid(), "uid-self");
    }

    @Test
    void aJOINOfThisTermIsAnsweredJoinedWithClosedThroughAndEachGroupsStatus()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of(STREAM, 20L));
        FastFrame.Held held = new FastFrame.Held(List.of(new FastFrame.HeldStream(STREAM,
                List.of(new FastFrame.HeldGroup(1, 0, 4, 6),
                        new FastFrame.HeldGroup(3, 0, 10, 12),
                        new FastFrame.HeldGroup(3, 0, 25, 26)))));

        FastFrame.Body answer = term.answerJoin(header(3), new FastFrame.Join(POD, held));

        FastFrame.Joined joined = (FastFrame.Joined) answer;
        assertThat(joined.closedThrough()).isEqualTo(2);
        assertThat(joined.status()).isEqualTo(HeldStatusAnswers.answer(held,
                Map.of(STREAM, 20L), List.of(read(store, 3)), 2));
        assertThat(joined.status().streams().get(0).groups()).extracting(g -> g.status())
                .containsExactly(FastFrame.Status.CLOSED_TERM, FastFrame.Status.COMMITTED,
                        FastFrame.Status.PENDING);
        assertThat(read(store, 3).member(POD.podUid())).as("rostered before answered")
                .isPresent();
    }

    @Test
    void aJOINOfAnotherTermIsRefusedNotRostered() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore(), Map.of());

        FastFrame.Body answer = term.answerJoin(header(4),
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }

    @Test
    void aDEPARTEDPodIsRefusedDeparting() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        Roster r = read(store, 3);
        put(store, new Roster(3, r.predecessor(), r.leader(),
                List.of(r.members().get(0), new Roster.Member(POD, Roster.State.DEPARTED)),
                r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));

        FastFrame.Body answer = term.answerJoin(header(3),
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.DEPARTING);
    }

    @Test
    void aDEPOSEDTermIsRefusedLowerEpoch() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        put(store, read(store, 3).fencedBy(5));

        FastFrame.Body answer = term.answerJoin(header(3),
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }
}
