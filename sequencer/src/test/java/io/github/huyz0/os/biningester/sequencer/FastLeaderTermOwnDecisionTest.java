// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A decision the term's OWN roster records -- a takeover's (ADR-0081 §5 step
 * 6) -- supersedes a reported group of an earlier term, judged on the roster
 * as the JOIN's write left it (M13.27j review round 3, T1; M13.27o).
 */
class FastLeaderTermOwnDecisionTest {

    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final Roster.Incarnation POD = FastTermStartTest.incarnation("a");

    @Test
    void aGROUPTheTermsOwnDecisionSupersedesIsAnsweredSuperseded() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        Roster own = read(store, 3);
        put(store, new Roster(3, own.predecessor(), own.leader(), own.members(),
                own.termRecord(), List.of(new Roster.Decision(0, STREAM, 5)), own.notBefore(),
                own.fencedBy(), own.closed()));
        FastLeaderTerm term = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }), key -> 0L);
        FastFrame.Held held = new FastFrame.Held(List.of(new FastFrame.HeldStream(STREAM,
                List.of(new FastFrame.HeldGroup(2, 0, 6, 8)))));

        FastFrame.Joined joined = (FastFrame.Joined) term.answerJoin(
                new FastFrame.Header(FastFrame.KIND_JOIN, 3, POD.podUid(), "uid-self"),
                new FastFrame.Join(POD, held));

        assertThat(joined.status().streams().get(0).groups().get(0).status())
                .as("term 3's takeover resumed the stream at 5, below the group's 6")
                .isEqualTo(FastFrame.Status.SUPERSEDED);
    }
}
