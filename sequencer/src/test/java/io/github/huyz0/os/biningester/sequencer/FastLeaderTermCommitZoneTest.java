// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A writer in the leader's own zone holds no required copy (M13.27s review
 * round 2, T5): the zone the term passes is the writer's, not any other --
 * outside the leader's zone every wrong zone also reads "required".
 */
class FastLeaderTermCommitZoneTest {

    /** In az-a, the fixture leader's zone; the term's own pod is in az-self. */
    private static final Roster.Incarnation A = FastTermStartTest.incarnation("a");

    @Test
    void aWRITERInTheLeadersZoneHoldsNoRequiredCopy() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        Mono mono = new Mono();
        FastLeaderTerm term = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, mono, Duration.ofMillis(250)),
                        nanos -> mono.advance(Duration.ofNanos(nanos))),
                key -> 0L, store, "p", mono, Duration.ofMillis(250));
        term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3, A.podUid(), "uid-self"),
                new FastFrame.Join(A, FastFrame.Held.NONE));
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        // q = 2 with az-self and az-a available: assigned, and A's copy is the
        // leader zone's own, never required of A.
        FastFrame.Body answer = term.answerCommit(
                new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3, A.podUid(), "uid-self"),
                commit(1, S2), new CommitDesk(l.leader(), stream -> 0L, Duration.ofSeconds(10)));

        assertThat(answer).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(S2, 0, 2, false))));
    }
}
