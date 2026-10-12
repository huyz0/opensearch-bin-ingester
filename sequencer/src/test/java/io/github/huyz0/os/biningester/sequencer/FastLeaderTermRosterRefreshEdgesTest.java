// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The refresh's edges (M13.33m review round 1, P1, T1-T2): a re-read that
 * fails after the term's own write is reported, never swallowed into a stale
 * next decision number; and a newer term's fence written right after a join
 * is seen by the next answer.
 */
class FastLeaderTermRosterRefreshEdgesTest {

    /** Term 3 over {@code store}, its roster set up in {@code inner}. */
    private static FastLeaderTerm term3(MemoryBinStore inner, BinStore store) throws Exception {
        FastTermStartTest.put(inner, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.put(inner, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(inner, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(inner, "p"),
                new EmptyTermCloser(inner, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(inner, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, store, "p", new Mono(), Duration.ofMillis(250));
    }

    private static void read(FastLeaderTerm term) throws Exception {
        term.answerHeld(new FastFrame.Header(FastFrame.KIND_HELD, 3,
                FastLeaderTermCommitTest.POD.podUid(), "uid-self"),
                new FastFrame.HeldReport(FastFrame.Held.NONE));
    }

    @Test
    void aRE_READThatFailsAfterAWriteIsReportedNotSwallowed() throws Exception {
        MemoryBinStore inner = new MemoryBinStore();
        boolean[] failing = {false};
        BinStore store = new ForwardingBinStore(inner) {
            @Override
            public InputStream get(String key) throws IOException {
                if (failing[0] && key.equals(Roster.key("p", 3))) {
                    throw new IOException("the roster cannot be read");
                }
                return inner.get(key);
            }
        };
        FastLeaderTerm term = term3(inner, store);
        read(term);
        new RosterDecisions(inner, "p", 3).record(Map.of(S1, 5L));
        failing[0] = true;

        assertThatThrownBy(term::rosterWritten)
                .as("the next decision number is not known past the write")
                .isInstanceOf(IOException.class);
    }

    @Test
    void aNEWERFenceWrittenRightAfterAJoinIsSeenByTheNextAnswer() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        read(term);
        FastLeaderTermCommitTest.join(term);
        FastTermStartTest.put(store, FastTermStartTest.read(store, 3).fencedBy(5));

        FastFrame.Body answer = term.answerCommit(
                FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit(FastLeaderTermCommitTest.POD.podId(), 1, S1),
                FastLeaderTermCommitTest.recorded(FastWriteLeaderTest.leader(1 << 20, 1_000)));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }
}
