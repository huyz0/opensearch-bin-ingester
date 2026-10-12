// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
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
 * A term that starts with unclosed terms has its desk held from the moment
 * it is built (M13.33o; ADR-0081 §5): it serves no COMMIT before its
 * takeover has heard every member, so the hold is never later than the first
 * frame.
 */
class FastLeaderTermTakeoverDeskTest {

    /** Term 3 over term 2, which recorded a wal_quorum and so stays open when {@code open}. */
    static FastLeaderTerm term3(MemoryBinStore store, boolean open) throws Exception {
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        Roster.Incarnation y = FastTermStartTest.incarnation("y");
        FastTermStartTest.put(store, new Roster(2, 1, y,
                List.of(new Roster.Member(y, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, open
                        ? new TreeMap<>(Map.of(FastWriteLeaderTest.Q1, 1)) : new TreeMap<>())),
                List.of(), 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        assertThat(opened.stillOpen()).hasSize(open ? 1 : 0);
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, store, "p", new Mono(), Duration.ofMillis(250));
    }

    private static String refusal(FastLeaderTerm term) throws Exception {
        join(term);
        CommitDesk desk = term.commitDesk(FastJournal.recover(new MemoryJournalFile(), 1 << 20),
                FastTermStartTest.SELF);
        FastFrame.Body answer = term.answerCommit(header(3, POD),
                commit(POD.podId(), 1, new RunKey(FastWriteLeaderTest.Q1, 0)), desk);
        return ((FastFrame.Refused) answer).text();
    }

    @Test
    void aTERMWithAnUnclosedPredecessorHoldsEveryCommitForItsTakeover() throws Exception {
        assertThat(refusal(term3(new MemoryBinStore(), true))).contains("takeover");
    }

    @Test
    void aTERMWithNothingUnclosedIsNotTakingOver() throws Exception {
        assertThat(refusal(term3(new MemoryBinStore(), false))).doesNotContain("takeover");
    }

    @Test
    void theDESKsHoldIsItsLeader() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore(), true);
        join(term);
        CommitDesk desk = term.commitDesk(FastJournal.recover(new MemoryJournalFile(), 1 << 20),
                FastTermStartTest.SELF);

        desk.hold().everyMemberHeard();

        FastFrame.Body answer = term.answerCommit(header(3, POD),
                commit(POD.podId(), 1, new RunKey(FastWriteLeaderTest.Q1, 0)), desk);
        assertThat(((FastFrame.Refused) answer).text()).doesNotContain("takeover");
    }
}
