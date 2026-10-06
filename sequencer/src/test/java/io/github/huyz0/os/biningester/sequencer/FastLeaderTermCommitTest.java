// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A COMMIT reaches the term it names (M13.27s): answered by the term's desk
 * for a writer its roster lists, with the writer's zone; refused otherwise.
 */
class FastLeaderTermCommitTest {

    private static final Roster.Incarnation POD = FastTermStartTest.incarnation("a");

    private static FastLeaderTerm term3(MemoryBinStore store) throws Exception {
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, store, "p", new Mono(), Duration.ofMillis(250));
    }

    private static FastFrame.Header header(long epoch, Roster.Incarnation from) {
        return new FastFrame.Header(FastWriteFrame.KIND_COMMIT, epoch, from.podUid(),
                "uid-self");
    }

    private static void join(FastLeaderTerm term) throws Exception {
        term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3, POD.podUid(), "uid-self"),
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    /** A desk whose term record holds S1's index at {@code wal_quorum = 1}. */
    private static CommitDesk recorded(FastWriteLeaderTest.Leader l) {
        return new CommitDesk(l.leader(), stream -> 0L, Duration.ofSeconds(10));
    }

    @Test
    void aROSTEREDWritersCOMMITIsAnsweredByTheDesk() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(header(3, POD), commit(1, S1), recorded(l));

        assertThat(answer).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(S1, 100, 1, false))));
        assertThat(l.journal().held()).hasSize(1);
    }

    @Test
    void aCOMMITFromAPodTheRosterDoesNotListIsRefusedNotRostered() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(header(3, POD), commit(1, S1), recorded(l));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(l.journal().held()).as("nothing assigned").isEmpty();
    }

    @Test
    void aCOMMITOfAnotherTermIsRefusedNotRostered() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(header(4, POD), commit(1, S1), recorded(l));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void theTermsOwnDeskIsBuiltOnceAndHoldsWhatItsTermRecordDoesNotRecord()
            throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);

        CommitDesk desk = term.commitDesk(journal, FastTermStartTest.SELF);

        assertThat(term.commitDesk(journal, FastTermStartTest.SELF)).isSameAs(desk);
        // ⚠️ AN EMPTY TERM RECORD ASSIGNS NOTHING until M13.27l feeds it from
        // the catalog: every COMMIT is held, as backpressure.
        FastFrame.Body answer = term.answerCommit(header(3, POD),
                commit(1, new RunKey(FastWriteLeaderTest.Q1, 0)), desk);
        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.BACKPRESSURE);
        assertThat(journal.held()).isEmpty();
    }
}
