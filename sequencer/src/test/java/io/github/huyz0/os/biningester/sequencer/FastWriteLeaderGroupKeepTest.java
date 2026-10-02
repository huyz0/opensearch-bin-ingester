// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A withdrawn batch never takes an earlier batch of its group with it, and one
 * pod returning re-offers only itself (M13.27e review round 2, T14, T15).
 */
class FastWriteLeaderGroupKeepTest {

    @Test
    void aWITHDRAWNBatchLeavesAnEarlierBatchOfItsGroupHeld() throws Exception {
        FastWriteFrame.Commit earlier = FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1);
        FastWriteFrame.Commit refused = FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S1,
                FastWriteLeaderTest.S2);
        long earlierBytes = new FastJournalRecord.Entry(7, FastWriteLeaderTest.S1, 100, 1, 3,
                earlier.key(), earlier.runs().get(0).records()).encode().length;
        long refusedBytes = new FastJournalRecord.Entry(7, FastWriteLeaderTest.S1, 102, 1, 3,
                refused.key(), refused.runs().get(0).records()).encode().length
                + new FastJournalRecord.Entry(7, FastWriteLeaderTest.S2, 0, 2, 3, refused.key(),
                        refused.runs().get(1).records()).encode().length;
        FastWriteLeaderCommitTest.FillingFile file = new FastWriteLeaderCommitTest.FillingFile();
        FastJournal journal = FastJournal.recover(file, earlierBytes + refusedBytes);
        file.journal = journal;
        FastWriteFrame.Commit other = FastWriteLeaderTest.commit(9, FastWriteLeaderTest.S1);
        file.filler = new FastJournalRecord.Entry(7, FastWriteLeaderTest.S1, 500, 1, 9,
                other.key(), other.runs().get(0).records());
        FastWriteLeader l = new FastWriteLeader(7, FastWriteLeaderTest.LEADER, new FastCursor(1_000),
                new TermRecordWriter(new MemoryBinStore(), "p", FastWriteLeaderTest.roster(),
                        new Mono(), Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), journal, () -> 3);
        l.open(FastWriteLeaderTest.S1, 100);
        l.open(FastWriteLeaderTest.S2, 0);
        l.commit(FastWriteLeaderTest.B, earlier, FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL);
        l.flushGroup();
        assertThat(l.expose()).as("the earlier batch is exposed and acked").hasSize(1);
        file.armed = true;

        assertThatThrownBy(() -> l.commit(FastWriteLeaderTest.B, refused,
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL))
                .isInstanceOf(IllegalStateException.class);

        assertThat(journal.held()).extracting(e -> e.firstOffset())
                .as("same epoch and assignedAfter, yet the exposed entry at 100 is still held")
                .contains(100L)
                .doesNotContain(102L);
    }

    @Test
    void oneDECLINERReturningReoffersOnlyItself() {
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");
        f.open(FastWriteLeaderTest.S1, 0);
        EntryId id = new EntryId(7, 0, FastWriteLeaderTest.S1, 0);
        f.assigned(id, 1, 2);
        Holder b2 = new Holder("uid-b2", "az-b");
        f.asked(id, FastWriteLeaderTest.B);
        f.withdraw(id, "uid-b");
        f.asked(id, b2);
        f.withdraw(id, "uid-b2");

        f.returned("uid-b");

        assertThat(f.holdersFor(id, List.of(b2, FastWriteLeaderTest.B)))
                .as("b2 is still full: only b came back").containsExactly(FastWriteLeaderTest.B);
    }
}
