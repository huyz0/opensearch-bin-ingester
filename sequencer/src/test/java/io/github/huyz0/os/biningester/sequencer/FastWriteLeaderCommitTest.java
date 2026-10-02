// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A refused append withdrawn whole, the bound and the frontier moved by a
 * commit, and a declining pod chosen again once it returns (M13.27e review
 * round 1, T12, T13, P9).
 */
class FastWriteLeaderCommitTest {

    /** Appends a filler entry to the journal, once, from inside the first append. */
    static final class FillingFile implements JournalFile {
        final MemoryJournalFile inner = new MemoryJournalFile();
        FastJournal journal;
        FastJournalRecord.Entry filler;
        boolean armed;

        @Override
        public byte[] readAll() {
            return inner.readAll();
        }

        @Override
        public void append(byte[] bytes) throws IOException {
            inner.append(bytes);
            if (armed) {
                armed = false;
                journal.append(filler);
            }
        }

        @Override
        public void force() {
            inner.force();
        }

        @Override
        public void truncate(long length) {
            inner.truncate(length);
        }

        @Override
        public void replace(byte[] contents) {
            inner.replace(contents);
        }

        @Override
        public long size() {
            return inner.size();
        }

        @Override
        public void close() {
        }
    }

    private static FastJournalRecord.Entry entryOf(FastWriteFrame.Commit commit, int run,
            long first, int q) {
        return new FastJournalRecord.Entry(7, commit.runs().get(run).stream(), first, q, 3,
                commit.key(), commit.runs().get(run).records());
    }

    @Test
    void anAPPENDRefusedMidBatchWithdrawsItWhole() throws Exception {
        FastWriteFrame.Commit commit = FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1,
                FastWriteLeaderTest.S2);
        long cap = entryOf(commit, 0, 100, 1).encode().length
                + entryOf(commit, 1, 0, 2).encode().length;
        FillingFile file = new FillingFile();
        FastJournal journal = FastJournal.recover(file, cap);
        file.journal = journal;
        FastWriteFrame.Commit other = FastWriteLeaderTest.commit(9, FastWriteLeaderTest.S1);
        // ANOTHER WRITER'S ENTRY, its own group: the withdrawal drops the
        // batch's group only.
        file.filler = new FastJournalRecord.Entry(7, FastWriteLeaderTest.S1, 500, 1, 9,
                other.key(), other.runs().get(0).records());
        FastCursor cursor = new FastCursor(1_000);
        FastWriteLeader l = new FastWriteLeader(7, FastWriteLeaderTest.LEADER, cursor,
                new TermRecordWriter(new MemoryBinStore(), "p", FastWriteLeaderTest.roster(),
                        new Mono(), Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), journal, () -> 3);
        l.open(FastWriteLeaderTest.S1, 100);
        l.open(FastWriteLeaderTest.S2, 0);
        file.armed = true;

        assertThatThrownBy(() -> l.commit(FastWriteLeaderTest.B, commit,
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL))
                .as("the second run no longer fits: the journal filled meanwhile")
                .isInstanceOf(IllegalStateException.class);

        assertThat(l.flushGroup()).as("never answered").isEmpty();
        assertThat(cursor.cursor(FastWriteLeaderTest.S1)).isEqualTo(100);
        assertThat(cursor.cursor(FastWriteLeaderTest.S2)).isZero();
        assertThat(journal.held()).as("the first run's entry dropped; the filler kept")
                .extracting(e -> e.firstOffset()).containsExactly(500L);
    }

    @Test
    void aCOMMITMovesTheBoundAndTheFrontier() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 2);
        l.leader().commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        l.leader().flushGroup();
        assertThat(l.leader().commit(FastWriteLeaderTest.B,
                FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S2), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL)).as("S2 is at B = 2").isInstanceOf(FastWriteLeader.Wait.class);

        l.leader().committed(FastWriteLeaderTest.S2, 2);

        assertThat(l.leader().commit(FastWriteLeaderTest.B,
                FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S2), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL)).isInstanceOf(FastWriteLeader.Pending.class);
        l.leader().flushGroup();
        l.leader().confirm(FastWriteLeaderTest.B, new FastWriteFrame.Confirm(
                FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(FastWriteLeaderTest.S2, 2)), true));
        assertThat(l.leader().expose()).as("the frontier moved past the committed 0-1")
                .hasSize(1);
    }

    @Test
    void aDECLININGPodIsChosenAgainOnceItReturns() {
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");
        f.open(FastWriteLeaderTest.S1, 0);
        EntryId id = new EntryId(7, 0, FastWriteLeaderTest.S1, 0);
        f.assigned(id, 1, 2);
        f.asked(id, FastWriteLeaderTest.B);
        f.withdraw(id, "uid-b");
        assertThat(f.holdersFor(id, List.of(FastWriteLeaderTest.B))).isEmpty();

        f.returned("uid-b");

        assertThat(f.holdersFor(id, List.of(FastWriteLeaderTest.B)))
                .as("the zone's only pod, back").containsExactly(FastWriteLeaderTest.B);
    }
}
