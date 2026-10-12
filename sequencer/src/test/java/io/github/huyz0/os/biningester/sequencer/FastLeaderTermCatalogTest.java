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
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The term record fed from the leader's catalog after the term start
 * (M13.27l; ADR-0081 §4): a fast index's {@code wal_quorum} recorded on the
 * term's roster BEFORE any batch of it is assigned under it, and the term's
 * desk assigning under it once it is.
 */
class FastLeaderTermCatalogTest {

    private static final RunKey S1 = new RunKey(FastWriteLeaderTest.Q1, 0);

    private static FastFrame.Body commitS1(FastLeaderTerm term, CommitDesk desk, long seq)
            throws Exception {
        return term.answerCommit(header(3, POD), commit(POD.podId(), seq, S1), desk);
    }

    @Test
    void aCATALOGsFastIndexIsRecordedBeforeItIsAssigned() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermTakeoverDeskTest.term3(store, false);
        join(term);
        CommitDesk desk = term.commitDesk(FastJournal.recover(new MemoryJournalFile(), 1 << 20),
                FastTermStartTest.SELF);
        term.catalogSays(Map.of(FastWriteLeaderTest.Q1, 1));

        assertThat(((FastFrame.Refused) commitS1(term, desk, 1)).text())
                .as("wanted, not yet recorded: the batch waits").contains("wal_quorum");

        assertThat(term.flushTermRecord()).isInstanceOf(TermRecordWriter.Written.class);
        Roster three = FastTermStartTest.read(store, 3);
        assertThat(three.termRecord().get(three.termRecord().size() - 1).walQuorum())
                .containsEntry(FastWriteLeaderTest.Q1, 1);
        assertThat(commitS1(term, desk, 2)).isInstanceOf(FastWriteFrame.Assigned.class);
    }

    @Test
    void aVALUEAlreadyRecordedWritesNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermTakeoverDeskTest.term3(store, false);
        term.catalogSays(Map.of(FastWriteLeaderTest.Q1, 1));
        term.flushTermRecord();

        term.catalogSays(Map.of(FastWriteLeaderTest.Q1, 1));

        assertThat(term.flushTermRecord()).isInstanceOf(TermRecordWriter.Idle.class);
    }
}
