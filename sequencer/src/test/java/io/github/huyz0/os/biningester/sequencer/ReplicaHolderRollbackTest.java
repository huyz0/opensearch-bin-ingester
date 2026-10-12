// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A REPLICA whose journal filled between the cap check and an append keeps
 * nothing of the batch (M13.28a review round 1, T4): the entries it appended
 * are dropped, and only what another caller journaled stays.
 */
class ReplicaHolderRollbackTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);
    private static final RunKey OTHER = new RunKey(new UUID(3, 3), 0);

    private static FastJournalRecord.Entry entry(long epoch, RunKey key, long first) {
        return new FastJournalRecord.Entry(epoch, key, first, 2, 4,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    @Test
    void aJOURNALFilledMidBatchKeepsNothingOfIt() throws Exception {
        FastJournalRecord.Entry a = entry(3, A, 10);
        FastJournalRecord.Entry b = entry(3, B, 0);
        FastWriteLeaderCommitTest.FillingFile file = new FastWriteLeaderCommitTest.FillingFile();
        FastJournal journal = FastJournal.recover(file,
                a.encode().length + b.encode().length);
        file.journal = journal;
        file.filler = entry(9, OTHER, 0);
        JoinedTerms terms = new JoinedTerms();
        terms.joined(3, 0, "uid-l");
        file.armed = true;

        FastFrame.Body answer = new ReplicaHolder(journal, terms, new HolderGate()).answer(
                new FastFrame.Header(FastWriteFrame.KIND_REPLICA, 3, "uid-l", "uid-h"),
                new FastWriteFrame.Replica(List.of(a, b)));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.BACKPRESSURE);
        assertThat(journal.held()).extracting(FastJournalRecord.Entry::key)
                .as("the other caller's entry alone").containsExactly(OTHER);
    }
}
