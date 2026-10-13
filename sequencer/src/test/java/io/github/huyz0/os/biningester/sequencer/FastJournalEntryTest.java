// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A held entry read by what a fast key names (M13.30o; ADR-0082 §6): its
 * epoch, stream, {@code assignedAfter} and first offset, each of which tells
 * two entries apart -- after a switch's discard one term may hold two entries
 * at one offset (ADR-0081 §5).
 */
class FastJournalEntryTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);

    private static FastJournalRecord.Entry entry(RunKey key, long epoch, long assignedAfter,
            long firstOffset, String id) {
        return new FastJournalRecord.Entry(epoch, key, firstOffset, 1, assignedAfter,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), assignedAfter * 100
                        + firstOffset), List.of(new SegmentRecord(id, OpType.INDEX,
                                OptionalLong.empty(), new byte[] {1})));
    }

    @Test
    void anENTRYIsFoundByWhatItsFastKeyNamesAndOnlyByThat() throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        journal.append(entry(A, 3, 0, 10, "first"));
        journal.append(entry(A, 3, 1, 10, "after the discard"));
        journal.append(entry(B, 3, 0, 10, "other stream"));

        assertThat(journal.entry(3, A, 1, 10)).map(e -> e.records().get(0).id())
                .hasValue("after the discard");
        assertThat(journal.entry(3, A, 0, 10)).map(e -> e.records().get(0).id())
                .hasValue("first");
        assertThat(journal.entry(4, A, 0, 10)).as("another term").isEmpty();
        assertThat(journal.entry(3, A, 0, 11)).as("another offset").isEmpty();
    }
}
