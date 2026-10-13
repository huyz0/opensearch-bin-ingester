// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A stream's exposed entries read from the journal for a catch-up past the
 * committed chain (M13.32b; ADR-0081 §6): its term's only, contiguous from an
 * entry's first offset, stopping at the first hole and at the first entry not
 * wholly below the quorum frontier -- never an unexposed one.
 */
class FastJournalRangeTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);

    private static FastJournalRecord.Entry entry(RunKey key, long epoch, long assignedAfter,
            long firstOffset, int records) {
        List<SegmentRecord> rs = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            rs.add(new SegmentRecord("d" + firstOffset + "-" + i + "-" + assignedAfter,
                    OpType.INDEX, OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(epoch, key, firstOffset, 1, assignedAfter,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9),
                        assignedAfter * 1_000 + firstOffset), rs);
    }

    private static List<Long> offsets(List<FastJournalRecord.Entry> entries) {
        return entries.stream().map(FastJournalRecord.Entry::firstOffset).toList();
    }

    private static FastJournal journal(FastJournalRecord.Entry... entries) throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        for (FastJournalRecord.Entry e : entries) {
            journal.append(e);
        }
        return journal;
    }

    @Test
    void entriesFromAnOffsetAreReadInOrderUpToTheFrontierAndNoFurther() throws Exception {
        FastJournal journal = journal(entry(A, 3, 0, 14, 2), entry(A, 3, 0, 10, 2),
                entry(A, 3, 0, 12, 2), entry(A, 2, 0, 16, 2), entry(B, 3, 0, 16, 2));

        assertThat(offsets(journal.exposedFrom(A, 3, 10, 16))).containsExactly(10L, 12L, 14L);
        assertThat(offsets(journal.exposedFrom(A, 3, 10, 15)))
                .as("[14, 16) not wholly exposed").containsExactly(10L, 12L);
        assertThat(offsets(journal.exposedFrom(A, 3, 12, 100)))
                .as("16 is another term's and another stream's").containsExactly(12L, 14L);
    }

    @Test
    void aHOLEOrAStartInsideAnEntryReadsNothingPastIt() throws Exception {
        FastJournal journal = journal(entry(A, 3, 0, 10, 2), entry(A, 3, 0, 14, 2));

        assertThat(offsets(journal.exposedFrom(A, 3, 10, 100))).as("12 is missing")
                .containsExactly(10L);
        assertThat(journal.exposedFrom(A, 3, 11, 100)).as("inside [10, 12)").isEmpty();
    }

    @Test
    void ofTwoEntriesAtOneOffsetTheLaterAssignmentIsRead() throws Exception {
        // ⚠️ its review round 1, T1: the later assignment appended FIRST, so the
        // order of appends cannot stand in for the order of assignments; and
        // T2: the superseded entry spans more than either newer one.
        // ⚠️ its review round 2, T3: and in the other order too, so neither the
        // first nor the last appended can stand in for the later assignment.
        FastJournal laterFirst = journal(entry(A, 3, 5, 10, 1), entry(A, 3, 5, 11, 1),
                entry(A, 3, 0, 10, 2));
        FastJournal laterLast = journal(entry(A, 3, 0, 10, 2), entry(A, 3, 5, 10, 1),
                entry(A, 3, 5, 11, 1));

        for (FastJournal journal : List.of(laterFirst, laterLast)) {
            assertThat(journal.exposedFrom(A, 3, 10, 100))
                    .extracting(FastJournalRecord.Entry::assignedAfter)
                    .containsExactly(5L, 5L);
        }
    }
}
