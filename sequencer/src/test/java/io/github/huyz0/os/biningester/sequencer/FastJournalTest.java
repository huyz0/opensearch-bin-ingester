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
 * The fast journal (M13.24; ADR-0081, ADR-0082 §4).
 *
 * <p>⚠️ EVERY CASE ENDS IN A CRASH AND A RECOVERY. What the journal is for is
 * the state a restarted container reads back, so a case that only inspects
 * the live object checks bookkeeping that dies with the process. The fake file
 * loses unforced bytes, or keeps a torn prefix of them, as ADR-0082 §4
 * assumes a disk does.
 */
class FastJournalTest {

    private static final long CAP = 1 << 20;
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 1);

    private static FastJournalRecord.Entry entry(RunKey key, long epoch, long assignedAfter,
            long firstOffset, int records) {
        List<SegmentRecord> rs = new java.util.ArrayList<>();
        for (int i = 0; i < records; i++) {
            rs.add(new SegmentRecord("doc-" + firstOffset + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1, 2, 3}));
        }
        return new FastJournalRecord.Entry(epoch, key, firstOffset, 2, assignedAfter,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), firstOffset), rs);
    }

    private static List<String> offsets(FastJournal journal) {
        return journal.held().stream().map(e -> e.key().partitionId() + "@" + e.firstOffset())
                .toList();
    }

    @Test
    void aSYNCEDEntryIsRecoveredAfterACrash() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        FastJournalRecord.Entry e = entry(A, 7, 0, 100, 2);
        assertThat(journal.append(e)).isTrue();
        journal.sync();

        file.crash();
        FastJournal recovered = FastJournal.recover(file, CAP);

        assertThat(recovered.held()).hasSize(1);
        assertThat(recovered.held().get(0).encode()).isEqualTo(e.encode());
    }

    @Test
    void anUNSYNCEDEntryIsLostByACrash() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(A, 7, 0, 100, 2));

        file.crash();

        assertThat(FastJournal.recover(file, CAP).held())
                .as("nothing was answered about it, so nothing is owed")
                .isEmpty();
    }

    @Test
    void aTORNTailIsTruncatedBeforeTheNextAppendSoThatAppendSurvives() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        FastJournalRecord.Entry first = entry(A, 7, 0, 100, 1);
        journal.append(first);
        journal.sync();
        journal.append(entry(A, 7, 0, 101, 1));
        file.crashTearing(10);

        FastJournal recovered = FastJournal.recover(file, CAP);
        assertThat(offsets(recovered)).containsExactly("0@100");
        assertThat(recovered.fileBytes())
                .as("the torn bytes are cut before anything is appended")
                .isEqualTo(first.encode().length);
        recovered.append(entry(A, 7, 0, 102, 1));
        recovered.sync();
        file.crash();

        assertThat(offsets(FastJournal.recover(file, CAP)))
                .as("an entry appended after an uncut tear would be unreadable behind it "
                        + "(ADR-0082 §4; M13.22c review round 2, P1)")
                .containsExactly("0@100", "0@102");
    }

    @Test
    void aRELEASECoversOnlyEntriesWhollyBelowItsOffsetAndOnlyItsStream() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(A, 7, 0, 0, 2));
        journal.append(entry(A, 7, 0, 2, 2));
        journal.append(entry(A, 7, 0, 4, 2));
        journal.append(entry(B, 7, 0, 0, 2));

        journal.release(A, 5);
        journal.sync();

        assertThat(offsets(journal))
                .as("A@0 and A@2 end at or below 5; A@4 spans 4..5 and is not wholly below; "
                        + "B is another stream")
                .containsExactly("0@4", "1@0");
        file.crash();
        assertThat(offsets(FastJournal.recover(file, CAP)))
                .as("and recovery applies the release as the live journal did")
                .containsExactly("0@4", "1@0");
    }

    @Test
    void aDROPRemovesItsGroupAtOrAboveItsOffsetAndNothingElse() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(A, 7, 5, 10, 1));
        journal.append(entry(A, 7, 5, 20, 1));
        journal.append(entry(A, 7, 6, 21, 1));
        journal.append(entry(A, 8, 5, 22, 1));

        journal.drop(A, 7, 5, 20);
        journal.sync();
        file.crash();

        assertThat(FastJournal.recover(file, CAP).held())
                .extracting(FastJournalRecord.Entry::firstOffset)
                .as("only (epoch 7, assignedAfter 5) at or above 20 is dropped: a later group "
                        + "at the same epoch, and another epoch, are other entries")
                .containsExactly(10L, 21L, 22L);
    }

    @Test
    void theBYTEBoundRefusesAnEntryThatWouldPassTheCap() throws Exception {
        FastJournalRecord.Entry e = entry(A, 7, 0, 0, 1);
        long cap = 2L * e.encode().length;
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, cap);
        assertThat(journal.append(e)).isTrue();
        assertThat(journal.append(entry(A, 7, 0, 1, 1))).isTrue();
        long before = journal.fileBytes();

        assertThat(journal.append(entry(A, 7, 0, 2, 1)))
                .as("a third would pass the cap: refused, which is the caller's backpressure")
                .isFalse();
        assertThat(journal.fileBytes()).as("and nothing of it was written").isEqualTo(before);

        journal.release(A, 1);
        assertThat(journal.append(entry(A, 7, 0, 2, 1)))
                .as("released bytes no longer count against the cap").isTrue();
    }

    @Test
    void releasedBytesPASTHalfTheFileAreCompactedAway() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        for (int i = 0; i < 4; i++) {
            journal.append(entry(A, 7, 0, i, 1));
        }
        journal.sync();

        journal.release(A, 3);
        journal.sync();

        assertThat(journal.fileBytes())
                .as("three of four entries released: the file is rewritten to the one held")
                .isEqualTo(journal.heldBytes());
        file.crash();
        assertThat(offsets(FastJournal.recover(file, CAP))).containsExactly("0@3");
    }
}
