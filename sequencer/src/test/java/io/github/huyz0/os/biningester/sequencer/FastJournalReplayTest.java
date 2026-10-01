// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What recovery replays, what the live journal reports, and what an I/O
 * failure ends (M13.24 review round 2, P1 and T1-T4, T7).
 */
class FastJournalReplayTest {

    private static final long CAP = 1 << 20;
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static FastJournalRecord.Entry entry(long firstOffset, int records, long assignedAfter) {
        List<SegmentRecord> rs = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            rs.add(new SegmentRecord("d" + firstOffset + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(7, A, firstOffset, 3, assignedAfter,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), firstOffset), rs);
    }

    private static List<Long> offsets(FastJournal journal) {
        return journal.held().stream().map(FastJournalRecord.Entry::firstOffset).toList();
    }

    @Test
    void aRELEASEStillInTheFileIsReplayedByRecovery() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        for (int i = 0; i < 4; i++) {
            journal.append(entry(i, 1, 0));
        }
        journal.release(A, 1);
        journal.sync();
        assertThat(journal.fileBytes()).as("one of four: under half, the release stays in the file")
                .isGreaterThan(journal.heldBytes());

        file.crash();

        assertThat(offsets(FastJournal.recover(file, CAP)))
                .as("recovery applies the release record it reads")
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void theLIVEJournalReflectsADropBeforeAnyRestart() throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), CAP);
        journal.append(entry(10, 1, 5));
        journal.append(entry(11, 1, 5));
        long one = journal.heldBytes() / 2;

        journal.drop(A, 7, 5, 11);

        assertThat(offsets(journal)).containsExactly(10L);
        assertThat(journal.heldBytes())
                .as("a dropped group no longer counts against the cap, before any restart")
                .isEqualTo(one);
    }

    @Test
    void theCAPCountsWhatRecoveryReadBack() throws Exception {
        FastJournalRecord.Entry e = entry(0, 1, 0);
        long cap = 2L * e.encode().length;
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, cap);
        journal.append(e);
        journal.append(entry(1, 1, 0));
        journal.sync();
        file.crash();

        FastJournal recovered = FastJournal.recover(file, cap);

        assertThat(recovered.heldBytes()).isEqualTo(cap);
        assertThat(recovered.append(entry(2, 1, 0)))
                .as("a restarted pod is as full as the one that crashed; accepting a third "
                        + "would hold twice its cap")
                .isFalse();
    }

    @Test
    void aDROPTakesEntriesStartingAtItsOffsetNotOnesThatStraddleIt() throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), CAP);
        journal.append(entry(18, 3, 5));
        journal.append(entry(21, 2, 5));

        journal.drop(A, 7, 5, 20);

        assertThat(offsets(journal))
                .as("18..20 starts below 20 and is kept; 21.. is dropped")
                .containsExactly(18L);
    }

    @Test
    void aNONPositiveCapIsRefused() {
        assertThatThrownBy(() -> FastJournal.recover(new MemoryJournalFile(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anIOFailureENDSTheJournal() throws Exception {
        MemoryJournalFile memory = new MemoryJournalFile();
        JournalFile failing = new JournalFile() {
            private int appends;

            @Override
            public byte[] readAll() {
                return memory.readAll();
            }

            @Override
            public void append(byte[] bytes) throws IOException {
                if (++appends == 2) {
                    memory.append(java.util.Arrays.copyOf(bytes, bytes.length / 2));
                    throw new IOException("no space left on device");
                }
                memory.append(bytes);
            }

            @Override
            public void force() {
                memory.force();
            }

            @Override
            public void truncate(long length) {
                memory.truncate(length);
            }

            @Override
            public void replace(byte[] contents) {
                memory.replace(contents);
            }

            @Override
            public long size() {
                return memory.size();
            }

            @Override
            public void close() {
            }
        };
        FastJournal journal = FastJournal.recover(failing, CAP);
        journal.append(entry(0, 1, 0));
        journal.sync();
        assertThatThrownBy(() -> journal.append(entry(1, 1, 0))).isInstanceOf(IOException.class);

        assertThatThrownBy(() -> journal.append(entry(2, 1, 0)))
                .as("half an entry is in the file; one appended and answered after it would be "
                        + "cut with it at the next recovery")
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(journal::sync).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.release(A, 1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> journal.drop(A, 7, 0, 0)).isInstanceOf(IllegalStateException.class);
        memory.force();
        assertThat(offsets(FastJournal.recover(memory, CAP)))
                .as("and recovery from the file cuts the half entry as a tear")
                .containsExactly(0L);
    }
}
