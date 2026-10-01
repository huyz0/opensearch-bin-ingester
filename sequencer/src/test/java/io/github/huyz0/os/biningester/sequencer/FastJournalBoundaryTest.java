// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * The journal's boundaries (M13.24 review round 1, P1, T2, T3): what recovery
 * refuses to cut, when compaction starts, and which streams a drop reaches.
 */
class FastJournalBoundaryTest {

    private static final long CAP = 1 << 20;
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);

    private static FastJournalRecord.Entry entry(RunKey key, long firstOffset) {
        return new FastJournalRecord.Entry(7, key, firstOffset, 2, 5,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), 1),
                List.of(new SegmentRecord("d" + firstOffset, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    @Test
    void recoveryREFUSESAWholeUnreadableRecordAndCutsNothing() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(A, 0));
        journal.sync();
        byte[] rest = Arrays.copyOfRange(entry(A, 1).encode(), FastJournalRecord.HEADER_BYTES,
                entry(A, 1).encode().length);
        CRC32C crc = new CRC32C();
        crc.update(rest);
        file.append(ByteBuffer.allocate(FastJournalRecord.HEADER_BYTES + rest.length)
                .putInt(FastJournalRecord.MAGIC).put((byte) 2).putInt(rest.length)
                .putInt((int) crc.getValue()).put(rest).array());
        file.force();
        long before = file.size();

        assertThatThrownBy(() -> FastJournal.recover(file, CAP))
                .as("a version-2 record a later build fsynced: refuse, never truncate")
                .isInstanceOf(IOException.class);
        assertThat(file.size()).as("nothing was cut").isEqualTo(before);
    }

    @Test
    void COMPACTIONWaitsUntilReleasedBytesPassHalfTheFile() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        for (int i = 0; i < 4; i++) {
            journal.append(entry(A, i));
        }
        journal.sync();
        long four = journal.fileBytes();

        journal.release(A, 1);
        journal.sync();

        assertThat(journal.fileBytes())
                .as("one of four released is under half: no rewrite -- compacting earlier "
                        + "rewrites up to the whole cap on every group fsync")
                .isGreaterThan(four);
    }

    @Test
    void aDROPReachesItsOwnStreamOnly() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(A, 10));
        journal.append(entry(B, 10));

        journal.drop(A, 7, 5, 0);
        journal.sync();
        file.crash();

        assertThat(FastJournal.recover(file, CAP).held())
                .extracting(FastJournalRecord.Entry::key)
                .as("one batch's runs share (epoch, assignedAfter) across streams; a drop for "
                        + "A must leave B's copy held")
                .containsExactly(B);
    }
}
