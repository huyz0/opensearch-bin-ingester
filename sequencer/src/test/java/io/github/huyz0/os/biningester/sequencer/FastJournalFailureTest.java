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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * I/O failures and crashes INSIDE the journal's operations (M13.24 review
 * round 3, P1, T1 and T2).
 *
 * <p>⚠️ THE OTHER CASES CRASH BETWEEN CALLS. A compaction that rewrote the file
 * in place, a failed fsync that was retried, a recovery that trusted bytes a
 * page cache held and the disk did not -- each passes every case that crashes
 * only between calls, and each loses entries that were answered.
 */
class FastJournalFailureTest {

    private static final long CAP = 1 << 20;
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static FastJournalRecord.Entry entry(long firstOffset) {
        return new FastJournalRecord.Entry(7, A, firstOffset, 2, 0,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), firstOffset),
                List.of(new SegmentRecord("d" + firstOffset, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1, 2})));
    }

    private static List<Long> offsets(FastJournal journal) {
        return journal.held().stream().map(FastJournalRecord.Entry::firstOffset).toList();
    }

    /** A file whose named operation fails once, after the first {@code skip} calls of it. */
    private static final class FailingFile implements JournalFile {
        final MemoryJournalFile memory = new MemoryJournalFile();
        final String op;
        int skip;

        FailingFile(String op, int skip) {
            this.op = op;
            this.skip = skip;
        }

        private void maybeFail(String called) throws IOException {
            if (called.equals(op) && skip-- == 0) {
                throw new IOException(called + " failed");
            }
        }

        @Override
        public byte[] readAll() {
            return memory.readAll();
        }

        @Override
        public void append(byte[] bytes) throws IOException {
            maybeFail("append");
            memory.append(bytes);
        }

        @Override
        public void force() throws IOException {
            maybeFail("force");
            memory.force();
        }

        @Override
        public void truncate(long length) throws IOException {
            maybeFail("truncate");
            memory.truncate(length);
        }

        @Override
        public void replace(byte[] contents) throws IOException {
            maybeFail("replace");
            memory.replace(contents);
        }

        @Override
        public long size() {
            return memory.size();
        }

        @Override
        public void close() {
        }
    }

    private static void assertEnded(FastJournal journal) {
        assertThatThrownBy(() -> journal.append(entry(99))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(journal::sync).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aFAILEDForceEndsTheJournalAndIsNeverRetried() throws Exception {
        FailingFile file = new FailingFile("force", 0);
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(0));

        assertThatThrownBy(journal::sync).isInstanceOf(IOException.class);

        assertEnded(journal);
    }

    @Test
    void aFAILEDCompactionEndsTheJournal() throws Exception {
        FailingFile file = new FailingFile("replace", 1);
        FastJournal journal = FastJournal.recover(file, CAP);
        for (int i = 0; i < 4; i++) {
            journal.append(entry(i));
        }
        journal.release(A, 3);

        assertThatThrownBy(journal::sync).as("the compaction's replace fails")
                .isInstanceOf(IOException.class);

        assertEnded(journal);
    }

    @Test
    void aFAILEDReleaseOrDropAppendEndsTheJournal() throws Exception {
        FailingFile releasing = new FailingFile("append", 1);
        FastJournal one = FastJournal.recover(releasing, CAP);
        one.append(entry(0));
        assertThatThrownBy(() -> one.release(A, 1)).isInstanceOf(IOException.class);
        assertEnded(one);

        FailingFile dropping = new FailingFile("append", 1);
        FastJournal two = FastJournal.recover(dropping, CAP);
        two.append(entry(0));
        assertThatThrownBy(() -> two.drop(A, 7, 0, 0)).isInstanceOf(IOException.class);
        assertEnded(two);
    }

    /** Crashes -- losing every unforced byte -- at the {@code at}-th file operation. */
    private static final class CrashingFile implements JournalFile {
        final MemoryJournalFile memory = new MemoryJournalFile();
        int at = -1;
        int ops;

        private void maybeCrash() throws IOException {
            if (at >= 0 && ops++ == at) {
                memory.crash();
                throw new IOException("crashed at operation " + at);
            }
        }

        @Override
        public byte[] readAll() {
            return memory.readAll();
        }

        @Override
        public void append(byte[] bytes) throws IOException {
            maybeCrash();
            memory.append(bytes);
        }

        @Override
        public void force() throws IOException {
            maybeCrash();
            memory.force();
        }

        @Override
        public void truncate(long length) throws IOException {
            maybeCrash();
            memory.truncate(length);
        }

        @Override
        public void replace(byte[] contents) throws IOException {
            maybeCrash();
            memory.replace(contents);
        }

        @Override
        public long size() {
            return memory.size();
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aCRASHAtAnyStepOfACompactingSyncLosesNoHeldEntry() throws Exception {
        for (int at = 0; at < 6; at++) {
            CrashingFile file = new CrashingFile();
            FastJournal journal = FastJournal.recover(file, CAP);
            for (int i = 0; i < 4; i++) {
                journal.append(entry(i));
            }
            journal.sync();
            journal.release(A, 3);
            file.ops = 0;
            file.at = at;

            try {
                journal.sync();
            } catch (IOException crashed) {
                // the crash under test
            }

            assertThat(offsets(FastJournal.recover(file.memory, CAP)))
                    .as("crash at operation %d of a compacting sync: entry 3 was held before "
                            + "and after; a compaction rewriting the file in place leaves an "
                            + "empty journal between its truncate and its force", at)
                    .contains(3L);
        }
    }

    /**
     * A file whose failed fsync leaves the page cache claiming bytes the disk
     * never received -- what Linux does after a writeback error, and why a
     * failed fsync is not retried (M13.24 review round 3, P1).
     */
    private static final class LyingFile implements JournalFile {
        private byte[] cache = new byte[0];
        private int forced;
        private final List<int[]> lost = new ArrayList<>();
        boolean failNextForce;

        @Override
        public byte[] readAll() {
            return cache.clone();
        }

        @Override
        public void append(byte[] bytes) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.writeBytes(cache);
            out.writeBytes(bytes);
            cache = out.toByteArray();
        }

        @Override
        public void force() throws IOException {
            if (failNextForce) {
                failNextForce = false;
                // the pages are marked clean: a later fsync will not write them
                lost.add(new int[] {forced, cache.length});
                forced = cache.length;
                throw new IOException("EIO on fsync");
            }
            forced = cache.length;
        }

        @Override
        public void truncate(long length) {
            cache = Arrays.copyOf(cache, (int) length);
            forced = Math.min(forced, (int) length);
        }

        @Override
        public void replace(byte[] contents) {
            cache = contents.clone();
            forced = cache.length;
            lost.clear();
        }

        @Override
        public long size() {
            return cache.length;
        }

        @Override
        public void close() {
        }

        /** A reboot: the disk holds what was forced, with the lost ranges stale (zero). */
        void reboot() {
            byte[] disk = Arrays.copyOf(cache, forced);
            for (int[] range : lost) {
                Arrays.fill(disk, range[0], Math.min(range[1], disk.length), (byte) 0);
            }
            cache = disk;
            lost.clear();
        }
    }

    @Test
    void recoveryAfterAFAILEDFsyncTrustsNoCachedByteSoLaterAnswersSurviveAReboot()
            throws Exception {
        LyingFile file = new LyingFile();
        FastJournal journal = FastJournal.recover(file, CAP);
        journal.append(entry(0));
        file.failNextForce = true;
        assertThatThrownBy(journal::sync).isInstanceOf(IOException.class);

        FastJournal next = FastJournal.recover(file, CAP);
        next.append(entry(1));
        next.sync();
        file.reboot();

        assertThat(offsets(FastJournal.recover(file, CAP)))
                .as("entry 1 was answered after its sync; a recovery that trusted entry 0's "
                        + "cached bytes left a stale hole in front of it, and the reboot's "
                        + "recovery cut entry 1 with that hole")
                .contains(1L);
    }
}
