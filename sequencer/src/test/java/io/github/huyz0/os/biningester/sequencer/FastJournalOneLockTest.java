// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A pod's journal is reached under one lock, its own (M13.27t; M13.27e's
 * review round 3, P13): the write leader, the replica endpoint, the HELD
 * report, a departure and a release all reach the same journal.
 *
 * <p>⚠️ **EACH CALLER LOCKED ITS OWN**, or none, so a HELD report read the
 * held map while the leader appended to it -- a {@code HashMap} read during a
 * write, and a report naming an entry half added.
 */
class FastJournalOneLockTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    /** A file whose first armed append blocks until opened. */
    private static final class BlockingFile implements JournalFile {
        final MemoryJournalFile inner = new MemoryJournalFile();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch open = new CountDownLatch(1);
        volatile boolean armed;

        @Override
        public byte[] readAll() {
            return inner.readAll();
        }

        @Override
        public void append(byte[] bytes) throws IOException {
            if (armed) {
                armed = false;
                entered.countDown();
                try {
                    open.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            inner.append(bytes);
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

    private static FastJournalRecord.Entry entry(long firstOffset) {
        return new FastJournalRecord.Entry(1, A, firstOffset, 2, 0,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), firstOffset),
                List.of(new SegmentRecord("d" + firstOffset, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    /** {@code journal} with an append of offset 0 blocked inside its file. */
    private static CompletableFuture<Boolean> appendBlocked(FastJournal journal,
            BlockingFile file) throws Exception {
        file.armed = true;
        CompletableFuture<Boolean> appending = CompletableFuture.supplyAsync(() -> {
            try {
                return journal.append(entry(0));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(file.entered.await(10, TimeUnit.SECONDS)).as("the append began").isTrue();
        return appending;
    }

    @Test
    void aREADWaitsForAnAppendInProgress() throws Exception {
        BlockingFile file = new BlockingFile();
        FastJournal journal = FastJournal.recover(file, 1 << 20);
        CompletableFuture<Boolean> appending = appendBlocked(journal, file);

        CompletableFuture<List<FastJournalRecord.Entry>> reading =
                CompletableFuture.supplyAsync(journal::held);

        assertThat(reading).as("held() waits for the append's lock")
                .failsWithin(300, TimeUnit.MILLISECONDS);
        file.open.countDown();
        assertThat(appending.get(10, TimeUnit.SECONDS)).isTrue();
        assertThat(reading.get(10, TimeUnit.SECONDS)).as("and sees the entry whole")
                .extracting(FastJournalRecord.Entry::firstOffset).containsExactly(0L);
    }

    @Test
    void aRELEASEWaitsForAnAppendInProgress() throws Exception {
        BlockingFile file = new BlockingFile();
        FastJournal journal = FastJournal.recover(file, 1 << 20);
        CompletableFuture<Boolean> appending = appendBlocked(journal, file);

        CompletableFuture<Void> releasing = CompletableFuture.runAsync(() -> {
            try {
                journal.release(A, 5);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(releasing).as("release waits for the append's lock")
                .failsWithin(300, TimeUnit.MILLISECONDS);
        file.open.countDown();
        appending.get(10, TimeUnit.SECONDS);
        releasing.get(10, TimeUnit.SECONDS);
        assertThat(journal.held()).as("released after it was appended").isEmpty();
    }
}
