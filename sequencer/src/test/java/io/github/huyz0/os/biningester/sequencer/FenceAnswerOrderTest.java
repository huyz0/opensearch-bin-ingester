// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A FENCE's pages, pinned at their edges (M13.33b review round 1, T1-T3, P1):
 * the order within a term, a page cap met exactly, an entry at exactly
 * {@code closedThrough}, and no copy landing between two pages.
 */
@Timeout(60)
class FenceAnswerOrderTest {

    private static FastJournalRecord.Entry entry(long epoch, long assignedAfter, RunKey key,
            long first) {
        return new FastJournalRecord.Entry(epoch, key, first, 2, assignedAfter,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    private static FastJournal holding(FastJournalRecord.Entry... entries) throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        for (FastJournalRecord.Entry e : entries) {
            assertThat(journal.append(e)).isTrue();
        }
        journal.sync();
        return journal;
    }

    private static FastFrame.Header from(long epoch) {
        return new FastFrame.Header(FastTakeoverFrame.KIND_FENCE, epoch, "uid-s", "uid-m");
    }

    private static List<FastJournalRecord.Entry> allPages(FenceAnswer answerer, long fenced)
            throws Exception {
        List<FastJournalRecord.Entry> all = new ArrayList<>();
        for (long page = 0; ; page++) {
            FastTakeoverFrame.Collected c = (FastTakeoverFrame.Collected) answerer.answer(
                    from(fenced + 1), new FastTakeoverFrame.Fence(List.of(fenced), 0, page));
            all.addAll(c.entries());
            if (c.last()) {
                return all;
            }
        }
    }

    @Test
    void withinATERMEntriesGoByAssignedAfterThenStreamThenOffset() throws Exception {
        RunKey a0 = new RunKey(new UUID(1, 1), 0);
        RunKey a1 = new RunKey(new UUID(1, 1), 1);
        RunKey b0 = new RunKey(new UUID(2, 2), 0);
        FastJournal journal = holding(entry(7, 2, a0, 1), entry(7, 1, b0, 2),
                entry(7, 1, a1, 3), entry(7, 1, a0, 5), entry(7, 1, a0, 4));
        long one = entry(7, 1, a0, 4).encode().length;

        List<FastJournalRecord.Entry> order = allPages(new FenceAnswer(journal, one), 7);

        assertThat(order).extracting(FastJournalRecord.Entry::firstOffset)
                .as("aa 1: a/0 at 4, 5, a/1, b/0; then aa 2").containsExactly(4L, 5L, 3L, 2L, 1L);
    }

    @Test
    void entriesFILLINGAPageExactlyShareIt() throws Exception {
        RunKey s = new RunKey(new UUID(1, 1), 0);
        FastJournal journal = holding(entry(7, 1, s, 10), entry(7, 1, s, 11));
        long both = entry(7, 1, s, 10).encode().length + entry(7, 1, s, 11).encode().length;

        FastTakeoverFrame.Collected page = (FastTakeoverFrame.Collected) new FenceAnswer(journal,
                both).answer(from(8), new FastTakeoverFrame.Fence(List.of(7L), 0));

        assertThat(page.entries()).hasSize(2);
        assertThat(page.last()).isTrue();
    }

    @Test
    void anENTRYAtExactlyClosedThroughIsDropped() throws Exception {
        RunKey s = new RunKey(new UUID(1, 1), 0);
        FastJournal journal = holding(entry(6, 1, s, 10), entry(7, 1, s, 20));

        new FenceAnswer(journal, 1 << 20).answer(from(8),
                new FastTakeoverFrame.Fence(List.of(7L), 6));

        assertThat(journal.held()).extracting(FastJournalRecord.Entry::epoch).containsExactly(7L);
    }

    @Test
    void aREPLICABelowTheFenceIsRefusedUnderTheGate() throws Exception {
        RunKey s = new RunKey(new UUID(1, 1), 0);
        FastJournal journal = holding();
        JoinedTerms terms = new JoinedTerms();
        terms.joined(7, 0, "uid-l");
        ReplicaHolder holder = new ReplicaHolder(journal, terms, new HolderGate(), () -> 8);

        FastFrame.Body answer = holder.answer(new FastFrame.Header(FastWriteFrame.KIND_REPLICA,
                7, "uid-l", "uid-m"), new FastWriteFrame.Replica(List.of(entry(7, 1, s, 10))));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(journal.held()).isEmpty();
    }

    @Test
    void aFENCEsPageWaitsForAnAppendInProgress() throws Exception {
        FastJournal journal = holding();
        HolderGate gate = new HolderGate();
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        CompletableFuture<Object> appending = CompletableFuture.supplyAsync(() -> {
            try {
                return gate.whileAccepting(() -> {
                    inside.countDown();
                    try {
                        open.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return true;
                });
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(inside.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<FastFrame.Body> paging = CompletableFuture.supplyAsync(() -> {
            try {
                return new FenceAnswer(journal, 1 << 20, gate).answer(from(8),
                        new FastTakeoverFrame.Fence(List.of(7L), 0));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(paging).as("the page is read under the gate")
                .failsWithin(300, TimeUnit.MILLISECONDS);
        open.countDown();
        appending.get(10, TimeUnit.SECONDS);
        assertThat(paging.get(10, TimeUnit.SECONDS)).isInstanceOf(FastTakeoverFrame.Collected.class);
    }
}
