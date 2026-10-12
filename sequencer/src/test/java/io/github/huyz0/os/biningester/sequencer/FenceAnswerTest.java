// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A member answers a successor's FENCE (M13.33b; ADR-0081 §5 step 4): it drops
 * what closed terms left, and answers the page asked of every entry it holds
 * of the fenced terms, in one fixed order -- so a page asked again is the same
 * page, and none is skipped.
 */
class FenceAnswerTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);

    private static FastJournalRecord.Entry entry(long epoch, RunKey key, long first) {
        return new FastJournalRecord.Entry(epoch, key, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    private static FastFrame.Header from(long epoch) {
        return new FastFrame.Header(FastTakeoverFrame.KIND_FENCE, epoch, "uid-s", "uid-m");
    }

    private record Member(FastJournal journal, FastWriteLeaderGroupTest.CountingFile file) {
    }

    private static Member holding(FastJournalRecord.Entry... entries) throws Exception {
        FastWriteLeaderGroupTest.CountingFile file = new FastWriteLeaderGroupTest.CountingFile();
        FastJournal journal = FastJournal.recover(file, 1 << 20);
        for (FastJournalRecord.Entry e : entries) {
            assertThat(journal.append(e)).isTrue();
        }
        journal.sync();
        return new Member(journal, file);
    }

    private static List<Long> offsets(FastFrame.Body answer) {
        return ((FastTakeoverFrame.Collected) answer).entries().stream()
                .map(FastJournalRecord.Entry::firstOffset).toList();
    }

    @Test
    void aMEMBERAnswersWhatItHoldsOfTheFencedTermsAndDropsClosedOnes() throws Exception {
        Member m = holding(entry(5, A, 1), entry(7, A, 10), entry(8, B, 20), entry(9, A, 30));
        int forces = m.file().forces;

        FastFrame.Body answer = new FenceAnswer(m.journal(), 1 << 20).answer(from(10),
                new FastTakeoverFrame.Fence(List.of(7L, 8L), 6));

        assertThat(answer).isInstanceOfSatisfying(FastTakeoverFrame.Collected.class, page -> {
            assertThat(page.page()).isZero();
            assertThat(page.last()).isTrue();
        });
        assertThat(offsets(answer)).as("term 9 is not fenced").containsExactly(10L, 20L);
        assertThat(m.journal().held()).extracting(FastJournalRecord.Entry::epoch)
                .as("term 5 is closed: dropped").doesNotContain(5L);
        assertThat(m.file().forces).as("the drop made durable").isGreaterThan(forces);
    }

    @Test
    void PAGESAreAskedForByNumberInOneFixedOrder() throws Exception {
        Member m = holding(entry(8, B, 20), entry(7, A, 11), entry(7, A, 10));
        long one = entry(7, A, 10).encode().length;
        FenceAnswer answerer = new FenceAnswer(m.journal(), one);

        FastFrame.Body p0 = answerer.answer(from(9), new FastTakeoverFrame.Fence(List.of(7L, 8L), 6, 0));
        FastFrame.Body p1 = answerer.answer(from(9), new FastTakeoverFrame.Fence(List.of(7L, 8L), 6, 1));
        FastFrame.Body again = answerer.answer(from(9),
                new FastTakeoverFrame.Fence(List.of(7L, 8L), 6, 1));
        FastFrame.Body p2 = answerer.answer(from(9), new FastTakeoverFrame.Fence(List.of(7L, 8L), 6, 2));

        assertThat(offsets(p0)).containsExactly(10L);
        assertThat(((FastTakeoverFrame.Collected) p0).last()).isFalse();
        assertThat(offsets(p1)).containsExactly(11L);
        assertThat(offsets(again)).as("asked again, the same page").containsExactly(11L);
        assertThat(offsets(p2)).containsExactly(20L);
        assertThat(((FastTakeoverFrame.Collected) p2).last()).isTrue();
    }

    @Test
    void aPAGEPastTheLastIsEmptyAndLast() throws Exception {
        Member m = holding(entry(7, A, 10));

        FastFrame.Body answer = new FenceAnswer(m.journal(), 1 << 20).answer(from(9),
                new FastTakeoverFrame.Fence(List.of(7L), 6, 5));

        assertThat(answer).isEqualTo(new FastTakeoverFrame.Collected(5, true, List.of()));
    }

    @Test
    void aMEMBERHoldingNothingAnswersOneEmptyPageMarkedLast() throws Exception {
        Member m = holding();

        assertThat(new FenceAnswer(m.journal(), 1 << 20).answer(from(9),
                new FastTakeoverFrame.Fence(List.of(7L), 6)))
                .isEqualTo(new FastTakeoverFrame.Collected(0, true, List.of()));
    }

    @Test
    void anENTRYLargerThanAPageGoesAloneOnItsPage() throws Exception {
        Member m = holding(entry(7, A, 10), entry(7, A, 11));

        FenceAnswer answerer = new FenceAnswer(m.journal(), 1);

        assertThat(offsets(answerer.answer(from(9),
                new FastTakeoverFrame.Fence(List.of(7L), 6, 0)))).containsExactly(10L);
        assertThat(offsets(answerer.answer(from(9),
                new FastTakeoverFrame.Fence(List.of(7L), 6, 1)))).containsExactly(11L);
    }

    @Test
    void aFENCENotAboveTheTermsItFencesIsRefused() throws Exception {
        Member m = holding(entry(7, A, 10));

        FastFrame.Body answer = new FenceAnswer(m.journal(), 1 << 20).answer(from(8),
                new FastTakeoverFrame.Fence(List.of(7L, 8L), 6));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(m.journal().held()).as("nothing dropped on its word").hasSize(1);
    }
}
