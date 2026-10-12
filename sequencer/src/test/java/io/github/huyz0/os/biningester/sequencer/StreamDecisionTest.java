// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Each stream decided from its live entries at offsets {@code >= c0}
 * (M13.33f; ADR-0081 §5 step 5): without quorum loss, committed up to the
 * first hole and resumed there; with it, every live entry committed at its
 * offset and every hole up to {@code c0 + B} voided.
 */
class StreamDecisionTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final long B = 10;

    /** An entry of term 7 at {@code first}, {@code count} records long. */
    private static FastJournalRecord.Entry at(long first, int count) {
        return at(7, first, count);
    }

    private static FastJournalRecord.Entry at(long epoch, long first, int count) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord("d" + first + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(epoch, S, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first), records);
    }

    private static List<Long> committed(StreamDecision.Outcome outcome) {
        return ((StreamDecision.Decided) outcome).commits().stream()
                .map(FastJournalRecord.Entry::firstOffset).toList();
    }

    @Test
    void withoutLOSSItCommitsUpToTheFirstHoleAndResumesThere() {
        StreamDecision.Outcome got = StreamDecision.decide(S, 10, B,
                List.of(at(16, 1), at(12, 2), at(10, 2)), false, true, false);

        assertThat(committed(got)).containsExactly(10L, 12L);
        assertThat(((StreamDecision.Decided) got).voids()).isEmpty();
        assertThat(((StreamDecision.Decided) got).resumeAt()).as("the hole at 14").isEqualTo(14);
    }

    @Test
    void anENTRYBelowTheChainsNextOffsetIsSkipped() {
        StreamDecision.Outcome got = StreamDecision.decide(S, 10, B,
                List.of(at(8, 2), at(10, 1)), false, true, false);

        assertThat(committed(got)).containsExactly(10L);
        assertThat(((StreamDecision.Decided) got).resumeAt()).isEqualTo(11);
    }

    @Test
    void anENTRYHeldByManyCountsOnce() {
        StreamDecision.Outcome got = StreamDecision.decide(S, 10, B,
                List.of(at(10, 2), at(10, 2), at(12, 1)), false, true, false);

        assertThat(committed(got)).containsExactly(10L, 12L);
    }

    @Test
    void withoutLOSSAnUnansweredMemberIsWaitedForUnlessCoverageAllows() {
        assertThat(StreamDecision.decide(S, 10, B, List.of(at(10, 1)), false, false, false))
                .isInstanceOf(StreamDecision.Wait.class);
        assertThat(StreamDecision.decide(S, 10, B, List.of(at(10, 1)), false, false, true))
                .as("zone coverage: an early decision").isInstanceOf(StreamDecision.Decided.class);
    }

    @Test
    void withLOSSEveryMemberIsWaitedForEvenUnderCoverage() {
        // a void must not cover a copy on a pod that survives
        assertThat(StreamDecision.decide(S, 10, B, List.of(at(10, 1)), true, false, true))
                .isInstanceOf(StreamDecision.Wait.class);
    }

    @Test
    void withLOSSEveryLiveEntryIsCommittedAndEveryHoleVoidedUpToC0PlusB() {
        StreamDecision.Outcome got = StreamDecision.decide(S, 10, B,
                List.of(at(16, 1), at(12, 2)), true, true, false);

        StreamDecision.Decided decided = (StreamDecision.Decided) got;
        assertThat(committed(got)).as("an exposed entry above a hole survives")
                .containsExactly(12L, 16L);
        assertThat(decided.voids()).containsExactly(new Recovery.VoidRange(S, 10, 12),
                new Recovery.VoidRange(S, 14, 16), new Recovery.VoidRange(S, 17, 20));
        assertThat(decided.resumeAt()).isEqualTo(20);
    }

    @Test
    void withLOSSAndNothingLiveTheWholeBoundIsVoided() {
        StreamDecision.Decided decided = (StreamDecision.Decided) StreamDecision.decide(S, 10, B,
                List.of(), true, true, false);

        assertThat(decided.commits()).isEmpty();
        assertThat(decided.voids()).containsExactly(new Recovery.VoidRange(S, 10, 20));
        assertThat(decided.resumeAt()).isEqualTo(20);
    }

    @Test
    void withoutLOSSAndNothingLiveItResumesAtC0() {
        StreamDecision.Decided decided = (StreamDecision.Decided) StreamDecision.decide(S, 10, B,
                List.of(), false, true, false);

        assertThat(decided.commits()).isEmpty();
        assertThat(decided.voids()).isEmpty();
        assertThat(decided.resumeAt()).isEqualTo(10);
    }

    @Test
    void twoDIFFERENTEntriesAtOneOffsetAreRefused() {
        // a reassignment follows a decision that supersedes the first
        assertThatThrownBy(() -> StreamDecision.decide(S, 10, B,
                List.of(at(7, 10, 1), at(8, 10, 1)), false, true, false))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anENTRYOfAnotherStreamIsRefused() {
        FastJournalRecord.Entry other = new FastJournalRecord.Entry(7, new RunKey(new UUID(2, 2),
                0), 10, 2, 0, new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));

        assertThatThrownBy(() -> StreamDecision.decide(S, 10, B, List.of(other), false, true,
                false)).isInstanceOf(IllegalArgumentException.class);
    }
}
