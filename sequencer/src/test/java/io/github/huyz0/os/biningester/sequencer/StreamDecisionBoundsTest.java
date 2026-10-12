// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * The decision's bounds (M13.33f review round 1, P1-P3, T1-T3): no committed
 * offset is ever voided and every void lies within {@code [c0, c0 + B)} -- an
 * overlap, or an entry reaching past the bound, is refused as a state the
 * protocol cannot produce.
 */
class StreamDecisionBoundsTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final long B = 10;

    private static FastJournalRecord.Entry at(long first, int count, long assignedAfter) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord("d" + first + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(7, S, first, 2, assignedAfter,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first), records);
    }

    private static FastJournalRecord.Entry at(long first, int count) {
        return at(first, count, 0);
    }

    @Test
    void anENTRYEndingExactlyAtTheBoundLeavesNoTailVoid() {
        StreamDecision.Decided d = (StreamDecision.Decided) StreamDecision.decide(S, 10, B,
                List.of(at(10, 4), at(14, 6)), true, true, false);

        assertThat(d.voids()).isEmpty();
        assertThat(d.resumeAt()).isEqualTo(20);
    }

    @Test
    void contiguousENTRIESFromC0LeaveNoHoleToVoid() {
        StreamDecision.Decided d = (StreamDecision.Decided) StreamDecision.decide(S, 10, B,
                List.of(at(10, 2), at(12, 3)), true, true, false);

        assertThat(d.voids()).singleElement().satisfies(v -> {
            assertThat(v.fromOffset()).as("only the tail").isEqualTo(15);
            assertThat(v.toOffsetExclusive()).isEqualTo(20);
        });
    }

    @Test
    void anOVERLAPIsRefusedNeverVoidingACommittedOffset() {
        // 10 to 15, then 12: a void after it would cover 13 and 14
        for (boolean lost : new boolean[] {true, false}) {
            assertThatThrownBy(() -> StreamDecision.decide(S, 10, B,
                    List.of(at(10, 5), at(12, 1)), lost, true, false))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void anENTRYReachingPastTheBoundIsRefused() {
        assertThatThrownBy(() -> StreamDecision.decide(S, 10, B, List.of(at(25, 1)), true, true,
                false)).as("past it").isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> StreamDecision.decide(S, 10, B, List.of(at(18, 3)), true, true,
                false)).as("straddling it").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void twoENTRIESAtOneOffsetDifferingOnlyInAssignedAfterAreRefused() {
        assertThatThrownBy(() -> StreamDecision.decide(S, 10, B,
                List.of(at(10, 1, 0), at(10, 1, 1)), false, true, false))
                .isInstanceOf(IllegalStateException.class);
    }
}
