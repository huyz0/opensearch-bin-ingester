// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An answer may carry a BATCHED delta (M13.34l): a leader's batching window
 * puts other pods' segments in the delta that committed this one, and the
 * answer's "committed or redirected, never both" check reads every segment's
 * runs -- where the delta's single-segment view refused, failing every
 * batched forwarded commit.
 */
class CommitAnswerBatchedDeltaTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);
    private static final RunKey FAST = new RunKey(new UUID(2, 2), 0);

    private static CommitDelta batched() {
        return new CommitDelta(9, List.of(
                new SegmentCommit("seg/mine", List.of(new RunCommit(A, 2, 10))),
                new SegmentCommit("seg/theirs", List.of(new RunCommit(B, 3, 0)))));
    }

    @Test
    void aBATCHEDDeltaIsAnAnswerAndARunInAnyOfItsSegmentsIsNeverAlsoRedirected() {
        assertThat(new CommitAnswer(List.of(batched()), List.of(FAST)).deltas())
                .containsExactly(batched());
        assertThatThrownBy(() -> new CommitAnswer(List.of(batched()), List.of(B)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both committed and redirected");
    }
}
