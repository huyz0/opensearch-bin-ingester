// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The commit-log record, tested directly.
 *
 * <p>⚠️ CommitLog refuses an empty commit before it ever builds a delta, so
 * these guards are unreachable from its tests — and the mutation that removed
 * the empty-runs check survived the whole CommitLogTest suite because of it. A
 * validation reachable only through a caller that pre-validates is untested.
 */
class CommitDeltaTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static RunCommit run(int count, long first) {
        return new RunCommit(new RunKey(A, 0), count, first);
    }

    @Test
    void aDeltaWithNoRunsIsRefused() {
        // ⚠️ It would consume a sequence number and commit nothing, so a replay
        // meets a gap it cannot explain — and a gap in the log stalls every
        // consumer of the partition.
        assertThatThrownBy(() -> new CommitDelta(0, "seg", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDeltaNamesASegmentAndANonNegativeSequence() {
        assertThatThrownBy(() -> new CommitDelta(0, "", List.of(run(1, 0))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommitDelta(-1, "seg", List.of(run(1, 0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aRunCommitCoversAtLeastOneRecordAtANonNegativeOffset() {
        assertThatThrownBy(() -> run(0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> run(1, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(run(3, 10).lastOffset()).as("inclusive").isEqualTo(12);
        assertThat(run(1, 10).lastOffset()).isEqualTo(10);
    }

    @Test
    void theRunsListIsCopiedSoALaterMutationCannotChangeTheDelta() {
        var mutable = new java.util.ArrayList<>(List.of(run(1, 0)));
        CommitDelta d = new CommitDelta(0, "seg", mutable);
        mutable.add(run(1, 5));
        assertThat(d.runs()).hasSize(1);
        assertThatThrownBy(() -> d.runs().add(run(1, 9)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
