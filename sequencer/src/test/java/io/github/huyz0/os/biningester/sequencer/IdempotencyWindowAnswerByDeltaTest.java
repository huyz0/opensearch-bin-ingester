// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a commit answer may list (M13.34c review round 1, T2): never a segment
 * with a run uncommitted -- refused, naming it, not answered with what is
 * there.
 */
class IdempotencyWindowAnswerByDeltaTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);

    @Test
    void aSEGMENTWithARunUncommittedIsRefusedNamingIt() {
        IdempotencyWindow window = new IdempotencyWindow(new MemoryBinStore(), "p");
        window.applied(new CommitRequest("pod", "i1", 5, "seg/a", Map.of(A, 2)), 1, 0);

        assertThatThrownBy(() -> window.answerByDelta(
                new CommitRequest("pod", "i1", 5, "seg/a", Map.of(A, 2, B, 1))))
                .isInstanceOfSatisfying(IdempotencyWindow.IncompleteReplayException.class,
                        e -> assertThat(e.missing()).containsExactly(B));
    }
}
