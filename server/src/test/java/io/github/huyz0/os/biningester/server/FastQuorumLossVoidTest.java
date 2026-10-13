// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Quorum loss voids, and only quorum loss does (SPEC test-plan row 13;
 * ADR-0081 §5, ADR-0001's scoped exception; M13.33v): a {@code q = 1} term
 * whose leader -- the one holder every exposed entry is sure to have -- is
 * gone voids the rest of the stream's bound after what survives; a member
 * gone while the leader answers voids nothing.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastQuorumLossVoidTest {

    @Test
    void aQ1LeadersLossVoidsTheBoundAfterWhatSurvives() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-m", List.of(entry(1, 5))))) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            assertThat(rig.term.committedNext(S))
                    .as("m's entry at 5 committed, then [6, 5 + B) voided as counted gaps")
                    .isEqualTo(5 + FastLeaderTerm.STREAM_BOUND);
        }
    }

    @Test
    void aSTREAMWithNoSurvivingEntryIsVoidedToItsBoundAfterQuorumLoss() throws Exception {
        // ⚠️ M13.33aa: S's exposed entries may all be gone with x, leaving no
        // entry to name it; resumed at 5, their offsets would be assigned again.
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of())) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            assertThat(rig.term.committedNext(S)).as("[5, 5 + B) voided")
                    .isEqualTo(5 + FastLeaderTerm.STREAM_BOUND);
        }
    }

    @Test
    void aMEMBERGoneWhileTheLeaderAnswersVoidsNothing() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-x", List.of(entry(1, 5))))) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-m"))).isTrue();

            assertThat(rig.term.committedNext(S)).as("x's entry at 5, and no void")
                    .isEqualTo(6);
        }
    }

    @Test
    void noPODGoneVoidsNothingEvenWithAHole() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-m", List.of(entry(1, 7))))) {
            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.term.committedNext(S))
                    .as("an entry above a hole at c0 is never exposed, and nothing is lost")
                    .isEqualTo(5);
        }
    }
}
