// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.INDEX;
import static io.github.huyz0.os.biningester.server.TakeoverRig.INDEX2;
import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.S2;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // ⚠️ M13.33u3: SPEC test-plan row 13's other cases.

    @Test
    void aVOIDStartsAtTheChainsNextOffsetNeverAtTheDeadTermsFirstAssigned() throws Exception {
        // the committed-stream case: x's term assigned from 3; 3 and 4 are
        // committed and released, so a void from 3 would void committed offsets
        try (TakeoverRig rig = new TakeoverRig(1, 5,
                Map.of("uid-m", List.of(entry(1, 3), entry(1, 4), entry(1, 5))))) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            assertThat(rig.voids).containsExactly(
                    new Recovery.VoidRange(S, 6, 5 + FastLeaderTerm.STREAM_BOUND));
            assertThat(rig.recovered).as("only 5; 3 and 4 never committed again")
                    .containsExactly("seg-r1:5+1@2");
            assertThat(rig.term.committedNext(S)).isEqualTo(5 + FastLeaderTerm.STREAM_BOUND);
        }
    }

    @Test
    void theTailIsVOIDEDToC0PlusBNeverToLongMaxValue() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5,
                Map.of("uid-m", List.of(entry(1, 5), entry(1, 7))))) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            assertThat(rig.voids).as("the hole at 6, then the tail to the bound only")
                    .containsExactly(new Recovery.VoidRange(S, 6, 7),
                            new Recovery.VoidRange(S, 8, 5 + FastLeaderTerm.STREAM_BOUND));
            assertThat(rig.term.committedNext(S)).isEqualTo(5 + FastLeaderTerm.STREAM_BOUND);
        }
    }

    @Test
    void aSTREAMOfAnIndexThatDidNotLoseQuorumIsNeverVoided() throws Exception {
        // the mixed-q case: x leads a q = 1 and a q = 2 index; its loss alone
        // is the q = 1 index's quorum loss, never the q = 2 index's
        try (TakeoverRig rig = new TakeoverRig(1, 5,
                Map.of("uid-m", List.of(entry(S2, 2, 1, 0))), Map.of(INDEX, 1, INDEX2, 2))) {
            assertThat(rig.takeOver(uid -> uid.equals("uid-x"))).isTrue();

            assertThat(rig.voids).containsExactly(
                    new Recovery.VoidRange(S, 5, 5 + FastLeaderTerm.STREAM_BOUND));
            assertThat(rig.term.committedNext(S2)).as("m's copy committed, nothing voided")
                    .isEqualTo(1);
        }
    }

    @Test
    void aPodOUTOfTheEndpointSliceIsWaitedForNeverTreatedAsGone() throws Exception {
        // ⚠️ x answers nothing but its UID is not deleted: it may hold the only
        // copy, so the takeover waits rather than voids -- then x is back
        AtomicBoolean back = new AtomicBoolean();
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-m", List.of(entry(1, 5))))) {
            Thread takeover = Thread.ofVirtual().start(() -> rig.takeOver(
                    uid -> uid.equals("uid-x") && !back.get(), uid -> false));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (rig.fenced.stream().filter("uid-x"::equals).count() <= FastTakeover.ROUNDS
                    && rig.term.committedNext(S) == 5 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }

            assertThat(rig.fenced).as("a whole collection waited x out")
                    .filteredOn("uid-x"::equals).hasSizeGreaterThan(FastTakeover.ROUNDS);
            assertThat(rig.voids).isEmpty();
            assertThat(rig.term.committedNext(S)).as("nothing decided while x is silent")
                    .isEqualTo(5);

            back.set(true);
            takeover.join(Duration.ofSeconds(20));

            assertThat(takeover.isAlive()).isFalse();
            assertThat(rig.voids).isEmpty();
            assertThat(rig.term.committedNext(S)).as("m's entry at 5, and no void")
                    .isEqualTo(6);
        }
    }
}
