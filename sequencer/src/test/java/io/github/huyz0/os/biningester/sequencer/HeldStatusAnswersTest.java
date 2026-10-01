// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The leader's HELD_STATUS (ADR-0081 §5 invariant (b), §5.7, §9; M13.26f).
 */
class HeldStatusAnswersTest {

    private static final RunKey A = HeldReportsTest.A;

    private static Roster withDecisions(long epoch, List<Roster.Decision> decisions) {
        Roster base = FastTermStartTest.roster(epoch, -1, "l", false, 0, 0, false);
        return new Roster(epoch, -1, base.leader(), base.members(), base.termRecord(), decisions,
                0, 0, false);
    }

    private static FastFrame.GroupStatus answer(long epoch, long assignedAfter, long lowest,
            long highest, long committedNext, List<Roster> rosters, long closedThrough) {
        FastFrame.Held held = new FastFrame.Held(List.of(new FastFrame.HeldStream(A,
                List.of(new FastFrame.HeldGroup(epoch, assignedAfter, lowest, highest)))));
        return HeldStatusAnswers.answer(held, Map.of(A, committedNext), rosters, closedThrough)
                .streams().get(0).groups().get(0);
    }

    @Test
    void eachSTATUSIsAnsweredFirstMatchFirst() {
        List<Roster> none = List.of();

        assertThat(answer(5, 0, 0, 9, 0, none, 5).status())
                .isEqualTo(FastFrame.Status.CLOSED_TERM);
        assertThat(answer(7, 0, 0, 9, 10, none, 5).status())
                .isEqualTo(FastFrame.Status.COMMITTED);
        assertThat(answer(7, 0, 0, 9, 9, none, 5).status()).as("offset 9 not yet committed")
                .isEqualTo(FastFrame.Status.PENDING);
        assertThat(answer(7, 0, 4, 9, 0,
                List.of(withDecisions(8, List.of(new Roster.Decision(0, A, 4)))), 5).status())
                .isEqualTo(FastFrame.Status.SUPERSEDED);
    }

    @Test
    void aDECISIONSupersedesOnlyLaterTermsOrItsOwnAtOrAboveTheGroupsNumber() {
        Roster sameTerm = withDecisions(7, List.of(new Roster.Decision(3, A, 20)));

        assertThat(answer(7, 2, 0, 30, 0, List.of(sameTerm), 0).supersededFrom())
                .as("assigned after 2 decisions: decision 3 came later").isEqualTo(20);
        assertThat(answer(7, 4, 0, 30, 0, List.of(sameTerm), 0).supersededFrom())
                .as("assigned after decision 3: not superseded by it").isEqualTo(Long.MAX_VALUE);
        assertThat(answer(9, 0, 0, 30, 0, List.of(sameTerm), 0).supersededFrom())
                .as("an earlier term never supersedes a later entry").isEqualTo(Long.MAX_VALUE);
        assertThat(answer(7, 4, 0, 30, 0, List.of(sameTerm,
                withDecisions(8, List.of(new Roster.Decision(0, A, 25),
                        new Roster.Decision(1, A, 22)))), 0).supersededFrom())
                .as("the lowest resume offset of every superseding decision").isEqualTo(22);
    }

    @Test
    void theRELEASEOffsetIsTheCommittedNextOffset() {
        FastFrame.Held held = new FastFrame.Held(List.of(new FastFrame.HeldStream(A,
                List.of(new FastFrame.HeldGroup(7, 0, 0, 9)))));

        assertThat(HeldStatusAnswers.answer(held, Map.of(A, 6L), List.of(), 0).streams().get(0)
                .releaseBelow()).isEqualTo(6);
        assertThat(HeldStatusAnswers.answer(held, Map.of(), List.of(), 0).streams().get(0)
                .releaseBelow()).as("a stream the chain has not committed").isZero();
    }
}
