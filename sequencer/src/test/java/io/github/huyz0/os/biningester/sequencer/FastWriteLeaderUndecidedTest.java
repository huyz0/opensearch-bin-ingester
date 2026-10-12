// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A stream a takeover has not decided is assigned nothing (M13.33l; ADR-0081
 * §5 invariant d): assignment resumes only once its chain entry and its
 * decision are durable -- an entry assigned before then could sit at an
 * offset the decision frees, and be superseded by the decision that freed it.
 */
class FastWriteLeaderUndecidedTest {

    @Test
    void aCOMMITOnAnUndecidedStreamWaitsAndTakesNoOffset() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().undecided(List.of(S1));

        FastWriteLeader.Outcome got = l.leader().commit(B, commit(1, S1), roster(), ALL);

        assertThat(got).isInstanceOfSatisfying(FastWriteLeader.Wait.class,
                w -> assertThat(w.why()).contains("decision"));
        assertThat(l.cursor().cursor(S1)).isEqualTo(100);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void onceDECIDEDTheSameCommitIsAssigned() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().undecided(List.of(S1));
        l.leader().commit(B, commit(1, S1), roster(), ALL);

        l.leader().decided(S1);

        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void aBATCHWithOneUndecidedRunWaitsWhole() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().undecided(List.of(S2));

        assertThat(l.leader().commit(B, commit(1, S1, S2), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.cursor().cursor(S1)).as("nothing of the batch assigned").isEqualTo(100);
        assertThat(l.journal().held()).as("nothing of the batch journalled").isEmpty();
    }

    @Test
    void decidingONEStreamKeepsEveryOtherHeld() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().undecided(List.of(S1, S2));

        l.leader().decided(S1);

        assertThat(l.leader().commit(B, commit(2, S2), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void anotherSTREAMIsAssignedMeanwhile() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().undecided(List.of(S2));

        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Pending.class);
    }
}
