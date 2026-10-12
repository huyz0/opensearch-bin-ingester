// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The leader backpressures at half its journal's cap (M13.27u; ADR-0081 §5.7,
 * §7): {@code fast.journal.cap} is a holder's, twice the leader's, so a
 * holder's unreleased entries of the current term never exceed one leader
 * cap and stale ones fill the other half.
 *
 * <p>⚠️ **THE LEADER FILLED THE WHOLE CAP**, so a holder already holding
 * stale entries refused its REPLICAs with backpressure, and the leader's
 * batches waited behind a holder the ADR says can never be full of them.
 */
class FastWriteLeaderLeaderShareTest {

    /** The encoded bytes one {@code commit(1, S1)} batch holds in the journal. */
    private static long oneBatch() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();
        return l.journal().heldBytes();
    }

    @Test
    void aBATCHPastHalfTheCapWaitsThoughTheWholeCapHoldsIt() throws Exception {
        long size = oneBatch();
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(2 * size - 1, 1_000);

        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void theBYTESAlreadyHeldCountAgainstTheLeadersHalf() throws Exception {
        // ⚠️ M13.27u review T1: every case started empty, so a check of the
        // batch alone, ignoring what is held, passed them all.
        long size = oneBatch();
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(4 * size - 1, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();

        assertThat(l.leader().commit(B, commit(2, S1), roster(), ALL))
                .as("two batches pass half of a cap that holds four less a byte")
                .isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.cursor().cursor(S1)).isEqualTo(102);
        assertThat(l.journal().held()).hasSize(1);
    }
}
