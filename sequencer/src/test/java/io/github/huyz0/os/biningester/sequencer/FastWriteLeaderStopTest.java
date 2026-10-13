// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.WA;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * A leader shutting down FIRST stops exposing as well as assigning (ADR-0081
 * §9; M13.35a): a CONFIRM arriving after must not be answered as complete,
 * and it departs only with no entry it exposed left uncommitted.
 */
class FastWriteLeaderStopTest {

    @Test
    void aSTOPPEDLeaderAssignsNothing() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        l.stop();

        assertThat(l.commit(WA, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
    }

    @Test
    void aSTOPPEDLeaderExposesNothing() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();
        l.stop();

        assertThat(l.expose()).as("exposable at q = 1, but stopped first").isEmpty();
    }

    @Test
    void aSTOPReportsAnEntryExposedAndNotYetCommitted() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();
        assertThat(l.expose()).hasSize(1);

        assertThat(l.stop()).as("the exposed entry is not committed").isTrue();
    }

    @Test
    void aSTOPReportsNothingOnceTheExposedEntryIsCommittedOrWhenNoneWasExposed()
            throws Exception {
        FastWriteLeader committed = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        committed.commit(WA, commit(1, S1), roster(), ALL);
        committed.flushGroup();
        committed.expose();
        committed.committed(S1, 102);
        FastWriteLeader unexposed = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        unexposed.commit(WA, commit(1, S1), roster(), ALL);
        unexposed.flushGroup();

        assertThat(committed.stop()).as("committed").isFalse();
        assertThat(unexposed.stop()).as("never exposed: its writer was never acked").isFalse();
    }
}
