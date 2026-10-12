// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.C;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A CONFIRM counts only the sender's own batch under its key (M13.27v review
 * round 1, P1): {@code assignedAfter} is shared by every batch between two
 * decisions, so it alone names no batch.
 */
class FastWriteLeaderConfirmBindingTest {

    /** B's batch on S2 at {@code wal_quorum 2}, assigned after decision 3, B's copy required. */
    private static FastWriteLeaderTest.Leader assignedToB() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S2), roster(), ALL);
        l.leader().flushGroup();
        return l;
    }

    @Test
    void aCONFIRMNamingAnotherWritersEntryCountsNothing() throws Exception {
        FastWriteLeaderTest.Leader l = assignedToB();

        // C, in az-c, under its own key: counted, B's entry had a second zone
        l.leader().confirm(C, new FastWriteFrame.Confirm(commit("c", 1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), true));

        assertThat(l.leader().expose()).as("C holds no copy of B's entry").isEmpty();
    }

    @Test
    void theWRITERsCONFIRMUnderAnotherOfItsKeysCountsNothing() throws Exception {
        FastWriteLeaderTest.Leader l = assignedToB();

        l.leader().confirm(B, new FastWriteFrame.Confirm(commit(2, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), true));

        assertThat(l.leader().expose()).as("batch 2 was never assigned").isEmpty();
    }
}
