// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import org.junit.jupiter.api.Test;

/**
 * A fast COMMIT's key names its sender's pod (ADR-0084 decision 8; M13.52g):
 * a rostered pod committing under another's key would collide with that
 * pod's dedupe.
 */
class FastLeaderTermCommitKeyTest {

    @Test
    void aCOMMITUnderAnotherPodsKeyIsRefusedAndAssignsNothing() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        FastLeaderTermCommitTest.join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(
                FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit("another", 1, S1), FastLeaderTermCommitTest.recorded(l));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(((FastFrame.Refused) answer).text()).contains("another");
        assertThat(l.journal().held()).as("nothing assigned").isEmpty();
    }
}
