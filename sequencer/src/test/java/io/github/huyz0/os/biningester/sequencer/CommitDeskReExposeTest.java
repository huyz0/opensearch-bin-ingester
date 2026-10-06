// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A retried batch already exposed is answered ASSIGNED and owed its EXPOSED
 * again (M13.27s review round 1, P1): its writer lost the first, and the
 * leader names each batch only once on its own.
 */
class CommitDeskReExposeTest {

    @Test
    void aRETRIEDExposedBatchIsOwedItsEXPOSEDAgain() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = new CommitDesk(l.leader(), stream -> 0L, Duration.ofSeconds(10));
        desk.answer(B, commit(1, S1), roster(), ALL);
        FastWriteLeader.Exposure exposed = new FastWriteLeader.Exposure("uid-b",
                new FastWriteFrame.Exposed(3, List.of(new FastWriteFrame.RunAt(S1, 100))));
        assertThat(desk.exposures()).as("the premise: exposed once").containsExactly(exposed);
        assertThat(desk.exposures()).as("and only once on its own").isEmpty();

        desk.answer(B, commit(1, S1), roster(), ALL);

        assertThat(desk.exposures()).as("the retry is owed it again").containsExactly(exposed);
    }
}
