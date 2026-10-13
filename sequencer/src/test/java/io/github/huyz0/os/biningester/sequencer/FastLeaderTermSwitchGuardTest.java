// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.term3;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A switch pauses only a stream the catalog no longer has fast (M13.34s
 * review round 1, P1): a default-path request naming a fast stream with a
 * live tail -- redirected, never committed -- must not pause it, or its
 * unexposed tail is discarded though the index never left fast mode.
 */
class FastLeaderTermSwitchGuardTest {

    @Test
    void aSTREAMStillFastIsNeverPausedOneTurnedDefaultIs() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term);
        term.catalogSays(Map.of(Q1, 1));
        term.flushTermRecord();
        CommitDesk desk = term.commitDesk(FastJournal.recover(new MemoryJournalFile(), 1 << 20),
                FastTermStartTest.SELF);
        assertThat(term.answerCommit(header(3, POD), commit(POD.podId(), 1, S1), desk))
                .as("the premise: a fast tail").isInstanceOf(FastWriteFrame.Assigned.class);

        term.switchPause(S1);
        assertThat(term.holdsDefault(S1)).as("still fast: not paused").isFalse();

        term.catalogSays(Map.of());
        term.switchPause(S1);
        assertThat(term.holdsDefault(S1)).as("turned default: paused").isTrue();
    }
}
