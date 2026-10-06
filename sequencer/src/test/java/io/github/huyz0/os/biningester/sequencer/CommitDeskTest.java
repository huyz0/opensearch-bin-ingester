// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * A COMMIT answered at the leader (M13.27s): ASSIGNED once the group holding
 * it is fsynced, each writer its own answer, backpressure as a refusal.
 */
class CommitDeskTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @Test
    void aRECORDEDCommitIsAnsweredASSIGNEDOnceItsGroupIsJournaled() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = new CommitDesk(l.leader(), stream -> 0L, WAIT);

        FastFrame.Body answer = desk.answer(B, commit(1, S1), roster(), ALL);

        assertThat(answer).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(S1, 100, 1, false))));
        assertThat(l.journal().held()).as("journaled before it was answered").hasSize(1);
    }

    @Test
    void anUNRECORDEDIndexIsRefusedAsBackpressure() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = new CommitDesk(l.leader(), stream -> 0L, WAIT);
        RunKey unrecorded = new RunKey(new UUID(5, 5), 0);

        FastFrame.Body answer = desk.answer(B, commit(1, unrecorded), roster(), ALL);

        assertThat(answer).isInstanceOfSatisfying(FastFrame.Refused.class, refused ->
                assertThat(refused.reason()).isEqualTo(FastFrame.Reason.BACKPRESSURE));
        assertThat(l.journal().held()).as("nothing assigned").isEmpty();
    }

    @Test
    void aSTREAMFirstCommittedHereStartsAtItsCommittedNextOffset() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        RunKey fresh = new RunKey(Q1, 3);
        CommitDesk desk = new CommitDesk(l.leader(),
                stream -> stream.equals(fresh) ? 500L : 0L, WAIT);

        FastFrame.Body answer = desk.answer(B, commit(1, fresh), roster(), ALL);

        assertThat(answer).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(fresh, 500, 1, false))));
    }

    @Test
    void COMMITSInOneGroupEachGetTheirOwnAnswer() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = new CommitDesk(l.leader(), stream -> 0L, WAIT);
        int writers = 8;
        List<Callable<FastFrame.Body>> calls = new ArrayList<>();
        for (int i = 1; i <= writers; i++) {
            long seq = i;
            calls.add(() -> desk.answer(B, commit(seq, S1), roster(), ALL));
        }
        List<Long> firsts = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Future<FastFrame.Body> each : pool.invokeAll(calls)) {
                FastWriteFrame.Assigned assigned = (FastWriteFrame.Assigned) each.get();
                firsts.add(assigned.runs().get(0).firstOffset());
            }
        }

        // ⚠️ EACH ITS OWN: two records a batch, so the batches' first offsets
        // are 100, 102, ... -- an answer handed to the wrong writer repeats one.
        assertThat(firsts).as("each writer answered its own batch's offsets")
                .doesNotHaveDuplicates().hasSize(writers)
                .allMatch(first -> first >= 100 && first < 100 + 2 * writers
                        && (first - 100) % 2 == 0);
        assertThat(l.journal().held()).hasSize(writers);
    }

    @Test
    void anEXPOSEDBatchRetriedIsAnsweredFromItsOffsets() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = new CommitDesk(l.leader(), stream -> 0L, WAIT);
        FastFrame.Body first = desk.answer(B, commit(1, S1), roster(), ALL);
        l.leader().expose();

        assertThat(desk.answer(B, commit(1, S1), roster(), ALL)).isEqualTo(first);
        assertThat(l.journal().held()).as("not assigned again").hasSize(1);
    }
}
