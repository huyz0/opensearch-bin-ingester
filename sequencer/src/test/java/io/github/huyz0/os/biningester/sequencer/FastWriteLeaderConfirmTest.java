// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A writer that did not journal its copy, an idle flush, and a partly
 * committed batch's retry (M13.27c review round 2, P5, T5, T6).
 */
class FastWriteLeaderConfirmTest {

    @Test
    void aWRITERThatDidNotJournalItsCopyIsReplaced() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().commit(FastWriteLeaderTest.B,
                FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL);
        l.leader().flushGroup();

        l.leader().confirm(FastWriteLeaderTest.B, new FastWriteFrame.Confirm(
                FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(FastWriteLeaderTest.S2, 0)), false));

        assertThat(l.leader().replicas(List.of(FastWriteLeaderTest.WA, FastWriteLeaderTest.B,
                FastWriteLeaderTest.C))).as("the copy is sent to a holder, never awaited").hasSize(1);
    }

    @Test
    void anIDLEFlushFsyncsNothing() throws Exception {
        FastWriteLeaderGroupTest.CountingFile file = new FastWriteLeaderGroupTest.CountingFile();
        FastWriteLeader l = new FastWriteLeader(7, FastWriteLeaderTest.LEADER, new FastCursor(1_000),
                new TermRecordWriter(new io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore(),
                        "p", FastWriteLeaderTest.roster(), new FastLeaseFenceHolderTest.Mono(),
                        java.time.Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), FastJournal.recover(file, 1 << 20), () -> 3);
        int before = file.forces;

        assertThat(l.flushGroup()).isEmpty();

        assertThat(file.forces).isEqualTo(before);
    }

    @Test
    void aBATCHWithOneRunCommittedIsStillHeldForItsRetry() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        l.leader().commit(FastWriteLeaderTest.WA, FastWriteLeaderTest.commit(1,
                FastWriteLeaderTest.S1, FastWriteLeaderTest.S2), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL);
        l.leader().flushGroup();
        l.leader().replicas(List.of(FastWriteLeaderTest.B)).forEach(send ->
                l.leader().replicaAck(send.holder(), new FastWriteFrame.ReplicaAck(3,
                        send.replica().entries().stream().map(e ->
                                new FastWriteFrame.RunAt(e.key(), e.firstOffset())).toList())));
        assertThat(l.leader().expose()).hasSize(1);

        l.leader().committed(FastWriteLeaderTest.S1, 102);

        assertThat(l.leader().commit(FastWriteLeaderTest.WA, FastWriteLeaderTest.commit(1,
                FastWriteLeaderTest.S1, FastWriteLeaderTest.S2), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL)).as("S2's run is not committed: still held")
                .isInstanceOf(FastWriteLeader.Answered.class);
    }
}
