// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * One REPLICA per batch per holder however many runs, a declined copy
 * replaced elsewhere, exposure once, a
 * single withdrawal, committed batches dropped, and a fresh assignedAfter
 * per batch (M13.27c review round 3, T7-T11, P8; carried by M13.27e).
 */
class FastWriteLeaderBatchTest {

    private static final UUID Q3 = new UUID(1, 3);
    private static final RunKey S3 = new RunKey(Q3, 0);

    private static Roster rosterWithTwoQTwoIndices() {
        Roster base = FastWriteLeaderTest.roster();
        return new Roster(7, -1, base.leader(), base.members(),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(FastWriteLeaderTest.Q1, 1,
                        FastWriteLeaderTest.Q2, 2, Q3, 2)))), List.of(), 0, 0, false);
    }

    private static FastWriteLeader leader(JournalFile file, long cap, AtomicLong decisions)
            throws Exception {
        FastWriteLeader leader = new FastWriteLeader(7, FastWriteLeaderTest.LEADER,
                new FastCursor(1_000),
                new TermRecordWriter(new MemoryBinStore(), "p", rosterWithTwoQTwoIndices(),
                        new Mono(), Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), FastJournal.recover(file, cap),
                decisions::get);
        leader.open(FastWriteLeaderTest.S1, 100);
        leader.open(FastWriteLeaderTest.S2, 0);
        leader.open(S3, 0);
        return leader;
    }

    @Test
    void aBATCHOfTwoRunsGoesToAHolderInOneReplica() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, new AtomicLong(3));
        l.commit(FastWriteLeaderTest.WA, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2, S3),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL);
        l.flushGroup();

        List<FastWriteLeader.ReplicaSend> sends = l.replicas(List.of(FastWriteLeaderTest.B));

        assertThat(sends).as("one frame per batch per holder, never one per partition")
                .hasSize(1);
        assertThat(sends.get(0).replica().entries()).hasSize(2);
    }

    @Test
    void aDECLINEDCopyIsReplacedInAnotherPod() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, new AtomicLong(3));
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL);
        l.flushGroup();
        l.confirm(FastWriteLeaderTest.B, new FastWriteFrame.Confirm(
                FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(FastWriteLeaderTest.S2, 0)), false));

        List<FastWriteLeader.ReplicaSend> sends = l.replicas(List.of(FastWriteLeaderTest.WA,
                FastWriteLeaderTest.B, FastWriteLeaderTest.C));

        assertThat(sends).extracting(FastWriteLeader.ReplicaSend::holder)
                .as("never back to the writer that declined it").containsExactly(FastWriteLeaderTest.C);
    }

    @Test
    void anEXPOSEDBatchIsExposedOnce() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, new AtomicLong(3));
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL);
        l.flushGroup();
        assertThat(l.expose()).hasSize(1);

        assertThat(l.expose()).isEmpty();
    }

    @Test
    void aWITHDRAWALTakesOnlyThatPodsAsk() {
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");
        f.open(FastWriteLeaderTest.S1, 0);
        EntryId id = new EntryId(7, 0, FastWriteLeaderTest.S1, 0);
        f.assigned(id, 1, 3);
        Holder b = FastWriteLeaderTest.B;
        Holder c = FastWriteLeaderTest.C;
        f.asked(id, b);
        f.asked(id, c);

        f.withdraw(id, "uid-b");

        assertThat(f.holdersFor(id, List.of(b, c, new Holder("uid-b2", "az-b"))))
                .as("c's ask still covers az-c").containsExactly(new Holder("uid-b2", "az-b"));
    }

    @Test
    void aCOMMITTEDBatchIsNoLongerHeld() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, new AtomicLong(3));
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL);
        l.flushGroup();

        assertThat(l.heldBatches()).isEqualTo(1);

        l.committed(FastWriteLeaderTest.S2, 2);

        assertThat(l.heldBatches()).as("dropped, never held for ever").isZero();
    }

    @Test
    void eachBATCHTakesTheDecisionNumberCurrentAtItsAssignment() throws Exception {
        AtomicLong decisions = new AtomicLong(3);
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, decisions);
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL);
        decisions.set(5);

        assertThat(l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S1),
                rosterWithTwoQTwoIndices(), FastWriteLeaderTest.ALL))
                .isEqualTo(new FastWriteLeader.Pending(5));
    }
}
