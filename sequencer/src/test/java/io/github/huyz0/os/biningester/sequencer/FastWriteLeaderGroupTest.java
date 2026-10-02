// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The group fsync, the exact bounds, and which REPLICAs go where (M13.27c
 * review round 1, P1, P2, T1-T3).
 */
class FastWriteLeaderGroupTest {

    /** Counts forces, and fails them while {@code failing}. */
    static final class CountingFile implements JournalFile {
        final MemoryJournalFile inner = new MemoryJournalFile();
        int forces;
        boolean failing;

        @Override
        public byte[] readAll() {
            return inner.readAll();
        }

        @Override
        public void append(byte[] bytes) {
            inner.append(bytes);
        }

        @Override
        public void force() throws IOException {
            if (failing) {
                throw new IOException("disk gone");
            }
            forces++;
            inner.force();
        }

        @Override
        public void truncate(long length) {
            inner.truncate(length);
        }

        @Override
        public void replace(byte[] contents) {
            inner.replace(contents);
        }

        @Override
        public long size() {
            return inner.size();
        }

        @Override
        public void close() {
        }
    }

    private static FastWriteLeader leader(JournalFile file, long cap, long bound)
            throws Exception {
        FastWriteLeader leader = new FastWriteLeader(7, FastWriteLeaderTest.LEADER,
                new FastCursor(bound),
                new TermRecordWriter(new MemoryBinStore(), "p", FastWriteLeaderTest.roster(),
                        new Mono(), Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), FastJournal.recover(file, cap), () -> 3);
        leader.open(FastWriteLeaderTest.S1, 100);
        leader.open(FastWriteLeaderTest.S2, 0);
        return leader;
    }

    @Test
    void anANSWERNeverPrecedesASuccessfulFsync() throws Exception {
        CountingFile file = new CountingFile();
        FastWriteLeader l = leader(file, 1 << 20, 1_000);
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        file.failing = true;

        assertThatThrownBy(l::flushGroup).isInstanceOf(IOException.class);

        assertThat(l.expose()).as("the leader's copy never counted").isEmpty();
        file.failing = false;
        assertThatThrownBy(l::flushGroup)
                .as("a failed fsync ends the journal (ADR-0083): recovered, never retried")
                .isInstanceOf(IllegalStateException.class);
        assertThat(l.expose()).isEmpty();
    }

    @Test
    void aGROUPOfCommitsTakesOneFsyncAndACommitNone() throws Exception {
        CountingFile file = new CountingFile();
        FastWriteLeader l = leader(file, 1 << 20, 1_000);
        int before = file.forces;
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S2),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        assertThat(file.forces).isEqualTo(before);

        assertThat(l.flushGroup()).hasSize(2);

        assertThat(file.forces - before).isEqualTo(1);
    }

    @Test
    void aRUNOfExactlyBFillingItsStreamExactlyIsAssigned() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, 2);

        assertThat(l.commit(FastWriteLeaderTest.B,
                FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL)).isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void aBATCHFillingTheJournalExactlyToItsCapIsAssigned() throws Exception {
        FastWriteFrame.Commit commit = FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S1);
        long size = new FastJournalRecord.Entry(7, FastWriteLeaderTest.S1, 100, 1, 3, commit.key(),
                commit.runs().get(0).records()).encode().length;
        FastWriteLeader l = leader(new MemoryJournalFile(), size, 1_000);

        assertThat(l.commit(FastWriteLeaderTest.B, commit, FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL)).isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void aCROSSZoneWritersOwnCopyCoversItsZoneSoNoReplicaGoesThere() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, 1_000);
        l.commit(FastWriteLeaderTest.B, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        l.flushGroup();

        List<FastWriteLeader.ReplicaSend> sends = l.replicas(List.of(FastWriteLeaderTest.WA,
                FastWriteLeaderTest.B, FastWriteLeaderTest.C));

        assertThat(sends).as("q = 2: the leader's zone and the writer's own copy").isEmpty();
    }

    @Test
    void twoBATCHESInOneGroupGoInTwoReplicas() throws Exception {
        FastWriteLeader l = leader(new MemoryJournalFile(), 1 << 20, 1_000);
        l.commit(FastWriteLeaderTest.WA, FastWriteLeaderTest.commit(1, FastWriteLeaderTest.S2),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        l.commit(FastWriteLeaderTest.WA, FastWriteLeaderTest.commit(2, FastWriteLeaderTest.S2),
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL);
        l.flushGroup();

        List<FastWriteLeader.ReplicaSend> sends = l.replicas(List.of(FastWriteLeaderTest.B));

        assertThat(sends).hasSize(2);
        assertThat(sends).allSatisfy(s -> assertThat(s.replica().entries()).hasSize(1));
        assertThat(sends).extracting(s -> s.replica().entries().get(0).idempotencyKey().fastSeq())
                .containsExactly(1L, 2L);
        Holder holder = sends.get(0).holder();
        assertThat(holder).isEqualTo(FastWriteLeaderTest.B);
    }
}
