// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader's side of a fast write (ADR-0081 §2; M13.27c).
 */
class FastWriteLeaderTest {

    static final UUID Q1 = new UUID(1, 1);
    static final UUID Q2 = new UUID(1, 2);
    static final RunKey S1 = new RunKey(Q1, 0);
    static final RunKey S2 = new RunKey(Q2, 0);
    static final Roster.Incarnation LEADER = new Roster.Incarnation("l", "uid-l", "az-a", "");
    static final Holder L = new Holder("uid-l", "az-a");
    static final Holder WA = new Holder("uid-wa", "az-a");
    static final Holder B = new Holder("uid-b", "az-b");
    static final Holder C = new Holder("uid-c", "az-c");
    static final Set<String> ALL = Set.of("uid-l", "uid-wa", "uid-b", "uid-c");

    static Roster roster() {
        List<Roster.Member> members = new ArrayList<>();
        members.add(new Roster.Member(LEADER, Roster.State.ROSTERED));
        for (Holder h : List.of(WA, B, C)) {
            members.add(new Roster.Member(new Roster.Incarnation(h.podUid(), h.podUid(), h.az(),
                    ""), Roster.State.ROSTERED));
        }
        return new Roster(7, -1, LEADER, members,
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(Q1, 1, Q2, 2)))),
                List.of(), 0, 0, false);
    }

    record Leader(FastWriteLeader leader, FastJournal journal, FastCursor cursor) {
    }

    static Leader leader(long journalCap, long bound) throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), journalCap);
        FastCursor cursor = new FastCursor(bound);
        FastWriteLeader leader = new FastWriteLeader(7, LEADER, cursor,
                new TermRecordWriter(new MemoryBinStore(), "p", roster(), new Mono(),
                        Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), journal, () -> 3);
        leader.open(S1, 100);
        leader.open(S2, 0);
        return new Leader(leader, journal, cursor);
    }

    static FastWriteFrame.Commit commit(long seq, RunKey... streams) {
        List<FastWriteFrame.CommitRun> runs = new ArrayList<>();
        for (RunKey s : streams) {
            runs.add(new FastWriteFrame.CommitRun(s, List.of(
                    new SegmentRecord("d" + seq, OpType.INDEX, OptionalLong.empty(), new byte[] {1}),
                    new SegmentRecord("e" + seq, OpType.INDEX, OptionalLong.empty(),
                            new byte[] {2}))));
        }
        return new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("pod",
                new UUID(9, 9), seq), runs);
    }

    @Test
    void aCOMMITIsAnsweredOnlyAfterItsGroupsFsyncAndExposedAtQOne() throws Exception {
        Leader l = leader(1 << 20, 1_000);

        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isEqualTo(new FastWriteLeader.Pending(3));
        assertThat(l.leader().expose()).as("nothing counted before the fsync").isEmpty();
        List<FastWriteLeader.Answer> answers = l.leader().flushGroup();

        assertThat(answers).containsExactly(new FastWriteLeader.Answer("uid-b",
                commit(1, S1).key(), new FastWriteFrame.Assigned(3, List.of(
                        new FastWriteFrame.AssignedRun(S1, 100, 1, false)))));
        assertThat(l.journal().held()).hasSize(1);
        assertThat(l.leader().expose()).containsExactly(new FastWriteLeader.Exposure("uid-b",
                new FastWriteFrame.Exposed(3, List.of(new FastWriteFrame.RunAt(S1, 100)))));
    }

    @Test
    void atQTwoAWriterElsewhereMustConfirmItsCopy() throws Exception {
        Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S2), roster(), ALL);
        FastWriteFrame.Assigned assigned = l.leader().flushGroup().get(0).assigned();
        assertThat(assigned.runs().get(0).copyRequired()).isTrue();
        assertThat(l.leader().expose()).isEmpty();

        l.leader().confirm(B, new FastWriteFrame.Confirm(commit(1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), false));
        assertThat(l.leader().expose()).as("not journaled with its offsets").isEmpty();
        l.leader().confirm(B, new FastWriteFrame.Confirm(commit(1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), true));

        assertThat(l.leader().expose()).hasSize(1);
    }

    @Test
    void atQTwoAWriterInTheLeadersZoneNeedsAReplicaElsewhere() throws Exception {
        Leader l = leader(1 << 20, 1_000);
        l.leader().commit(WA, commit(1, S2), roster(), ALL);
        assertThat(l.leader().flushGroup().get(0).assigned().runs().get(0).copyRequired())
                .isFalse();

        List<FastWriteLeader.ReplicaSend> sends = l.leader().replicas(List.of(WA, B, C));

        assertThat(sends).hasSize(1);
        assertThat(sends.get(0).holder()).isEqualTo(B);
        assertThat(sends.get(0).replica().entries()).hasSize(1);
        assertThat(l.leader().replicas(List.of(WA, B, C))).as("asked once").isEmpty();
        l.leader().replicaAck(B, new FastWriteFrame.ReplicaAck(3,
                List.of(new FastWriteFrame.RunAt(S2, 0))));
        assertThat(l.leader().expose()).hasSize(1);
    }

    @Test
    void aBATCHWaitsWholeAndTakesNoOffsetUntilEveryRunFits() throws Exception {
        Leader l = leader(1 << 20, 3);
        UUID unknown = new UUID(5, 5);
        RunKey stranger = new RunKey(unknown, 0);
        l.leader().open(stranger, 0);

        assertThat(l.leader().commit(B, commit(1, S1, stranger), roster(), ALL))
                .as("no wal_quorum recorded").isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.leader().commit(B, commit(2, S2), roster(), Set.of("uid-l", "uid-wa")))
                .as("one zone available for q = 2").isInstanceOf(FastWriteLeader.Wait.class);
        l.leader().commit(B, commit(3, S1), roster(), ALL);
        assertThat(l.leader().commit(B, commit(4, S2, S1), roster(), ALL))
                .as("S1 has 1 of B = 3 left").isInstanceOf(FastWriteLeader.Wait.class);

        assertThat(l.cursor().cursor(S2)).as("S2 took nothing while S1 waited").isZero();
        assertThat(l.cursor().cursor(S1)).isEqualTo(102);
    }

    @Test
    void aFULLJournalHoldsTheBatchUnassigned() throws Exception {
        Leader l = leader(10, 1_000);

        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
        assertThat(l.cursor().cursor(S1)).isEqualTo(100);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void aRETRYIsAnsweredFromItsOffsetsOnlyOnceExposedAndHeld() throws Exception {
        Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S2), roster(), ALL);
        l.leader().flushGroup();
        assertThat(l.leader().commit(B, commit(1, S2), roster(), ALL))
                .as("unexposed: assigned anew").isInstanceOf(FastWriteLeader.Pending.class);
        assertThat(l.cursor().cursor(S2)).isEqualTo(4);

        l.leader().commit(B, commit(2, S1), roster(), ALL);
        FastWriteFrame.Assigned first = l.leader().flushGroup().get(1).assigned();
        FastWriteFrame.Exposed exposed = l.leader().expose().get(0).exposed();
        assertThat(l.leader().commit(B, commit(2, S1), roster(), ALL))
                .as("its offsets, and its EXPOSED again in case that was lost")
                .isEqualTo(new FastWriteLeader.Answered(first, exposed));

        l.leader().committed(S1, 102);
        assertThat(l.leader().commit(B, commit(2, S1), roster(), ALL))
                .as("committed and no longer held: assigned anew")
                .isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void aBATCHIsExposedOnlyOnceEveryRunIs() throws Exception {
        Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1, S2), roster(), ALL);
        l.leader().flushGroup();

        assertThat(l.leader().expose()).as("S1 at q = 1 is exposed, S2 at q = 2 is not")
                .isEmpty();
        l.leader().confirm(B, new FastWriteFrame.Confirm(commit(1, S1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), true));
        assertThat(l.leader().expose()).hasSize(1);
    }

    @Test
    void aCOMMITNamingAStreamTwiceIsRefused() throws Exception {
        Leader l = leader(1 << 20, 1_000);

        assertThatThrownBy(() -> l.leader().commit(B, commit(1, S1, S1), roster(), ALL))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
