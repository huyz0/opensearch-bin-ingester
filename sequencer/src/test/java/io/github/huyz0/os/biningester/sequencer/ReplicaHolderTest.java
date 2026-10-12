// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A holder journals a REPLICA from the leader of a term it joined, fsyncs,
 * and answers REPLICA_ACK (M13.28a; ADR-0081 §2.4): refused from anyone
 * else, while departing, and past its cap -- keeping nothing of a refused
 * batch -- and a re-sent REPLICA is acked again, never journaled twice.
 *
 * <p>⚠️ **NOTHING ANSWERED A REPLICA**, so every copy the leader asked of a
 * holder was a 501, and an index at {@code wal_quorum 2} whose writer shares
 * the leader's zone never reached its quorum.
 */
class ReplicaHolderTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);

    private static FastJournalRecord.Entry entry(long epoch, RunKey key, long first) {
        return new FastJournalRecord.Entry(epoch, key, first, 2, 4,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    private static FastFrame.Header from(String sender, long epoch) {
        return new FastFrame.Header(FastWriteFrame.KIND_REPLICA, epoch, sender, "uid-h");
    }

    private static JoinedTerms joined3() {
        JoinedTerms terms = new JoinedTerms();
        terms.joined(3, 0, "uid-l");
        return terms;
    }

    private record Holder(ReplicaHolder holder, FastJournal journal,
            FastWriteLeaderGroupTest.CountingFile file, HolderGate gate) {
    }

    private static Holder holder(long cap) throws Exception {
        FastWriteLeaderGroupTest.CountingFile file = new FastWriteLeaderGroupTest.CountingFile();
        FastJournal journal = FastJournal.recover(file, cap);
        HolderGate gate = new HolderGate();
        return new Holder(new ReplicaHolder(journal, joined3(), gate), journal, file, gate);
    }

    private static FastWriteFrame.Replica replica(long epoch) {
        return new FastWriteFrame.Replica(List.of(entry(epoch, A, 10), entry(epoch, B, 0)));
    }

    @Test
    void aREPLICAFromItsTermsLeaderIsJournaledSyncedAndAcked() throws Exception {
        Holder h = holder(1 << 20);
        int forces = h.file().forces;

        FastFrame.Body answer = h.holder().answer(from("uid-l", 3), replica(3));

        assertThat(answer).isEqualTo(new FastWriteFrame.ReplicaAck(4, List.of(
                new FastWriteFrame.RunAt(A, 10), new FastWriteFrame.RunAt(B, 0))));
        assertThat(h.journal().held()).extracting(FastJournalRecord.Entry::firstOffset)
                .containsExactlyInAnyOrder(10L, 0L);
        assertThat(h.file().forces).as("fsynced before the ack").isGreaterThan(forces);
    }

    @Test
    void aTERMThisPodHasNotJoinedIsRefusedNotRostered() throws Exception {
        Holder h = holder(1 << 20);

        FastFrame.Body answer = h.holder().answer(from("uid-l", 4), replica(4));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(h.journal().held()).isEmpty();
    }

    @Test
    void aSENDERThatIsNotTheTermsLeaderIsRefused() throws Exception {
        Holder h = holder(1 << 20);

        FastFrame.Body answer = h.holder().answer(from("uid-x", 3), replica(3));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(h.journal().held()).isEmpty();
    }

    @Test
    void anENTRYOfAnotherTermThanItsFrameIsRefused() throws Exception {
        Holder h = holder(1 << 20);

        FastFrame.Body answer = h.holder().answer(from("uid-l", 3), replica(2));

        assertThat(answer).isInstanceOf(FastFrame.Refused.class);
        assertThat(h.journal().held()).isEmpty();
    }

    @Test
    void aDEPARTINGHolderRefusesDepartingAndKeepsNothing() throws Exception {
        Holder h = holder(1 << 20);
        h.gate().depart();

        FastFrame.Body answer = h.holder().answer(from("uid-l", 3), replica(3));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.DEPARTING);
        assertThat(h.journal().held()).isEmpty();
    }

    @Test
    void aREPLICAPastTheCapIsRefusedBackpressureKeepingNothing() throws Exception {
        long one = entry(3, A, 10).encode().length;
        Holder h = holder(one);

        FastFrame.Body answer = h.holder().answer(from("uid-l", 3), replica(3));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.BACKPRESSURE);
        assertThat(h.journal().held()).as("not half a batch").isEmpty();
    }

    @Test
    void aRESENTReplicaIsAckedAgainNeverJournaledTwice() throws Exception {
        Holder h = holder(1 << 20);
        h.holder().answer(from("uid-l", 3), replica(3));

        FastFrame.Body again = h.holder().answer(from("uid-l", 3), replica(3));

        assertThat(again).isInstanceOf(FastWriteFrame.ReplicaAck.class);
        assertThat(h.journal().held()).hasSize(2);
    }

    @Test
    void aDEPARTUREAfterAnAppendFindsTheJournalNotEmpty() throws Exception {
        // ⚠️ M13.26h's review, P10: the departing flag and the empty check are
        // one step under the gate a REPLICA appends under.
        Holder h = holder(1 << 20);
        h.holder().answer(from("uid-l", 3), replica(3));

        assertThat(h.gate().departIfEmpty(h.journal())).isFalse();
        assertThat(h.holder().answer(from("uid-l", 3), replica(3)))
                .as("and accepts nothing after").isInstanceOf(FastFrame.Refused.class);
    }

    @Test
    void anEMPTYJournalDepartsAndNoReplicaLandsAfter() throws Exception {
        Holder h = holder(1 << 20);

        assertThat(h.gate().departIfEmpty(h.journal())).isTrue();

        assertThat(((FastFrame.Refused) h.holder().answer(from("uid-l", 3), replica(3)))
                .reason()).isEqualTo(FastFrame.Reason.DEPARTING);
        assertThat(h.journal().held()).isEmpty();
    }

    @Test
    void theTERMsLeaderIsRecordedWithItsJoin() {
        JoinedTerms terms = joined3();

        assertThat(terms.leaderOf(3)).contains("uid-l");
        assertThat(terms.leaderOf(4)).isEmpty();
    }
}
