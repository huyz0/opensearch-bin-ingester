// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A writer's CONFIRM reaches the term it names (M13.27v; ADR-0081 §2.3):
 * from the ROSTERED writer under its own key, its copy counts -- or, not
 * journaled, is replaced -- and it is answered by an empty HELD_STATUS; from
 * anyone else, refused and counted nowhere.
 *
 * <p>⚠️ **NOTHING ROUTED A CONFIRM**, so a writer outside the leader's zone
 * answered every ASSIGNED that required its copy into a 501, and an index at
 * {@code wal_quorum 2} never exposed a batch it wrote.
 */
class FastLeaderTermConfirmTest {

    /** In az-b: the fixture's leader is in az-a, so its copy is required at quorum 2. */
    private static final Roster.Incarnation B = FastTermStartTest.incarnation("b");

    private static FastFrame.Header header(long epoch, Roster.Incarnation from) {
        return new FastFrame.Header(FastWriteFrame.KIND_CONFIRM, epoch, from.podUid(),
                "uid-self");
    }

    private static FastLeaderTerm joined(MemoryBinStore store) throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        assertThat(term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3, B.podUid(),
                "uid-self"), new FastFrame.Join(B, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
        return term;
    }

    /** B's batch on S2 assigned after decision 3, B's copy required. */
    private static CommitDesk assigned(FastLeaderTerm term, FastWriteLeaderTest.Leader l)
            throws Exception {
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        assertThat(term.answerCommit(FastLeaderTermCommitTest.header(3, B),
                commit(B.podId(), 1, S2), desk)).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(S2, 0, 2, true))));
        return desk;
    }

    private static FastWriteFrame.Confirm confirm(String pod, boolean journaled) {
        return new FastWriteFrame.Confirm(commit(pod, 1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), journaled);
    }

    private static List<FastWriteLeader.ReplicaSend> replicas(FastWriteLeaderTest.Leader l) {
        return l.leader().replicas(List.of(FastWriteLeaderTest.WA, FastWriteLeaderTest.B,
                FastWriteLeaderTest.C));
    }

    @Test
    void aJOURNALEDCopyCompletesTheQuorumAndTheBatchIsExposed() throws Exception {
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = assigned(term, l);

        FastFrame.Body answer = term.answerConfirm(header(3, B), confirm(B.podId(), true), desk);

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(desk.exposures()).extracting(FastWriteLeader.Exposure::writerUid)
                .containsExactly(B.podUid());
    }

    @Test
    void anUNJOURNALEDCopyIsReplacedNotAwaited() throws Exception {
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = assigned(term, l);
        assertThat(replicas(l)).as("B's copy is awaited").isEmpty();

        FastFrame.Body answer = term.answerConfirm(header(3, B), confirm(B.podId(), false), desk);

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(replicas(l)).as("sent to a holder instead").hasSize(1);
    }

    @Test
    void aCONFIRMOnAStreamThisLeaderNeverOpenedCountsNowhere() throws Exception {
        // ⚠️ A PREDECESSOR's BATCH, or a stray: it threw "not open", and the
        // route answered 500 to a writer that retries.
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);

        FastFrame.Body answer = term.answerConfirm(header(3, B), new FastWriteFrame.Confirm(
                commit(B.podId(), 1, S2).key(), 3, List.of(new FastWriteFrame.RunAt(
                        new RunKey(new UUID(5, 5), 0), 0)), true), desk);

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(desk.exposures()).isEmpty();
    }

    @Test
    void aCONFIRMOfAnotherTermIsRefusedAndCountsNothing() throws Exception {
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = assigned(term, l);

        FastFrame.Body answer = term.answerConfirm(header(4, B), confirm(B.podId(), true), desk);

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(desk.exposures()).isEmpty();
    }

    @Test
    void aDEPOSEDTermRefusesACONFIRMLowerEpoch() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = joined(store);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        FastTermStartTest.put(store, FastTermStartTest.read(store, 3).fencedBy(5));

        FastFrame.Body answer = term.answerConfirm(header(3, B), confirm(B.podId(), true), desk);

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }

    @Test
    void aCONFIRMFromAPodTheRosterDoesNotListIsRefusedAndCountsNothing() throws Exception {
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = assigned(term, l);
        Roster.Incarnation stranger = FastTermStartTest.incarnation("c");

        FastFrame.Body answer = term.answerConfirm(header(3, stranger),
                confirm(B.podId(), true), desk);

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(desk.exposures()).isEmpty();
    }

    @Test
    void aCONFIRMUnderAnotherPodsKeyIsRefusedAndCountsNothing() throws Exception {
        FastLeaderTerm term = joined(new MemoryBinStore());
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = assigned(term, l);

        FastFrame.Body answer = term.answerConfirm(header(3, B), confirm("another", true), desk);

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(((FastFrame.Refused) answer).text()).contains("another");
        assertThat(desk.exposures()).isEmpty();
    }
}
