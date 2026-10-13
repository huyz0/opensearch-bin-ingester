// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.C;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.WA;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.leader;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The leader's side of a switch to the default path (M13.34q; ADR-0081 §8): a
 * stream with a fast tail paused -- assigned and exposed nothing, its quorum
 * frontier read in the same step -- its unexposed tail discarded from that
 * frontier, its batches never exposed while their other streams' runs still
 * count, and the stream unpaused at the chain's next offset.
 */
class FastWriteLeaderSwitchTest {

    @Test
    void aPAUSEDStreamAssignsAndExposesNothingItsFrontierHeldWhereItWasRead()
            throws Exception {
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();

        assertThat(l.leader().pauseForSwitch(S1)).hasValue(100);

        assertThat(l.leader().expose()).as("complete at q = 1, but held").isEmpty();
        assertThat(l.leader().commit(B, commit(2, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Wait.class);
    }

    @Test
    void aSTREAMWithNoFastTailIsNotPaused() throws Exception {
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);

        assertThat(l.leader().pauseForSwitch(S1)).isEmpty();
    }

    @Test
    void aDISCARDDropsTheTailAndItsBatchIsNeverExposedThoughItsOffsetsAreRetaken()
            throws Exception {
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();
        long frontier = l.leader().pauseForSwitch(S1).orElseThrow();

        assertThat(l.leader().discardFrom(S1, frontier)).containsExactly(commit(1, S1).key());
        assertThat(l.journal().held()).as("dropped from the journal").isEmpty();
        l.leader().unpause(S1, frontier);
        l.leader().commit(B, commit(2, S1), roster(), ALL);

        assertThat(l.leader().flushGroup()).singleElement().satisfies(a -> assertThat(
                a.assigned().runs().get(0).firstOffset()).as("the cursor resumed").isEqualTo(100));
        assertThat(l.leader().expose()).as("only the new batch, though both span 100-101")
                .extracting(FastWriteLeader.Exposure::exposed)
                .extracting(FastWriteFrame.Exposed::runs)
                .containsExactly(List.of(new FastWriteFrame.RunAt(S1, 100)));
        assertThat(l.leader().commit(B, commit(1, S1), roster(), ALL))
                .as("the discarded batch's retry is assigned anew, never answered")
                .isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void aDISCARDEDBatchsOtherStreamIsStillReplicated() throws Exception {
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        RunKey s3 = new RunKey(Q2, 1);
        l.leader().open(s3, 0);
        l.leader().commit(WA, commit(1, S2, s3), roster(), ALL);
        l.leader().flushGroup();
        long frontier = l.leader().pauseForSwitch(s3).orElseThrow();
        l.leader().discardFrom(s3, frontier);

        List<FastWriteLeader.ReplicaSend> sends = l.leader().replicas(List.of(WA, B, C));

        assertThat(sends).as("S2's copy still asked, s3's never").singleElement()
                .satisfies(send -> assertThat(send.replica().entries())
                        .extracting(FastJournalRecord.Entry::key).containsExactly(S2));
    }

    @Test
    void anUNPAUSEDStreamIsNeverAssignedBelowTheChainsNextOffset() throws Exception {
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();
        long frontier = l.leader().pauseForSwitch(S1).orElseThrow();
        l.leader().discardFrom(S1, frontier);

        l.leader().unpause(S1, 105); // the default-path run committed 100-104
        l.leader().commit(B, commit(2, S1), roster(), ALL);

        assertThat(l.leader().flushGroup()).singleElement().satisfies(a -> assertThat(
                a.assigned().runs().get(0).firstOffset()).isEqualTo(105));
    }

    @Test
    void aBATCHDiscardedBeforeItWasAnsweredLeavesNoPhantomCopyAsked() throws Exception {
        // ⚠️ its review round 1, P1/T1: counted at the flush, the dropped entry
        // stood in the frontier -- asked of holders, or "assigned twice".
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        l.leader().commit(WA, commit(1, S2), roster(), ALL);
        long frontier = l.leader().pauseForSwitch(S2).orElseThrow();
        l.leader().discardFrom(S2, frontier);
        l.leader().unpause(S2, 1); // the default-path run took offset 0

        assertThat(l.leader().flushGroup()).as("answered, its writer retries").hasSize(1);
        assertThat(l.leader().replicas(List.of(WA, B, C))).as("no copy asked").isEmpty();
    }

    @Test
    void aDISCARDKeepsAnExposedBatchBelowItsFrontier() throws Exception {
        // ⚠️ its review round 1, T2.
        FastWriteLeaderTest.Leader l = leader(1 << 20, 1_000);
        l.leader().commit(B, commit(1, S1), roster(), ALL);
        l.leader().flushGroup();
        l.leader().expose();
        l.leader().commit(B, commit(2, S1), roster(), ALL);
        l.leader().flushGroup();
        long frontier = l.leader().pauseForSwitch(S1).orElseThrow();

        assertThat(l.leader().discardFrom(S1, frontier)).as("the unexposed one only")
                .containsExactly(commit(2, S1).key());
        assertThat(l.journal().held()).extracting(FastJournalRecord.Entry::firstOffset)
                .containsExactly(100L);
    }
}
