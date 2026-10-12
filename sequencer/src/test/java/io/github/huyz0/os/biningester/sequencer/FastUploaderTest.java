// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader's upload (M13.31a; ADR-0081 §7): each stream's EXPOSED entries
 * of its term, contiguous from the chain's next offset, written as one
 * segment and one delta at the offsets already assigned -- never an entry
 * above the quorum frontier -- then the streams committed at the leader and
 * the copies released, its own and every holder's.
 */
class FastUploaderTest {

    /** The chain: each stream's next offset, and every commit made at given offsets. */
    private static final class Chain implements FastUploader.Chain {
        final Map<RunKey, Long> next = new HashMap<>(Map.of(S1, 100L, S2, 0L));
        final List<Map<RunKey, Long>> commits = new ArrayList<>();

        @Override
        public long next(RunKey stream) {
            return next.get(stream);
        }

        boolean failing;

        @Override
        public Map<RunKey, Long> commitAt(String segmentKey, Map<RunKey, Long> firsts,
                Map<RunKey, Integer> counts) throws java.io.IOException {
            if (failing) {
                throw new java.io.IOException("the delta's answer was lost");
            }
            commits.add(Map.copyOf(firsts));
            counts.forEach((s, n) -> next.put(s, firsts.get(s) + n));
            Map<RunKey, Long> after = new HashMap<>();
            counts.keySet().forEach(s -> after.put(s, next.get(s)));
            return after;
        }
    }

    private static final class Rig {
        final FastWriteLeaderTest.Leader l;
        final Chain chain = new Chain();
        final List<Map<RunKey, List<SegmentRecord>>> uploads = new ArrayList<>();
        final List<Map<RunKey, Long>> released = new ArrayList<>();
        final FastUploader uploader;

        Rig() throws Exception {
            this(1_000);
        }

        Rig(long bound) throws Exception {
            l = FastWriteLeaderTest.leader(1 << 20, bound);
            uploader = new FastUploader(7, l.journal(), l.leader(), chain, runs -> {
                uploads.add(Map.copyOf(runs));
                return "seg-" + uploads.size();
            }, released::add);
        }

        /** S1's batch {@code seq}, at q = 1: exposed once the leader's group is fsynced. */
        void exposeS1(long seq) throws Exception {
            l.leader().commit(B, commit(seq, S1), roster(), ALL);
            l.leader().flushGroup();
            l.leader().expose();
        }
    }

    @Test
    void anEXPOSEDPrefixIsOneSegmentCommittedAtItsOffsetsThenReleased() throws Exception {
        Rig rig = new Rig();
        rig.exposeS1(1);

        assertThat(rig.uploader.upload()).hasValue(Map.of(S1, 102L));

        assertThat(rig.uploads).singleElement().satisfies(u ->
                assertThat(u.get(S1)).extracting(SegmentRecord::id).containsExactly("d1", "e1"));
        assertThat(rig.chain.commits).containsExactly(Map.of(S1, 100L));
        assertThat(rig.l.journal().held()).as("the leader's own copy released").isEmpty();
        assertThat(rig.released).containsExactly(Map.of(S1, 102L));
        assertThat(rig.l.cursor().cursor(S1)).isEqualTo(102);
    }

    @Test
    void anUPLOADMovesTheLeadersBound() throws Exception {
        // ⚠️ its review round 2, T6: B = 3 holds one batch of 2 until committed.
        Rig rig = new Rig(3);
        rig.exposeS1(1);
        assertThat(rig.l.leader().commit(B, commit(2, S1), roster(), ALL))
                .as("the premise: 2 + 2 > B").isInstanceOf(FastWriteLeader.Wait.class);

        rig.uploader.upload();

        assertThat(rig.l.leader().commit(B, commit(2, S1), roster(), ALL))
                .isInstanceOf(FastWriteLeader.Pending.class);
    }

    @Test
    void anENTRYAboveTheQuorumFrontierIsNeverUploaded() throws Exception {
        Rig rig = new Rig();
        rig.exposeS1(1);
        rig.l.leader().commit(B, commit(2, S1), roster(), ALL);

        rig.uploader.upload();

        assertThat(rig.chain.commits).containsExactly(Map.of(S1, 100L));
        assertThat(rig.uploads.get(0).get(S1)).as("only the exposed batch").hasSize(2);
    }

    @Test
    void anUNEXPOSEDStreamMakesNoRequest() throws Exception {
        Rig rig = new Rig();
        rig.l.leader().commit(B, commit(1, S2), roster(), ALL);
        rig.l.leader().flushGroup();
        rig.l.leader().expose();

        assertThat(rig.uploader.upload()).as("S2 at q = 2, held by the leader alone")
                .isEmpty();
        assertThat(rig.uploads).isEmpty();
        assertThat(rig.chain.commits).isEmpty();
    }

    @Test
    void anENTRYTheChainAlreadyCommittedIsReleasedNotUploadedAgain() throws Exception {
        // ⚠️ its review round 1, P2: a delta that landed with its answer lost.
        Rig rig = new Rig();
        rig.exposeS1(1);
        rig.chain.next.put(S1, 102L);

        assertThat(rig.uploader.upload()).hasValue(Map.of(S1, 102L));
        assertThat(rig.uploads).isEmpty();
        assertThat(rig.l.journal().held()).isEmpty();
        assertThat(rig.released).containsExactly(Map.of(S1, 102L));
    }

    @Test
    void anENTRYBeyondAGapWaits() throws Exception {
        // ⚠️ its review round 1, T1: committing it would leave the gap a hole.
        Rig rig = new Rig();
        rig.exposeS1(1);
        rig.exposeS1(2);
        // ⚠️ the first batch's copy gone, the chain still at 100: a hole at 100-101
        rig.l.journal().release(S1, 102);

        assertThat(rig.uploader.upload()).isEmpty();
        assertThat(rig.uploads).isEmpty();
    }

    @Test
    void aFAILEDDeltaReleasesNothingAndCommitsNothingAtTheLeader() throws Exception {
        // ⚠️ its review round 1, T4.
        Rig rig = new Rig();
        rig.exposeS1(1);
        rig.chain.failing = true;

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> rig.uploader.upload())
                .isInstanceOf(java.io.IOException.class);
        assertThat(rig.l.journal().held()).as("held for the next upload").hasSize(1);
        assertThat(rig.released).isEmpty();
        assertThat(rig.l.leader().frontier(S1)).isEqualTo(102);
    }

    @Test
    void anENTRYOfAnotherTermIsNotThisUploadsToMake() throws Exception {
        Rig rig = new Rig();
        // ⚠️ held as a holder of term 6, on a stream this term never opened
        rig.l.journal().append(new FastJournalRecord.Entry(6, new RunKey(new UUID(3, 3), 0), 0,
                1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(5, 5), 0),
                List.of(new SegmentRecord("old", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1}))));

        assertThat(rig.uploader.upload()).as("a takeover's to decide").isEmpty();
        assertThat(rig.uploads).isEmpty();
    }

    @Test
    void twoEXPOSEDBatchesOfAStreamAreOneRun() throws Exception {
        Rig rig = new Rig();
        rig.exposeS1(1);
        rig.exposeS1(2);

        assertThat(rig.uploader.upload()).isEqualTo(Optional.of(Map.of(S1, 104L)));
        assertThat(rig.uploads.get(0).get(S1)).extracting(SegmentRecord::id)
                .containsExactly("d1", "e1", "d2", "e2");
    }
}
