// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The fake ships with the seam, in the same commit — architecture.md's rule for
 * all four I/O seams. ⚠️ A fake that does not honour the contract is worse than
 * no fake: every test written against it passes while the real implementation
 * is free to differ, so these tests pin the fake to the SAME contract
 * {@link Sequencer}'s javadoc states.
 */
class FakeSequencerTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID METRICS = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static long firstOffsetOf(CommitDelta delta, RunKey key) {
        return delta.runs().stream().filter(r -> r.key().equals(key))
                .map(RunCommit::firstOffset).findFirst().orElseThrow();
    }

    /**
     * ⚠️ THE FAKE MUST CARRY THE ATTRIBUTION, and reverting it to the two-arg
     * `SegmentCommit` left `:sequencer:`, `:ingest:` and `:http:` all green
     * while contradicting the fixture's own javadoc. A fake that drops it lets
     * a test pass while the real delta carries nothing for a successor to
     * rebuild its window from — the drift this fixture exists to prevent.
     */
    @Test
    void theFakeCopiesTheRequestsTripleOntoEachSegment() throws Exception {
        FakeSequencer fake = new FakeSequencer();

        var delta = fake.commitAll(java.util.List.of(
                new CommitRequest("poda", "inc-1", 4, "seg-a", counts(new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0), 1)),
                new CommitRequest("podb", "inc-2", 0, "seg-b", counts(new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0), 1))));

        assertThat(delta.segments().get(0).attribution())
                .isEqualTo(new binjava.format.SegmentCommit.Attribution("poda", "inc-1", 4));
        assertThat(delta.segments().get(1).attribution())
                .as("the second segment's OWN triple, not the first's")
                .isEqualTo(new binjava.format.SegmentCommit.Attribution("podb", "inc-2", 0));
    }

    @Test
    void aBATCHEDCommitAdvancesOffsetsACROSSSegmentsJustAsTheRealLogDoes() throws Exception {
        // ⚠️ THE FAKE'S MULTI-REQUEST PATH HAD NO TEST AT ALL, and two mutations
        // survived the whole tree: dropping every request but the first, and
        // snapshotting the offsets before the loop so they do NOT advance across
        // segments. The second is exactly the I2 divergence — two pods flushing
        // one stream in a window both starting where the fake had reached — and
        // a fake that gets it wrong lets every caller's tests pass while the
        // real sequencer assigns one range twice. That is the failure a fake
        // exists to make visible.
        FakeSequencer fake = new FakeSequencer();
        RunKey stream = new RunKey(LOGS, 0);

        CommitDelta delta = fake.commitAll(java.util.List.of(
                new CommitRequest("pod0", "i1", 0, "seg-a", counts(stream, 3)),
                new CommitRequest("pod1", "i1", 0, "seg-b", counts(stream, 5))));

        assertThat(delta.segments()).as("every submitted flush is in the delta").hasSize(2);
        // ⚠️ WHICH KEY CARRIES WHICH RUNS, not merely that both are present.
        // Measured: giving every segment its NEIGHBOUR's key left the entire
        // tree green — offsets stay correct, only the pairing is wrong, and the
        // pairing is what the Sequencer contract settles. This is the fixture
        // every downstream module tests against, so a fake that mis-pairs hides
        // the defect in all of them.
        assertThat(delta.segments().get(0).segmentKey()).isEqualTo("seg-a");
        assertThat(delta.segments().get(1).segmentKey()).isEqualTo("seg-b");
        assertThat(delta.segments().get(0).runs().get(0).recordCount()).isEqualTo(3);
        assertThat(delta.segments().get(1).runs().get(0).recordCount()).isEqualTo(5);
        assertThat(delta.segments().get(0).runs().get(0).firstOffset()).isZero();
        assertThat(delta.segments().get(1).runs().get(0).firstOffset())
                .as("the second segment starts where the FIRST stopped, not at zero")
                .isEqualTo(3);
        assertThat(fake.nextOffset(stream)).as("and the fake reflects both").isEqualTo(8);
    }

    @Test
    void theFakeRefusesAColLidingBatchWITHOUTAdvancingItsState() throws Exception {
        // ⚠️ REFUSE BEFORE MUTATING, like the real log. Building the segments
        // first and letting `CommitDelta`'s constructor refuse leaves the
        // offsets already advanced and the sequence already incremented — so a
        // fake that REJECTED a batch had still moved, and a caller's retry would
        // see offsets skip where the real `CommitLog` never does.
        FakeSequencer fake = new FakeSequencer();
        RunKey stream = new RunKey(LOGS, 0);
        fake.commit(new CommitRequest("pod0", "i1", 0, "seg-a", counts(stream, 4)));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> fake.commitAll(java.util.List.of(
                new CommitRequest("pod0", "i1", 1, "seg-clash", counts(stream, 1)),
                new CommitRequest("pod1", "i1", 0, "seg-clash", counts(stream, 1)))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(fake.nextOffset(stream))
                .as("a refused batch must leave the fake exactly where it was").isEqualTo(4);
        assertThat(fake.commit(new CommitRequest("pod0", "i1", 2, "seg-b", counts(stream, 1)))
                .sequence())
                .as("and must not have consumed a sequence number either").isEqualTo(1);
    }

    @Test
    void offsetsAreContiguousPerStreamAcrossCommits() throws Exception {
        // ⚠️ FR-3: a stable, monotonic offset per (index, partition). The
        // second commit must start where the first ended -- not at zero, which
        // is what a fake that forgets its state returns.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey key = new RunKey(LOGS, 0);
            CommitDelta first = seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", counts(key, 3)));
            CommitDelta second = seq.commit(new CommitRequest("pod1", "i1", 1L, "seg-2", counts(key, 4)));
            assertThat(firstOffsetOf(first, key)).isZero();
            assertThat(firstOffsetOf(second, key)).isEqualTo(3L);
        }
    }

    @Test
    void eachStreamHasItsOwnOffsetSpace() throws Exception {
        // ⚠️ Offsets are per (index, partition), NOT global -- a single shared
        // counter would make every stream's offsets sparse and non-contiguous,
        // which FR-3 forbids and which a consumer resuming from an offset
        // cannot work with.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey logs = new RunKey(LOGS, 0);
            RunKey metrics = new RunKey(METRICS, 0);
            seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", counts(logs, 5)));
            CommitDelta d = seq.commit(new CommitRequest("pod1", "i1", 1L, "seg-2", counts(metrics, 2)));
            assertThat(firstOffsetOf(d, metrics))
                    .as("a fresh stream starts at 0 however many offsets another stream used")
                    .isZero();
            // ⚠️ Both halves, deliberately. `isZero()` alone is satisfied by an
            // implementation that returns 0 for EVERYTHING -- the red record for
            // an earlier draft of this test showed exactly that, passing against
            // a stub. Asserting that `logs` then CONTINUES at 5 is what makes
            // this fail against a stuck offset as well as against a global one.
            CommitDelta back = seq.commit(new CommitRequest("pod1", "i1", 2L, "seg-3", counts(logs, 1)));
            assertThat(firstOffsetOf(back, logs))
                    .as("the other stream kept its own position meanwhile")
                    .isEqualTo(5L);
        }
    }

    @Test
    void partitionsOfTheSameIndexAreSeparateStreams() throws Exception {
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey p0 = new RunKey(LOGS, 0);
            RunKey p1 = new RunKey(LOGS, 1);
            seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", counts(p0, 7)));
            CommitDelta d = seq.commit(new CommitRequest("pod1", "i1", 1L, "seg-2", counts(p1, 1)));
            assertThat(firstOffsetOf(d, p1)).isZero();
            // ⚠️ As above: the continuation half is what discriminates.
            CommitDelta back = seq.commit(new CommitRequest("pod1", "i1", 2L, "seg-3", counts(p0, 1)));
            assertThat(firstOffsetOf(back, p0)).isEqualTo(7L);
        }
    }

    @Test
    void theSequenceNumberIsMonotonicAndGapless() throws Exception {
        // ⚠️ I1's shape at the seam: sequence numbers are consumed one at a
        // time. A fake that returns a constant would let a test pass while the
        // real chain skipped or reused one.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey key = new RunKey(LOGS, 0);
            for (int i = 0; i < 5; i++) {
                CommitDelta d = seq.commit(
                        new CommitRequest("pod1", "i1", i, "seg-" + i, counts(key, 1)));
                assertThat(d.sequence()).as("commit %d", i).isEqualTo(i);
            }
        }
    }

    @Test
    void commitsFromDifferentNodesShareOneStreamsOffsetSpace() throws Exception {
        // ⚠️ THE POINT OF THE SEAM (M4 SPEC § Deployment constraint). From M5
        // these arrive over the network from different nodes; the offsets they
        // receive must still be one contiguous run per stream, because a
        // consumer reads one stream and cannot tell which node produced what.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey key = new RunKey(LOGS, 0);
            CommitDelta a = seq.commit(new CommitRequest("podA", "i1", 0L, "seg-a", counts(key, 2)));
            CommitDelta b = seq.commit(new CommitRequest("podB", "i1", 0L, "seg-b", counts(key, 2)));
            assertThat(firstOffsetOf(a, key)).isZero();
            assertThat(firstOffsetOf(b, key))
                    .as("node B continues node A's run -- the offset space is the STREAM's, "
                            + "not the node's. ⚠️ This says nothing about replay detection: "
                            + "the fake does not dedup on (podId, flushSeq) and neither does "
                            + "the real sequencer until M4.10")
                    .isEqualTo(2L);
        }
    }

    @Test
    void theDeltaCarriesTheSegmentKeyItWasCommittedFor() throws Exception {
        try (FakeSequencer seq = new FakeSequencer()) {
            CommitDelta d = seq.commit(new CommitRequest("pod1", "i1", 0L, "bins/cluster-a/seg-xyz",
                    counts(new RunKey(LOGS, 0), 1)));
            assertThat(d.segmentKey()).isEqualTo("bins/cluster-a/seg-xyz");
        }
    }

    @Test
    void oneCommitCarryingManyStreamsReturnsEveryRunWithItsOwnCountAndOffset() throws Exception {
        // ⚠️ THE SHAPE THE SEAM EXISTS FOR, and round-1 test review found every
        // other test in this class committed exactly ONE run -- so a fake that
        // silently dropped all but the first, or hardcoded recordCount, passed
        // the whole suite. `recordCounts` is a Map because the product bundles
        // MANY streams into one segment; that is the entire economic argument
        // (one PUT carries a thousand indices), so it is the case most worth
        // pinning.
        try (FakeSequencer seq = new FakeSequencer()) {
            // ⚠️ SIX streams, not three, and that is a correctness property of
            // the test rather than thoroughness for its own sake. The fixture
            // hands in a LinkedHashMap but CommitRequest replaces it with
            // Map.copyOf, whose iteration order is SALT-randomised per JVM --
            // so the test controls neither side of the ordering it asserts. At
            // three keys a missing sort still produces the asserted order 1 run
            // in 6, i.e. a broken sort would pass CI most times; round-2 review
            // measured exactly that. Six keys drops the escape to 1 in 720.
            RunKey a = new RunKey(LOGS, 0);
            RunKey b = new RunKey(LOGS, 1);
            RunKey c = new RunKey(LOGS, 2);
            RunKey d0 = new RunKey(METRICS, 0);
            RunKey e0 = new RunKey(METRICS, 1);
            RunKey f0 = new RunKey(METRICS, 2);
            Map<RunKey, Integer> many = new LinkedHashMap<>();
            many.put(f0, 7);
            many.put(c, 11);
            many.put(d0, 2);
            many.put(a, 3);
            many.put(e0, 13);
            many.put(b, 5);
            CommitDelta d = seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", many));

            assertThat(d.runs()).as("every stream committed comes back").hasSize(6);
            assertThat(d.runs()).extracting(RunCommit::key)
                    .as("sorted by RunKey -- a delta's run order must be deterministic, "
                            + "so two replays produce byte-identical deltas")
                    .containsExactly(a, b, c, d0, e0, f0);
            assertThat(d.runs()).extracting(RunCommit::recordCount)
                    .as("each run keeps ITS OWN count, not the first one's")
                    .containsExactly(3, 5, 11, 2, 13, 7);
            assertThat(d.runs()).extracting(RunCommit::firstOffset)
                    .as("each stream's first commit starts at its own 0")
                    .containsExactly(0L, 0L, 0L, 0L, 0L, 0L);
        }
    }

    @Test
    void everyStreamInAMultiRunCommitAdvancesByItsOwnCount() throws Exception {
        // ⚠️ The follow-on: a fake that returned the right runs but advanced
        // them all by the same amount would pass the test above.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey a = new RunKey(LOGS, 0);
            RunKey b = new RunKey(LOGS, 1);
            Map<RunKey, Integer> first = new LinkedHashMap<>();
            first.put(a, 3);
            first.put(b, 5);
            seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", first));

            Map<RunKey, Integer> second = new LinkedHashMap<>();
            second.put(a, 1);
            second.put(b, 1);
            CommitDelta d = seq.commit(new CommitRequest("pod1", "i1", 1L, "seg-2", second));
            assertThat(firstOffsetOf(d, a)).isEqualTo(3L);
            assertThat(firstOffsetOf(d, b)).isEqualTo(5L);
        }
    }

    @Test
    void nextOffsetReportsWhereTheNextCommitForThatStreamWouldLand() throws Exception {
        // ⚠️ A public accessor on a testFixtures class is API for other
        // modules' tests; untested, `return 0L` survives.
        try (FakeSequencer seq = new FakeSequencer()) {
            RunKey key = new RunKey(LOGS, 0);
            assertThat(seq.nextOffset(key)).as("nothing committed yet").isZero();
            seq.commit(new CommitRequest("pod1", "i1", 0L, "seg-1", counts(key, 4)));
            assertThat(seq.nextOffset(key)).isEqualTo(4L);
            assertThat(seq.nextOffset(new RunKey(METRICS, 0)))
                    .as("an untouched stream is still at 0").isZero();
        }
    }

    /**
     * ⚠️ THE FAKE MUST REFUSE TOO (M4.10d). A fake that applied a replay twice
     * would let every caller's retry test pass while the real sequencer
     * answered instead of committing -- the exact divergence a fake kept in the
     * same commit exists to prevent.
     */
    @Test
    void aReplayedTripleIsAnsweredRatherThanAppliedTwice() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            CommitRequest flush = new CommitRequest("poda", "i1", 0, "seg/0",
                    counts(new RunKey(LOGS, 0), 3));
            CommitDelta first = fake.commitAll(List.of(flush));
            long assigned = first.segments().get(0).runs().get(0).firstOffset();

            CommitDelta replay = fake.commitAll(List.of(flush));

            assertThat(replay.segments().get(0).runs().get(0).firstOffset())
                    .as("the retry gets the offsets that already apply")
                    .isEqualTo(assigned);
            assertThat(fake.nextOffset(new RunKey(LOGS, 0)))
                    .as("and NOTHING advanced -- 3 records committed, not 6")
                    .isEqualTo(3L);
        }
    }

    /**
     * ⚠️ A RESTART IS NOT A REPLAY, in the fake as in the real one: the same
     * pod reissues flushSeq 0 under a new incarnation and must be ACCEPTED.
     */
    @Test
    void aRestartedPodReissuingFlushSeqZeroIsAcceptedByTheFake() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            fake.commitAll(List.of(new CommitRequest("poda", "i1", 0, "seg/0", counts(new RunKey(LOGS, 0), 3))));
            fake.commitAll(List.of(new CommitRequest("poda", "i2", 0, "seg/1", counts(new RunKey(LOGS, 0), 4))));

            assertThat(fake.nextOffset(new RunKey(LOGS, 0)))
                    .as("both flushes committed: 3 then 4")
                    .isEqualTo(7L);
        }
    }

    /**
     * ⚠️ THE FAKE MUST MERGE TOO. Review measured `segments.addAll(answered)`
     * deletable with the suite green: the fake would then drop a replayed
     * caller's segment from a mixed batch, so a caller matching by segment key
     * would find nothing -- while production, which IS pinned, returns it.
     */
    @Test
    void aMixedBatchThroughTheFakeCarriesBothTheFreshAndTheAnswered() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            CommitRequest a = new CommitRequest("poda", "i1", 0, "seg/a",
                    counts(new RunKey(LOGS, 0), 3));
            fake.commitAll(List.of(a));
            CommitRequest fresh = new CommitRequest("podb", "i9", 0, "seg/b",
                    counts(new RunKey(LOGS, 0), 4));

            CommitDelta mixed = fake.commitAll(List.of(a, fresh));

            assertThat(mixed.segments().stream().map(s -> s.segmentKey()).toList())
                    .as("both submitted segments come back")
                    .containsExactlyInAnyOrder("seg/a", "seg/b");
        }
    }

    /**
     * ⚠️ AND ITS ALL-REPLAY SEQUENCE NAMES THE DELTA THAT HOLDS THE RECORDS,
     * not the slot the next write would take -- `nextSequence - 1`, which
     * review measured mutable to `nextSequence` with everything green.
     */
    @Test
    void anAllReplayBatchThroughTheFakeReportsTheSequenceThatHoldsIt() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            CommitRequest a = new CommitRequest("poda", "i1", 0, "seg/a",
                    counts(new RunKey(LOGS, 0), 3));
            long landed = fake.commitAll(List.of(a)).sequence();

            assertThat(fake.commitAll(List.of(a)).sequence())
                    .as("the retry names the delta that already holds these records")
                    .isEqualTo(landed);
        }
    }

    /**
     * ⚠️ THE FAKE MUST KEY ON THE SEGMENT TOO. One flush may submit several
     * segments under one flushSeq; keying on the triple alone makes the second
     * overwrite the first, so both retried requests resolve to the LAST segment
     * and the returned delta names it twice. Review measured the fake THROWING
     * on an input the real sequencer answers -- the divergence a fake exists to
     * prevent.
     */
    @Test
    void theFakeAnswersPerSEGMENTWhenOneTripleCarriesTwo() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            CommitRequest a = new CommitRequest("podx", "i1", 5, "seg/a",
                    counts(new RunKey(LOGS, 0), 3));
            CommitRequest b = new CommitRequest("podx", "i1", 5, "seg/b",
                    counts(new RunKey(LOGS, 0), 4));
            CommitDelta first = fake.commitAll(List.of(a, b));
            long offsetA = firstOffsetOf(first, RA_KEY_A);
            long offsetB = firstOffsetOf(first, RA_KEY_B);

            CommitDelta retry = fake.commitAll(List.of(a, b));

            assertThat(firstOffsetOf(retry, RA_KEY_A)).as("seg/a keeps ITS offsets")
                    .isEqualTo(offsetA);
            assertThat(firstOffsetOf(retry, RA_KEY_B)).as("seg/b keeps ITS offsets")
                    .isEqualTo(offsetB);
        }
    }

    private static final String RA_KEY_A = "seg/a";
    private static final String RA_KEY_B = "seg/b";

    private static long firstOffsetOf(CommitDelta delta, String segmentKey) {
        return delta.segments().stream()
                .filter(s -> s.segmentKey().equals(segmentKey))
                .findFirst()
                .orElseThrow(() -> new AssertionError("nothing for " + segmentKey))
                .runs().get(0).firstOffset();
    }
}
