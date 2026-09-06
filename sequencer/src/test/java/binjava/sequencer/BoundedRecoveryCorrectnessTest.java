// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.BoundedRecoveryFixture.PREFIX;
import static binjava.sequencer.BoundedRecoveryFixture.RA;
import static binjava.sequencer.BoundedRecoveryFixture.frozen;
import static binjava.sequencer.BoundedRecoveryFixture.manager;
import static binjava.sequencer.BoundedRecoveryFixture.noRenew;
import static binjava.sequencer.BoundedRecoveryFixture.request;
import static binjava.sequencer.BoundedRecoveryFixture.streamOfTerm;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.Checkpoint.StreamOffsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What bounded recovery RECONSTRUCTS (M4.9).
 *
 * <p>⚠️ {@code LocalSequencerBoundedRecoveryTest} IS THE OTHER HALF, and it is
 * satisfied by a recovery that reads nothing and is wrong — which is why this
 * one exists: it asserts what recovery RECONSTRUCTS. Some tests here also count
 * requests, where the offset alone cannot tell which path was taken.
 *
 * <p>⚠️ EACH TERM COMMITS ITS OWN STREAM. Offsets in a {@code RunCommit} are
 * ABSOLUTE, not incremental, so replaying only the NEWEST ancestor already
 * yields the final offset for any stream that ancestor touched — measured, with
 * one shared stream a mutant that stopped the walk after the first ancestor
 * PASSED.
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BoundedRecoveryCorrectnessTest {

    @Test
    void boundedRecoveryReconstructsEXACTLYWhatFullReplayWould() throws Exception {
        // ⚠️ THE COST SUITE IS SATISFIED BY A RECOVERY THAT READS
        // NOTHING AND IS WRONG. This is the other half: the offsets a bounded
        // takeover produces must equal what the unbounded walk produced.
        // ⚠️ EACH TERM COMMITS ITS OWN STREAM, so the oldest term's offset can
        // only come from that term -- through the checkpoint, since the walk
        // stops there. A shared stream would let the newest ancestor's absolute
        // offsets satisfy this without any inheritance at all.
        MemoryBinStore backing = new MemoryBinStore();
        for (int t = 0; t < 4; t++) {
            LocalSequencer s = LocalSequencer.start(backing, PREFIX, manager(backing, "pod" + t), 8,
                    noRenew(), 1, frozen()).orElseThrow();
            for (int i = 0; i < 5; i++) {
                s.commit(request("pod" + t, i, "seg/" + t + "/" + i, streamOfTerm(t)));
            }
            s.close();
        }

        LocalSequencer next = LocalSequencer.start(backing, PREFIX, manager(backing, "last"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        try {
            // ⚠️ THE OLDEST TERM'S STREAM IS THE ONE THAT PROVES INHERITANCE:
            // nothing since term 0 has touched it, so a recovery that stopped
            // without carrying the checkpoint's cumulative offsets restarts it
            // at 0 -- I2, and the contract `Sequencer.commit` states verbatim.
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("term 0's five records are still counted, three terms later")
                    .isEqualTo(5);
            assertThat(next.commit(request("last", 1, "seg/last2", streamOfTerm(3)))
                            .allRuns().get(0).firstOffset())
                    .as("and so are the newest ancestor's")
                    .isEqualTo(5);
        } finally {
            next.close();
        }
    }
    @Test
    void theDELTASAfterTheCheckpointAreReplayed() throws Exception {
        // ⚠️ THE OTHER HALF OF "newest checkpoint PLUS AT MOST K DELTAS". With
        // K = 1 the newest checkpoint covers the whole chain, so a reader that
        // skipped the tail entirely still produced the right answer — measured,
        // `if (hop.from() > 0) { return; }` survived the whole suite before this
        // test existed.
        // ⚠️ K = 5 WITH 12 COMMITS leaves the last two deltas above the newest
        // checkpoint, and dropping them re-issues two ACKED offsets: I2, silent.
        // ⚠️ 12 IS NOT A MULTIPLE OF 5 ON PURPOSE. At K = 4 the checkpoint would
        // land exactly on the chain's end and the tail would be empty again.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first = LocalSequencer.start(backing, PREFIX, manager(backing, "pod0"), 8,
                noRenew(), 5, frozen()).orElseThrow();
        for (int i = 0; i < 12; i++) {
            // ⚠️ ONE COMMIT IN THE TAIL GOES TO ITS OWN STREAM, and without it
            // this test repeats at the DELTA level the mistake its own class
            // javadoc records at the TERM level: offsets are ABSOLUTE, so
            // reading only the LAST delta above the checkpoint already yields
            // 12. Measured -- `keyFor(hop.from())` for `hop.from() - 1`, and
            // `checkpoint.sequence() + 1` for `checkpoint.sequence()`, each drop
            // the FIRST uncovered delta and survived the whole suite.
            first.commit(request("pod0", i, "seg/0/" + i,
                    i == 10 ? streamOfTerm(1) : streamOfTerm(0)));
        }
        first.close();

        LocalSequencer next = LocalSequencer.start(backing, PREFIX, manager(backing, "last"), 8,
                noRenew(), 5, frozen()).orElseThrow();
        try {
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("eleven on the shared stream -- not the ten the checkpoint covers")
                    .isEqualTo(11);
            assertThat(next.commit(request("last", 1, "seg/last2", streamOfTerm(1)))
                            .allRuns().get(0).firstOffset())
                    .as("and the FIRST uncovered delta's own stream is not restarted at 0 -- "
                            + "which is what an off-by-one in `startAfter` or in the hop's "
                            + "`from` does, silently, and is I2")
                    .isEqualTo(1);
        } finally {
            next.close();
        }
    }
    @Test
    void recoveringAChainThatCheckpointedItsOWNProgressKeepsItsNextSequence() throws Exception {
        // ⚠️ `nextSequence` IS DERIVED FROM THE ENTRIES READ, so a checkpoint
        // that lets the reader SKIP them takes it with them -- to zero when the
        // checkpoint covers the whole chain -- and the next commit re-races
        // slots that are already taken. Seeding it from the checkpoint's own
        // sequence is what restores it.
        // ⚠️ THIS PATH DOES NOT ARISE IN PRODUCTION TODAY, because
        // `LocalSequencer.start` recovers a FRESHLY MINTED epoch that has no
        // checkpoint of its own; measured, the seeding survived every other test
        // in this commit. It is pinned here rather than deleted because the
        // alternative is code no test can distinguish from a no-op, and because
        // a restart recovering its own chain is exactly this shape.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        CheckpointWriter.Ticker frozen = frozen();
        long expected;
        try (CheckpointWriter writer =
                new CheckpointWriter(store, log, "bins", 1, Duration.ofSeconds(30), frozen)) {
            for (int i = 0; i < 5; i++) {
                java.util.List<CommitRequest> batch =
                        java.util.List.of(request("poda", i, "seg/" + i));
                log.commitAll(batch);
                writer.observe(batch);
            }
            expected = log.nextSequence();
        }

        CommitLog reopened = new CommitLog(store, "bins", 1);
        reopened.recover();

        assertThat(reopened.nextSequence())
                .as("the checkpoint carried the chain's own progress, not just its offsets")
                .isEqualTo(expected);
        assertThat(reopened.nextOffset(RA))
                .as("and the offsets with it")
                .isEqualTo(5);
    }
    @Test
    void aCheckpointCoveringEntriesPASTTheBarrierIsNotUsed() throws Exception {
        // ⚠️ I3, AND THE ONE WAY THIS BOUND CAN CORRUPT RATHER THAN MERELY COST.
        // A fenced leader does not stop at once: it goes on committing, and its
        // checkpoint writer goes on pointing LATEST at newer state. Its
        // successor seals it at the slot its own CONTINUE names, and everything
        // past that slot is a DISCARDED SUFFIX. Folding a checkpoint that covers
        // those entries into the history applies the suffix -- silently, and
        // with exactly the confident air of a bounded recovery.
        // ⚠️ A checkpoint at sequence S reflects the chain through S-1, so it is
        // usable only when S-1 <= the barrier. There is no OLDER checkpoint to
        // fall back to, because the pointer holds one; the walk simply carries
        // on unbounded, which is slow and correct.
        MemoryBinStore backing = new MemoryBinStore();
        long acked = 0;
        LocalSequencer first = LocalSequencer.start(backing, PREFIX, manager(backing, "pod0"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        for (int i = 0; i < 5; i++) {
            first.commit(request("pod0", i, "seg/0/" + i, streamOfTerm(0)));
            acked++;
        }
        first.close();

        // The fenced leader's writer, still running, points LATEST at state far
        // beyond anything its successor will seal.
        // ⚠️ THE BOUNDARY IS PINNED AT barrier + 2, NOT AT 9,999. A first draft
        // used a huge sequence, which any of the plausible off-by-ones refuse
        // just as readily; measured, both `sequence() - 2 > upTo` (admitting ONE
        // entry past the barrier -- the exact I3) and `sequence() - 1 >= upTo`
        // (refusing a checkpoint sitting ON the barrier, silently reverting to a
        // full crossing) survived it. The sibling test below pins the other
        // side, so the two together allow exactly one value.
        // ⚠️ RECOVERED, because `nextSequence()` on a fresh CommitLog is ZERO
        // until it reads the chain -- a first draft of this fixture put both
        // checkpoints BELOW the barrier and proved nothing.
        CommitLog reader = new CommitLog(backing, PREFIX, 1);
        reader.recover();
        long barrier = reader.nextSequence();
        Checkpoint pastTheBarrier = new Checkpoint(barrier + 2,
                Map.of(streamOfTerm(0), new StreamOffsets(9_999, 0)), Map.of("pod0", 99L));
        byte[] body = pastTheBarrier.encode();
        backing.put(new LogKeys(PREFIX, 1).latestCheckpointKey(),
                new Body(body.length, () -> new java.io.ByteArrayInputStream(body)));

        LocalSequencer next = LocalSequencer.start(backing, PREFIX, manager(backing, "last"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        try {
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("the barrier-crossing checkpoint is refused, so the offset is the "
                            + "REAL acked count and not the 9,999 it claimed")
                    .isEqualTo(acked);
        } finally {
            next.close();
        }
    }
    @Test
    void aCheckpointSittingEXACTLYOnTheBarrierIsUsed() throws Exception {
        // ⚠️ THE OTHER SIDE OF THE BOUNDARY. A checkpoint at sequence S covers
        // [0, S-1], so S = barrier + 1 covers everything through the barrier and
        // nothing past it -- the most useful checkpoint there can be. Refusing
        // it is not a correctness bug; it silently reverts to a full crossing,
        // which is the cost this whole task exists to remove.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first = LocalSequencer.start(backing, PREFIX, manager(backing, "pod0"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        for (int i = 0; i < 5; i++) {
            first.commit(request("pod0", i, "seg/0/" + i, streamOfTerm(0)));
        }
        first.close();

        CommitLog reader = new CommitLog(backing, PREFIX, 1);
        reader.recover();
        long barrier = reader.nextSequence();
        Checkpoint onTheBarrier = new Checkpoint(barrier + 1,
                Map.of(streamOfTerm(0), new StreamOffsets(5, 0)), Map.of("pod0", 4L));
        byte[] body = onTheBarrier.encode();
        backing.put(new LogKeys(PREFIX, 1).latestCheckpointKey(),
                new Body(body.length, () -> new java.io.ByteArrayInputStream(body)));

        CountingBinStore counting = new CountingBinStore(backing);
        LocalSequencer next = LocalSequencer.start(counting, PREFIX, manager(counting, "last"), 8,
                noRenew(), 1, frozen()).orElseThrow();
        try {
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("accepted, so the offsets it carries are the ones used")
                    .isEqualTo(5);
            // ⚠️ EXACT, like the failover test's counts and for the same
            // reason: an inequality leaves room a regression can live in.
            // ⚠️ THIS NOTE USED TO INVENTORY THE OTHER SUITES' ASSERTIONS, and
            // that inventory went stale SIX TIMES -- twice inside the very
            // commits that fixed it, once inside the same delta that moved the
            // assertion it described. A sentence counting another file's
            // assertions is falsified by the next commit that moves one, so it
            // is deleted rather than restated. What is left is the reason,
            // which no edit elsewhere can make false.
            assertThat(counting.counts().total())
                    .as("the bounded crossing costs a small constant")
                    .isEqualTo(18L);
        } finally {
            next.close();
        }
    }
    @Test
    void aRunOfBURNEDEpochsStillStopsAtTheCheckpointBehindThem() throws Exception {
        // ⚠️ THE ARITHMETIC BRANCH'S TRUE ARM IS REACHED BY NOTHING ELSE. When a
        // chain was sealed or abandoned before it was ever opened, the walk
        // steps back by arithmetic rather than by following a CONTINUE, and
        // until this test the probe on that path could be deleted with the whole
        // suite green. The walk here must step 4 -> 3 -> 2 -> 1. `neverOpened`'s javadoc records that this shape comes
        // from a store outage spanning a failover.
        // ⚠️ ASSERTED BY REQUEST COUNT, not by a sentinel offset. A first draft
        // seeded a pointer CLAIMING 500 where the deltas said 4, which does
        // separate "used it" from "recomputed" -- but `Math::max` then keeps the
        // 500 even if the walk FOLDS the checkpoint and carries on to the
        // origin, and that mutant survived the whole suite. Counting requests
        // sees it, needs no state production cannot reach, and is what the
        // sibling CONTINUE-branch test already does.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog epoch1 = new CommitLog(store, PREFIX, 1);
        epoch1.open(0, 0);
        try (CheckpointWriter writer =
                new CheckpointWriter(store, epoch1, PREFIX, 1, Duration.ofSeconds(30), frozen())) {
            for (int i = 0; i < 40; i++) {
                List<CommitRequest> batch =
                        List.of(request("pod1", i, "seg/1/" + i, streamOfTerm(1)));
                epoch1.commitAll(batch);
                writer.observe(batch);
            }
        }
        long burnedAt = epoch1.nextSequence();

        // ⚠️ TWO EPOCHS ARE BURNED, not one, because this test is NAMED for a RUN
        // of them and a single burn enters the arithmetic branch exactly once.
        // Measured: guarding the probe to fire only on the FIRST arithmetic step
        // survives the whole build, and under it a run of two or more walks the
        // ancestry unbounded again -- the crossing this task removes, on the
        // shape `neverOpened`'s javadoc names, where an outage spanning two
        // failovers burns two.
        CommitLog epoch4 = new CommitLog(store, PREFIX, 4);
        epoch4.open(3, burnedAt);

        CountingBinStore counting = new CountingBinStore(store);
        CommitLog reader = new CommitLog(counting, PREFIX, 4);
        reader.recover();

        assertThat(reader.nextOffset(streamOfTerm(1)))
                .as("epoch 1's forty records survive TWO burned epochs between")
                .isEqualTo(40);
        // ⚠️ AN ANCESTOR'S CHECKPOINT MUST NOT SEED THIS CHAIN'S OWN NEXT SLOT.
        // The `chainEpoch == ownEpoch` guard on that seeding was killed by
        // nothing: `if (true)` left the build green while this reads 41 -- the
        // ancestor's next slot adopted as ours, which sends the first commit
        // past every empty slot the seal protocol depends on.
        assertThat(reader.nextSequence())
                .as("epoch 3 is empty, so its next slot is its own, not epoch 1's")
                .isEqualTo(1);
        assertThat(counting.counts().total())
                .as("and cost a small constant -- one probe per burned epoch plus the "
                        + "bounded read, where an unbounded walk of these forty deltas "
                        + "measures in the fifties")
                .isEqualTo(14L);
    }

    @Test
    void theBOUNDEDCrossingStillStopsAtTheSlotTheCONTINUENamed() throws Exception {
        // ⚠️ THE BARRIER REACHES `crossFromCheckpoint` FROM THREE CALL SITES,
        // each passing its own: the first hop, the walk's probe after a
        // CONTINUE, and the burned-epoch step (whose limit is already
        // MAX_VALUE). This test nets the FIRST; its sibling below nets the
        // second. An earlier version of this comment said "twice", and the
        // undercount is exactly why the second had no net. `applyChain` returns at `entry.sequence() > hop.upTo()`,
        // so `upTo` is the only thing keeping a FENCED leader's discarded suffix
        // out of the history -- I3. Measured: replacing `upTo` with
        // `Long.MAX_VALUE` in the checkpoint hop left the whole build green,
        // while the same widening of the pre-existing `Hop(chainEpoch, 0, ...)`
        // sites fails `CommitLogCrossEpochTest`. The bounded path had no net.
        // ⚠️ THE CHECKPOINT MUST BE ACCEPTED for this to reach the bounded hop,
        // so it sits AT the barrier -- the sibling test above covers a
        // checkpoint that is refused, and its fixture has no entries past the
        // barrier at all. This one has a zombie entry and a usable checkpoint.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog fenced = new CommitLog(store, PREFIX, 1);
        fenced.open(0, 0);
        fenced.commitAll(List.of(request("pod1", 0, "seg/1/0", streamOfTerm(1))));
        long barrier = fenced.nextSequence() - 1;
        // ⚠️ WRITTEN AFTER THE BARRIER by a leader that does not yet know it is
        // fenced. Its successor's CONTINUE names the slot above, so this is a
        // discarded suffix and must never enter the history.
        fenced.commitAll(List.of(request("pod1", 1, "seg/1/zombie", streamOfTerm(1))));

        Checkpoint atTheBarrier = new Checkpoint(barrier,
                Map.of(streamOfTerm(1), new StreamOffsets(0, 0)), Map.of("pod1", 0L));
        byte[] pointer = atTheBarrier.encode();
        store.put(new LogKeys(PREFIX, 1).latestCheckpointKey(),
                new Body(pointer.length, () -> new java.io.ByteArrayInputStream(pointer)));

        CommitLog successor = new CommitLog(store, PREFIX, 2);
        successor.open(1, barrier);

        assertThat(successor.nextOffset(streamOfTerm(1)))
                .as("the entry past the barrier is a discarded suffix and is NOT folded in")
                .isEqualTo(1);
    }

    @Test
    void theBOUNDEDCrossingSTOPSAtTheBarrierReachedThroughAnUncheckpointedEpoch()
            throws Exception {
        // ⚠️ THE SIBLING CALL SITE. The walk's own probe, after following a
        // CONTINUE, passes `limit` rather than the first hop's `upTo` -- and
        // widening THAT one to MAX_VALUE left the whole build green, because the
        // test above only reaches the first hop. Measured: a fenced ancestor's
        // discarded suffix then lands in a takeover's offsets.
        // ⚠️ AN UNCHECKPOINTED EPOCH IN BETWEEN is what routes the walk through
        // that site -- the row's own "term shorter than K above terms that did
        // checkpoint".
        MemoryBinStore store = new MemoryBinStore();
        CommitLog fenced = new CommitLog(store, PREFIX, 1);
        fenced.open(0, 0);
        fenced.commitAll(List.of(request("pod1", 0, "seg/1/0", streamOfTerm(1))));
        long barrier = fenced.nextSequence() - 1;
        fenced.commitAll(List.of(request("pod1", 1, "seg/1/zombie", streamOfTerm(1))));

        Checkpoint atTheBarrier = new Checkpoint(barrier,
                Map.of(streamOfTerm(1), new StreamOffsets(0, 0)), Map.of("pod1", 0L));
        byte[] pointer = atTheBarrier.encode();
        store.put(new LogKeys(PREFIX, 1).latestCheckpointKey(),
                new Body(pointer.length, () -> new java.io.ByteArrayInputStream(pointer)));

        // Epoch 2 commits nothing and checkpoints nothing, so the walk must
        // follow its CONTINUE down to epoch 1 and probe there.
        CommitLog middle = new CommitLog(store, PREFIX, 2);
        middle.open(1, barrier);

        CommitLog successor = new CommitLog(store, PREFIX, 3);
        successor.open(2, middle.nextSequence());

        assertThat(successor.nextOffset(streamOfTerm(1)))
                .as("the zombie above epoch 1's barrier is not folded in, even when the "
                        + "walk reaches that checkpoint through an uncheckpointed epoch")
                .isEqualTo(1);
    }

    @Test
    void anUNCHECKPOINTEDAncestryStillRecoversCorrectly() throws Exception {
        // ⚠️ THE FALLBACK MUST STAY CORRECT. Chains written before this change,
        // and any term shorter than K, carry no checkpoint at all -- recovery
        // then walks the ancestry exactly as M4.6e does.
        // ⚠️ DISTINCT STREAM PER TERM, for the reason `request` records: with a
        // single shared stream this test passed against a mutant that stopped
        // the walk after ONE ancestor, because absolute offsets in the newest
        // ancestor's entries already carried the answer.
        MemoryBinStore backing = new MemoryBinStore();
        for (int t = 0; t < 3; t++) {
            LocalSequencer s = LocalSequencer.start(backing, PREFIX, manager(backing, "pod" + t), 8)
                    .orElseThrow();
            for (int i = 0; i < 4; i++) {
                s.commit(request("pod" + t, i, "seg/" + t + "/" + i, streamOfTerm(t)));
            }
            s.close();
        }

        LocalSequencer next =
                LocalSequencer.start(backing, PREFIX, manager(backing, "last"), 8).orElseThrow();
        try {
            assertThat(next.commit(request("last", 0, "seg/last", streamOfTerm(0)))
                            .allRuns().get(0).firstOffset())
                    .as("no checkpoint anywhere, so the TRANSITIVE walk must reach term 0")
                    .isEqualTo(4);
        } finally {
            next.close();
        }
    }
}
