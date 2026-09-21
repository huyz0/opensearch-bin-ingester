// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The commit chain, in memory, so a GC pass costs no request (M8.3, M7.25,
 * NFR-3).
 *
 * <p>⚠️ **`List&lt;CommitDelta&gt;` WAS CONSUMED BY THREE CLASSES AND PRODUCED
 * BY NOTHING.** {@code RetentionPass}, {@code SegmentGc} and
 * {@code RetainedOffsets} all take one; no method in any {@code src/main} could
 * produce one, not even expensively — {@code CommitLog.recover()} returns
 * {@code void} and {@code ChainReplay.Result} discards the deltas it reads. So
 * M7's "0 LIST, 0 GET" was true of {@code SegmentGc} and unproven of a PASS,
 * and the cheap wrong answer — reaching for the recovery walk once per pass —
 * costs one LIST per 1,000 deltas plus one GET per delta, for ever.
 *
 * <p>⚠️ **FED FROM `apply`, NOT FROM `commit`**, for the same reason the
 * compaction histogram beside it is: {@code apply} runs on a live commit AND on
 * every entry a recovery replays, so a restarted leader's chain is the one the
 * LOG describes rather than the one this process happened to watch.
 *
 * <p>⚠️ **AND IT IS BOUNDED.** A chain grows for the life of a term, so an
 * unbounded copy is a leak with a slow fuse — the pass that consumes it says
 * how far it got, and what a bound drops is visible rather than silent.
 */
class ChainMemoryTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(RunKey key, int records) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, records);
        return m;
    }

    @Test
    void aCOMMITTEDDeltaIsInTheChainWithoutReadingTheStore() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CountingBinStore counting = new CountingBinStore(store);
            CommitLog log = new CommitLog(counting, "p/", 0);
            CommitDelta first = log.commit("seg-1", counts(new RunKey(A, 0), 3));
            CommitDelta second = log.commit("seg-2", counts(new RunKey(A, 0), 2));

            long before = counting.counts().total();
            ChainMemory.Snapshot chain = log.chain().snapshot();

            assertThat(chain.deltas().stream().map(CommitDelta::sequence))
                    .as("⚠️ IN SEQUENCE ORDER, which is what a GC pass folds")
                    .containsExactly(first.sequence(), second.sequence());
            assertThat(counting.counts().total())
                    .as("⚠️ ZERO REQUESTS. This is the whole row: the alternative in the tree "
                            + "is the recovery walk, at one LIST per 1,000 deltas plus one GET "
                            + "per delta, on every pass for ever")
                    .isEqualTo(before);
            assertThat(chain.complete()).isTrue();
            assertThat(chain.firstSequence()).isZero();
        }
    }

    @Test
    void aSNAPSHOTIsACOPYAndNotTheLIVEList() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog log = new CommitLog(store, "p/", 0);
            log.commit("seg-1", counts(new RunKey(A, 0), 1));
            List<CommitDelta> taken = log.chain().snapshot().deltas();

            // ⚠️ A PASS HOLDS ITS SNAPSHOT WHILE THE WRITER KEEPS COMMITTING.
            // Handing out the live list makes that a ConcurrentModification at
            // best, and at worst a pass that condemns a segment committed after
            // it started -- deleting an object whose records nobody has read.
            log.commit("seg-2", counts(new RunKey(A, 0), 1));

            assertThat(taken).hasSize(1);
            assertThatThrownBy(() -> taken.add(null))
                    .as("⚠️ AND IT IS IMMUTABLE, so a pass cannot edit the source it reads")
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(log.chain().snapshot().deltas()).hasSize(2);
        }
    }

    @Test
    void aRECOVEREDLogRebuildsTheChainFromTheLOGRatherThanFromUptime() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog writer = new CommitLog(store, "p/", 0);
            writer.commit("seg-1", counts(new RunKey(A, 0), 3));
            writer.commit("seg-2", counts(new RunKey(A, 0), 2));

            CommitLog restarted = new CommitLog(store, "p/", 0);
            assertThat(restarted.chain().snapshot().deltas())
                    .as("⚠️ NOTHING BEFORE RECOVERY: a log that has replayed nothing knows "
                            + "nothing, and inventing a chain here would let a GC pass run "
                            + "against a term it has not read")
                    .isEmpty();
            restarted.recover();

            assertThat(restarted.chain().snapshot().deltas().stream()
                    .map(CommitDelta::sequence))
                    .as("⚠️ FED FROM `apply`, so a replay fills it exactly as a live commit "
                            + "does. Without this a restarted leader's first GC pass sees an "
                            + "EMPTY chain and condemns nothing, for as long as the term lasts")
                    .containsExactly(0L, 1L);
        }
    }

    @Test
    void aRECOVERYREBUILDSTheChainRatherThanAddingToWhatIsLeft() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog writer = new CommitLog(store, "p/", 0);
            writer.commit("seg-1", counts(new RunKey(A, 0), 1));
            writer.commit("seg-2", counts(new RunKey(A, 0), 1));
            writer.commit("seg-3", counts(new RunKey(A, 0), 1));

            CommitLog log = new CommitLog(store, "p/", 0);
            log.recover();
            // ⚠️ A PASS HAS RUN AND FORGOTTEN WHAT IT COLLECTED, which is the
            // ordinary state of a chain in a healthy deployment.
            log.chain().forgetThrough(0, 1);
            assertThat(log.chain().snapshot().deltas()).hasSize(1);

            log.recover();

            // ⚠️ CLEARED BEFORE THE REPLAY REFILLS IT. Without that, a replay
            // meets a chain whose newest sequence is already 2 and rejects
            // every earlier delta as a repeat -- so a recovery after a pass
            // would rebuild a chain MISSING its oldest entries while claiming
            // to be complete, and the next pass would move a retained-offset
            // boundary over segments it never saw.
            assertThat(log.chain().snapshot().deltas().stream().map(CommitDelta::sequence))
                    .containsExactly(0L, 1L, 2L);
            assertThat(log.chain().snapshot().firstSequence()).isZero();
        }
    }

    @Test
    void aSEALAndACONTINUEAreNOTDeltasAndDoNotEnterTheChain() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog log = new CommitLog(store, "p/", 0);
            log.commit("seg-1", counts(new RunKey(A, 0), 1));
            log.seal(1, 8);

            // ⚠️ THEY CARRY NO RUNS AND NO SEGMENT KEY, so a pass has nothing
            // to do with them -- and `SegmentGc` would see a delta with no
            // segments and count it as one it examined.
            assertThat(log.chain().snapshot().deltas()).hasSize(1);
        }
    }

    @Test
    void aPASSThatHASConsumedTheChainCanForgetIt() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog log = new CommitLog(store, "p/", 0);
            log.commit("seg-1", counts(new RunKey(A, 0), 1));
            log.commit("seg-2", counts(new RunKey(A, 0), 1));
            log.commit("seg-3", counts(new RunKey(A, 0), 1));

            log.chain().forgetThrough(0, 1);

            ChainMemory.Snapshot left = log.chain().snapshot();
            assertThat(left.deltas().stream().map(CommitDelta::sequence))
                    .as("⚠️ INCLUSIVE, and the boundary is the pass's: what it has already "
                            + "collected cannot be collected again")
                    .containsExactly(2L);
            assertThat(left.firstSequence())
                    .as("⚠️ AND THE SNAPSHOT SAYS WHERE IT NOW STARTS, so a reader cannot "
                            + "mistake a truncated chain for a complete one that begins at 0")
                    .isEqualTo(2L);
            assertThat(left.complete())
                    .as("⚠️ STILL COMPLETE: forgetting what a pass consumed is not losing it")
                    .isTrue();
            assertThat(left.lastEpoch()).isZero();
            assertThat(left.lastSequence())
                    .as("⚠️ AND THE SNAPSHOT NAMES WHAT A PASS SHOULD FORGET NEXT. Without "
                            + "this pair a pass cannot say what it just consumed: the deltas "
                            + "carry no epoch, so a caller holding only the FIRST boundary can "
                            + "forget the predecessor's half of a crossed chain and nothing "
                            + "else -- and re-condemns this term's segments on every pass "
                            + "afterwards")
                    .isEqualTo(2L);
        }
    }

    @Test
    void aCHAINPastItsCapDropsTheOLDESTAndSAYSSo() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog log = new CommitLog(store, "p/", 0, 2);
            log.commit("seg-1", counts(new RunKey(A, 0), 1));
            log.commit("seg-2", counts(new RunKey(A, 0), 1));
            log.commit("seg-3", counts(new RunKey(A, 0), 1));

            ChainMemory.Snapshot chain = log.chain().snapshot();

            assertThat(chain.deltas().stream().map(CommitDelta::sequence))
                    .as("⚠️ THE OLDEST GOES. A chain grows for the life of a term -- one "
                            + "delta per commit interval, for days -- so an unbounded copy is "
                            + "a leak with a slow fuse")
                    .containsExactly(1L, 2L);
            assertThat(chain.complete())
                    .as("⚠️ AND IT SAYS SO. A pass over a chain missing its oldest deltas "
                            + "UNDER-deletes, which is the safe direction -- but a pass that "
                            + "believed the chain complete would move a retained-offset "
                            + "boundary over segments it never saw")
                    .isFalse();
            assertThat(chain.firstSequence()).isEqualTo(1L);
        }
    }

    @Test
    void aDELTARecordedTWICEIsHeldONCE() {
        // ⚠️ THE SEQUENCE IS THE IDENTITY, and a repeat is not an append. A
        // replay refills a chain that `recover` has just cleared, so THAT path
        // cannot double -- but this class is public and the retention loop that
        // drives it (M8.5) holds it across passes. A chain that took the same
        // delta twice hands a pass one segment twice, and a segment condemned
        // twice is a DELETE issued twice: the second against a key that is
        // already gone.
        ChainMemory chain = new ChainMemory(10);
        CommitDelta delta = delta(0, "seg-1");

        chain.record(0, delta);
        chain.record(0, delta);

        assertThat(chain.snapshot().deltas()).hasSize(1);
    }

    @Test
    void aTAKEOVERCarriesTheOLDTermsDeltasIntoTheNEWChain() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            CommitLog first = new CommitLog(store, "p/", 1);
            first.commit("seg-old-1", counts(new RunKey(A, 0), 3));
            first.commit("seg-old-2", counts(new RunKey(A, 0), 2));
            io.github.huyz0.os.biningester.format.Seal seal = first.seal(1, 8);

            // ⚠️ THE ORDER `LocalSequencer.start` USES: recover this chain,
            // then open it against the predecessor's sealed slot.
            CommitLog successor = new CommitLog(store, "p/", 2);
            successor.recover();
            successor.open(1, seal.sequence());
            successor.commit("seg-new", counts(new RunKey(A, 0), 1));

            ChainMemory.Snapshot chain = successor.chain().snapshot();

            // ⚠️ A SUCCESSOR'S OWN CHAIN IS EMPTY AT TAKEOVER, so the crossing
            // walk is the ONLY reader of the predecessor's deltas -- and those
            // segments are exactly what its first retention pass must be able
            // to condemn. Without them every object the old term wrote stays
            // billed for the life of the new one. Review MEASURED both halves
            // of getting this wrong: the crossing dropped its deltas entirely,
            // and a dedupe on the SEQUENCE alone then discarded this term's own
            // delta as a repeat of the predecessor's -- while reporting the
            // chain COMPLETE, which is the one state this class promises
            // cannot happen.
            assertThat(chain.deltas().stream()
                    .flatMap(d -> d.segments().stream())
                    .map(io.github.huyz0.os.biningester.format.SegmentCommit::segmentKey))
                    .containsExactly("seg-old-1", "seg-old-2", "seg-new");
            assertThat(chain.complete()).isTrue();
            assertThat(chain.firstEpoch())
                    .as("⚠️ AND THE SNAPSHOT SAYS WHICH CHAIN IT STARTS IN. A sequence number "
                            + "means nothing without its epoch once a chain has crossed")
                    .isEqualTo(1L);
        }
    }

    @Test
    void aFORGETBoundaryIsReadWithItsEPOCHAndNotTheSequenceAlone() {
        // ⚠️ EVERY DELTA OF AN OLDER EPOCH IS BEFORE EVERY DELTA OF A NEWER
        // ONE, whatever the sequences say. A boundary compared on the sequence
        // alone would keep the predecessor's delta 7 while dropping this
        // term's delta 1 -- holding what was already collected and forgetting
        // what was not.
        ChainMemory chain = new ChainMemory(10);
        chain.record(1, delta(7, "seg-old"));
        chain.record(2, delta(1, "seg-new"));

        chain.forgetThrough(2, 0);

        assertThat(chain.snapshot().deltas().stream()
                .flatMap(d -> d.segments().stream())
                .map(io.github.huyz0.os.biningester.format.SegmentCommit::segmentKey))
                .as("⚠️ THE OLD TERM'S DELTA GOES, because epoch 1 is entirely before epoch 2 "
                        + "-- and the new term's delta 1 stays, because it is after the "
                        + "boundary")
                .containsExactly("seg-new");
    }

    /** One delta naming TWO segments, as a batched commit does. */
    private static CommitDelta twoSegments(long sequence, String first, String second) {
        return new CommitDelta(sequence, List.of(
                new io.github.huyz0.os.biningester.format.SegmentCommit(first,
                        List.of(new io.github.huyz0.os.biningester.format.RunCommit(new RunKey(A, 0), 1, 0)),
                        new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution("poda", "i1", 1)),
                new io.github.huyz0.os.biningester.format.SegmentCommit(second,
                        List.of(new io.github.huyz0.os.biningester.format.RunCommit(new RunKey(A, 1), 1, 0)),
                        new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution("podb", "i2", 1))));
    }

    @org.junit.jupiter.api.Test
    void aDELTAWithOneKEPTSegmentIsNotForgottenThoughItsOTHERWasDeleted() {
        // ⚠️ EVERY SEGMENT, NOT ANY. Review MEASURED "any" surviving every
        // suite, because every delta in the tree had exactly one segment. A
        // batched commit names several; forgetting it because ONE was deleted
        // drops the kept one out of the orphan sweep's keep list, which fails
        // open -- and a committed segment is deleted as an orphan.
        ChainMemory chain = new ChainMemory(100);
        chain.record(1, twoSegments(1, "bins/data/gone", "bins/data/kept"));

        int dropped = chain.forgetCollected(java.util.Set.of("bins/data/gone"));

        org.assertj.core.api.Assertions.assertThat(dropped).isZero();
        org.assertj.core.api.Assertions.assertThat(chain.snapshot().deltas())
                .as("⚠️ STILL HELD: one of its segments is still in the bucket")
                .hasSize(1);
    }

    @org.junit.jupiter.api.Test
    void aFULLYCollectedChainSaysWhereItRESUMESRatherThanZero() {
        // ⚠️ THE EMPTY-CHAIN BOUNDARY, written when a collection empties the
        // chain. Left at (0, 0) it says "nothing has ever been collected" about
        // a chain that has been -- M8.38's defect arriving by a second door.
        ChainMemory chain = new ChainMemory(100);
        chain.record(3, delta(7, "bins/data/a"));
        chain.record(3, delta(8, "bins/data/b"));

        chain.forgetCollected(java.util.Set.of("bins/data/a", "bins/data/b"));

        ChainMemory.Snapshot empty = chain.snapshot();
        org.assertj.core.api.Assertions.assertThat(empty.deltas()).isEmpty();
        org.assertj.core.api.Assertions.assertThat(empty.firstEpoch()).isEqualTo(3);
        org.assertj.core.api.Assertions.assertThat(empty.firstSequence())
                .as("the next delta this chain will hold, not 0").isEqualTo(9);
    }

    private static CommitDelta delta(long sequence, String segmentKey) {
        return new CommitDelta(sequence, List.of(new io.github.huyz0.os.biningester.format.SegmentCommit(segmentKey,
                List.of(new io.github.huyz0.os.biningester.format.RunCommit(new RunKey(A, 0), 1, 0)),
                new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution("poda", "i1", 1))));
    }

    @Test
    void aPASSCanFORGETExactlyWhatTheSNAPSHOTHandedIt() {
        // ⚠️ THE ROUND TRIP THE RETENTION LOOP MAKES (M8.5): take a snapshot,
        // collect it, forget it by the boundary the snapshot itself named. It
        // has to work across a takeover, where the two halves number
        // independently.
        ChainMemory chain = new ChainMemory(10);
        chain.record(1, delta(7, "seg-old"));
        chain.record(2, delta(1, "seg-new"));
        ChainMemory.Snapshot taken = chain.snapshot();

        chain.forgetThrough(taken.lastEpoch(), taken.lastSequence());

        ChainMemory.Snapshot emptied = chain.snapshot();
        assertThat(emptied.deltas())
                .as("⚠️ EVERYTHING THE PASS SAW IS GONE, both epochs of it")
                .isEmpty();
        assertThat(emptied.firstEpoch())
                .as("⚠️ AND AN EMPTY CHAIN STILL SAYS WHERE IT WOULD RESUME. Reporting (0, 0) "
                        + "would say it starts at the beginning of the OLDEST epoch there has "
                        + "ever been -- so a reader would take a chain that has been fully "
                        + "COLLECTED for one that holds everything and has simply never been "
                        + "written to")
                .isEqualTo(2L);
        assertThat(emptied.firstSequence()).isEqualTo(2L);

        chain.record(2, delta(2, "seg-newer"));
        assertThat(chain.snapshot().deltas().stream()
                .flatMap(d -> d.segments().stream())
                .map(io.github.huyz0.os.biningester.format.SegmentCommit::segmentKey))
                .as("⚠️ AND WHAT ARRIVES AFTERWARDS IS STILL TAKEN. A chain that forgot its "
                        + "tail boundary would drop the next delta as a repeat")
                .containsExactly("seg-newer");
    }

    @Test
    void anEPOCHIsNeverNEGATIVE() {
        // ⚠️ A NEGATIVE EPOCH COULD ONLY COME FROM A CORRUPT LEASE, and it
        // would order BEFORE every real one -- so a chain carrying it would be
        // forgotten by any boundary a pass named, and its segments would never
        // be condemned. `CommitLog`'s own constructor refuses one for the same
        // reason.
        assertThatThrownBy(() -> new EpochDelta(-1, delta(0, "seg-1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCAPThatIsNotPositiveIsREFUSED() {
        // ⚠️ A CAP OF ZERO IS A CHAIN THAT IS ALWAYS EMPTY AND ALWAYS CLAIMS TO
        // BE INCOMPLETE, which is a GC that silently never collects -- the
        // failure this whole row exists to make impossible.
        assertThatThrownBy(() -> new ChainMemory(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChainMemory(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
