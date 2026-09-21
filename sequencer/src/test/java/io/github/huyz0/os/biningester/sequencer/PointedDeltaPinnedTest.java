// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.Checkpoint.PodState;
import io.github.huyz0.os.biningester.format.Checkpoint.StreamOffsets;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Which chain objects GC may collect (M7.7, FR-9, ADR-0033, ADR-0036).
 *
 * <p>⚠️ TWO WAYS TO GET THIS WRONG, AND NEITHER THROWS. Delete the delta a
 * checkpoint pod slot POINTS AT and a detected replay stops being answerable —
 * ADR-0036's criterion 6 — for a pointer that, since M5.55, can be of ANY age,
 * because inherited pointers ride in every checkpoint and are re-propagated by
 * each successor. Delete a delta whose segments are still RETAINED and those
 * segments become unlocatable: ADR-0033 keeps the offset-to-segment index in
 * the deltas and deliberately out of the checkpoint, so a consumer reading an
 * offset inside the retention window gets nothing. Both leave every
 * delete-side assertion green.
 */
class PointedDeltaPinnedTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final String PREFIX = "bucket";
    private static final long EPOCH = 3;

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);
    private final LogKeys keys = new LogKeys(PREFIX, EPOCH);

    /** Writes a delta at {@code sequence} naming one segment, and returns it. */
    private ChainGc.DeltaAt writeDelta(long sequence, String segmentKey) throws Exception {
        CommitDelta delta = new CommitDelta(sequence, List.of(new SegmentCommit(segmentKey,
                List.of(new RunCommit(STREAM, 5, sequence * 5)),
                new SegmentCommit.Attribution("poda", "i1", sequence))));
        backing.put(keys.keyFor(sequence), Body.ofBytes(delta.encode()));
        return new ChainGc.DeltaAt(EPOCH, sequence, delta);
    }

    private void writeCheckpoint(long sequence, Checkpoint checkpoint) throws Exception {
        backing.put(keys.checkpointKeyFor(sequence), Body.ofBytes(checkpoint.encode()));
    }

    private static Checkpoint checkpointAt(long sequence, PodState pod) {
        return new Checkpoint(sequence, Map.of(STREAM, new StreamOffsets(1000, 0)),
                Map.of("poda", pod));
    }

    private static ChainGc.CheckpointAt ckpt(long sequence) {
        return new ChainGc.CheckpointAt(EPOCH, sequence);
    }

    private ChainGc gc() {
        return new ChainGc(store, PREFIX, 1000, 2);
    }

    @Test
    void aDeltaWhoseSegmentsAreGONEAndNothingPointsAtIsDELETED() throws Exception {
        ChainGc.DeltaAt delta = writeDelta(1, "bucket/data/seg-1");
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9),
                List.of(delta), Set.of(), List.of());
        assertThat(result.deltasDeleted()).isEqualTo(1);
        assertThat(backing.stat(keys.keyFor(1))).isEmpty();
    }

    @Test
    void aDeltaAPodSlotPOINTSAtIsKEPTEvenWhenEVERYTHINGElseSaysCollect() throws Exception {
        ChainGc.DeltaAt pointed = writeDelta(1, "bucket/data/seg-1");
        Checkpoint newest = checkpointAt(9, new PodState("i1", 42, EPOCH, 1));
        ChainGc.Result result = gc().collect(newest, ckpt(9), List.of(pointed), Set.of(), List.of());
        assertThat(result.deltasDeleted()).isZero();
        assertThat(result.pinnedByPointer()).isEqualTo(1);
        assertThat(backing.stat(keys.keyFor(1)))
                .as("⚠️ THE OBJECT MUST STILL BE THERE, not merely uncounted: ADR-0036 "
                        + "answers a detected replay by READING the pointed delta, and a "
                        + "404 there turns 'always answered' into an exception on a retry "
                        + "that is safe")
                .isNotEmpty();
    }

    @Test
    void aPointerOLDERThanEverythingElseStillPinsIt() throws Exception {
        ChainGc.DeltaAt ancient = writeDelta(1, "bucket/data/seg-1");
        for (long s = 2; s <= 40; s++) {
            writeDelta(s, "bucket/data/seg-" + s);
        }
        Checkpoint newest = checkpointAt(50, new PodState("i1", 42, EPOCH, 1));
        ChainGc.Result result = gc().collect(newest, ckpt(50), List.of(ancient), Set.of(), List.of());
        assertThat(result.deltasDeleted())
                .as("⚠️ PINNING BY AGE PASSES ON A YOUNG CHAIN AND DELETES THE POINTER'S "
                        + "TARGET ON A REAL ONE. Since M5.55 inherited pointers ride in "
                        + "every checkpoint and are re-propagated by each successor, so a "
                        + "pinned delta can be of any age -- forty sequences below the "
                        + "newest checkpoint here")
                .isZero();
        assertThat(backing.stat(keys.keyFor(1))).isNotEmpty();
    }

    @Test
    void aDeltaWhoseSegmentIsSTILLRetainedIsKEPT() throws Exception {
        ChainGc.DeltaAt delta = writeDelta(1, "bucket/data/seg-1");
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9),
                List.of(delta), Set.of("bucket/data/seg-1"), List.of());
        assertThat(result.deltasDeleted())
                .as("⚠️ THE DELTA IS THE ONLY OBJECT THAT SAYS WHICH SEGMENT HOLDS AN "
                        + "OFFSET (ADR-0033 keeps that index OUT of the checkpoint), so "
                        + "collecting it while its segment is retained makes a consumer "
                        + "reading inside the retention window resolve to nothing -- and "
                        + "every delete-side assertion stays green")
                .isZero();
        assertThat(result.holdingRetainedSegments()).isEqualTo(1);
        assertThat(backing.stat(keys.keyFor(1)))
                .as("the object must still be THERE, not merely uncounted -- a consumer "
                        + "reading an offset this delta indexes resolves through it to a "
                        + "segment that is still retained")
                .isNotEmpty();
    }

    @Test
    void ONEStillRetainedSegmentIsEnoughToKeepADeltaNamingSEVERAL() throws Exception {
        CommitDelta many = new CommitDelta(1, List.of(
                new SegmentCommit("bucket/data/seg-a", List.of(new RunCommit(STREAM, 5, 0)),
                        new SegmentCommit.Attribution("poda", "i1", 1)),
                new SegmentCommit("bucket/data/seg-b", List.of(new RunCommit(STREAM, 5, 5)),
                        new SegmentCommit.Attribution("poda", "i1", 2))));
        backing.put(keys.keyFor(1), Body.ofBytes(many.encode()));
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9),
                List.of(new ChainGc.DeltaAt(EPOCH, 1, many)), Set.of("bucket/data/seg-b"),
                List.of());
        assertThat(result.deltasDeleted())
                .as("a delta indexes every segment of its batch, and one surviving segment "
                        + "needs the whole index")
                .isZero();
    }

    @Test
    void aDeltaATOrABOVETheNewestCheckpointIsKEPT() throws Exception {
        ChainGc.DeltaAt atCheckpoint = writeDelta(9, "bucket/data/seg-9");
        ChainGc.DeltaAt above = writeDelta(10, "bucket/data/seg-10");
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9),
                List.of(atCheckpoint, above), Set.of(), List.of());
        assertThat(result.deltasDeleted())
                .as("⚠️ THE CHECKPOINT'S SEQUENCE IS EXCLUSIVE -- the next slot NOT covered "
                        + "by it -- so a reader resumes by replaying FROM it. Collecting "
                        + "the delta AT that sequence drops one delta's offsets, which is "
                        + "invariant I2")
                .isZero();
    }

    @Test
    void aDeltaOfANOTHEREpochIsNOTJudgedByThisChainsPointer() throws Exception {
        CommitDelta delta = new CommitDelta(1, List.of(new SegmentCommit("bucket/data/seg-1",
                List.of(new RunCommit(STREAM, 5, 0)),
                new SegmentCommit.Attribution("poda", "i1", 1))));
        LogKeys otherEpoch = new LogKeys(PREFIX, EPOCH + 1);
        backing.put(otherEpoch.keyFor(1), Body.ofBytes(delta.encode()));
        Checkpoint newest = checkpointAt(9, new PodState("i1", 42, EPOCH, 1));
        // ⚠️ THE CHECKPOINT IS IN THE DELTA's EPOCH (M8.39): a newer epoch's
        // delta is AFTER an older epoch's checkpoint, and not covered by it.
        ChainGc.Result result = gc().collect(newest, new ChainGc.CheckpointAt(EPOCH + 1, 9),
                List.of(new ChainGc.DeltaAt(EPOCH + 1, 1, delta)), Set.of(), List.of());
        assertThat(result.deltasDeleted())
                .as("a pointer is (epoch, sequence), and matching on the sequence alone "
                        + "pins the wrong object across a takeover -- or, worse, fails to "
                        + "pin the right one")
                .isEqualTo(1);
        assertThat(backing.stat(otherEpoch.keyFor(1)))
                .as("⚠️ AND THE OBJECT ITSELF MUST BE GONE, not merely counted. A delta key "
                        + "built from the pass's OWN epoch rather than the delta's deletes "
                        + "a key that does not exist, counts it, and leaves the real "
                        + "object to leak forever -- the delta half of the defect the "
                        + "checkpoint half already pins")
                .isEmpty();
    }

    @Test
    void theNEWESTCheckpointIsNEVERDeleted() throws Exception {
        writeCheckpoint(9, checkpointAt(9, PodState.bare(42)));
        gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(), Set.of(), List.of(new ChainGc.CheckpointAt(EPOCH, 9)));
        assertThat(backing.stat(keys.checkpointKeyFor(9)))
                .as("it is what `LATEST` points at and what bounds every recovery walk; "
                        + "deleting it makes recovery unbounded at best and impossible at "
                        + "worst")
                .isNotEmpty();
    }

    @Test
    void OLDERCheckpointsBeyondTheKeptTailAreCollected() throws Exception {
        for (long s : new long[] {1, 4, 7, 9}) {
            writeCheckpoint(s, checkpointAt(s, PodState.bare(42)));
        }
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(),
                Set.of(), List.of(ckpt(1), ckpt(4), ckpt(7), ckpt(9)));
        assertThat(result.checkpointsDeleted())
                .as("keepNewest = 2, so 9 and 7 stay and 4 and 1 go")
                .isEqualTo(2);
        assertThat(backing.stat(keys.checkpointKeyFor(9))).isNotEmpty();
        assertThat(backing.stat(keys.checkpointKeyFor(7)))
                .as("⚠️ THE TAIL IS NOT DECORATION: a recovery that has already read "
                        + "LATEST and is following it would 404 on a checkpoint deleted "
                        + "between the two requests, and keeping one behind the newest is "
                        + "what makes that race harmless")
                .isNotEmpty();
        assertThat(backing.stat(keys.checkpointKeyFor(4))).isEmpty();
        assertThat(backing.stat(keys.checkpointKeyFor(1))).isEmpty();
    }

    @Test
    void theKEPTTailIsTheNEWESTWhateverOrderTheCallerPassesThemIn() throws Exception {
        for (long s : new long[] {1, 4, 7, 9}) {
            writeCheckpoint(s, checkpointAt(s, PodState.bare(42)));
        }
        gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(), Set.of(),
                List.of(ckpt(9), ckpt(7), ckpt(4), ckpt(1)));
        assertThat(backing.stat(keys.checkpointKeyFor(9)))
                .as("⚠️ DESCENDING IS THE CALLER'S NATURAL ORDER -- a walk BACK from "
                        + "LATEST -- and keeping 'the last keepNewest of the list' over it "
                        + "keeps the OLDEST and deletes the NEWEST, which is the one "
                        + "object recovery cannot lose. The sort is what makes the "
                        + "javadoc's 'in any order' true")
                .isNotEmpty();
        assertThat(backing.stat(keys.checkpointKeyFor(7))).isNotEmpty();
        assertThat(backing.stat(keys.checkpointKeyFor(1))).isEmpty();
    }

    @Test
    void aCheckpointOfANOTHEREpochIsDeletedUnderITSOwnKey() throws Exception {
        LogKeys older = new LogKeys(PREFIX, EPOCH - 1);
        backing.put(older.checkpointKeyFor(2),
                Body.ofBytes(checkpointAt(2, PodState.bare(1)).encode()));
        for (long s : new long[] {7, 9}) {
            writeCheckpoint(s, checkpointAt(s, PodState.bare(42)));
        }
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(),
                Set.of(), List.of(new ChainGc.CheckpointAt(EPOCH - 1, 2), ckpt(7), ckpt(9)));
        assertThat(result.checkpointsDeleted()).isEqualTo(1);
        assertThat(backing.stat(older.checkpointKeyFor(2)))
                .as("⚠️ A CHECKPOINT KEY IS PER EPOCH. Building it from one epoch for every "
                        + "checkpoint deletes a key that does not exist, counts it as "
                        + "collected, and leaves the real object behind -- a leak that "
                        + "grows with every takeover")
                .isEmpty();
    }

    @Test
    void aPARTIALFailureAttributesITSCountsToTheRightKind() throws Exception {
        ChainGc.DeltaAt one = writeDelta(1, "bucket/data/seg-1");
        ChainGc.DeltaAt two = writeDelta(2, "bucket/data/seg-2");
        for (long s : new long[] {3, 4, 7, 9}) {
            writeCheckpoint(s, checkpointAt(s, PodState.bare(42)));
        }
        BinStore firstCallFails = new ForwardingBinStore(backing) {
            private int calls;

            @Override
            public void delete(List<String> deleting) throws java.io.IOException {
                if (calls++ == 0) {
                    throw new java.io.IOException("the store is unreachable");
                }
                backing.delete(deleting);
            }
        };
        ChainGc.Result result = new ChainGc(firstCallFails, PREFIX, 1, 2)
                .collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(one, two), Set.of(),
                        List.of(ckpt(3), ckpt(4), ckpt(7), ckpt(9)));
        assertThat(result.deltasDeleted())
                .as("⚠️ A PARTIAL FAILURE LANDS WHEREVER THE STORE PUT IT, so deriving one "
                        + "count by subtracting the other from a total mis-attributes it: "
                        + "review MEASURED a first-batch failure reporting TWO deltas "
                        + "deleted and no checkpoints, while one delta was still in the "
                        + "bucket and a checkpoint had gone")
                .isEqualTo(1);
        assertThat(result.checkpointsDeleted()).isEqualTo(2);
        assertThat(backing.stat(keys.keyFor(1))).isNotEmpty();
        assertThat(backing.stat(keys.keyFor(2))).isEmpty();
    }

    @Test
    void aKeepNewestOfONEIsREFUSED() {
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> new ChainGc(store, PREFIX, 1000, 1))
                .as("keeping only the newest leaves a recovery that read LATEST a request "
                        + "ago following a pointer to an object this pass just deleted -- "
                        + "and the whole keep-a-tail argument rests on this refusal")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keepNewest");
    }

    @Test
    void aNONPOSITIVEDeleteBatchIsREFUSED() {
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> new ChainGc(store, PREFIX, 0, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPassOverNOTHINGCostsNOTHING() {
        ChainGc.Result result = gc().collect(checkpointAt(9, PodState.bare(42)), ckpt(9), List.of(),
                Set.of(), List.of());
        assertThat(result.deltasDeleted()).isZero();
        assertThat(store.counts().total())
                .as("GC runs on an interval forever, so a pass with nothing to collect must "
                        + "issue no request at all")
                .isZero();
    }

    @Test
    void aPassIssuesNOListAndNOGetAndBATCHESItsDeletes() throws Exception {
        List<ChainGc.DeltaAt> chain = new ArrayList<>();
        for (long s = 1; s <= 2500; s++) {
            chain.add(writeDelta(s, "bucket/data/seg-" + s));
        }
        long putsBefore = store.counts().puts();
        ChainGc.Result result = gc().collect(checkpointAt(3000, PodState.bare(42)), ckpt(3000),
                chain, Set.of(), List.of());
        assertThat(result.deltasDeleted()).isEqualTo(2500);
        assertThat(store.counts().lists()).isZero();
        assertThat(store.counts().gets())
                .as("the caller already holds the chain it replayed; re-reading each delta "
                        + "to delete it doubles the cost of every recovery")
                .isZero();
        assertThat(store.counts().deletes()).isEqualTo(3);
        assertThat(store.counts().puts() - putsBefore).isZero();
    }

    @Test
    void aFAILEDBatchLeavesTheObjectsAndIsNotCounted() throws Exception {
        ChainGc.DeltaAt delta = writeDelta(1, "bucket/data/seg-1");
        ChainGc refusing = new ChainGc(new FailingDeleteStore(backing), PREFIX, 1000, 2);
        ChainGc.Result result = refusing.collect(checkpointAt(9, PodState.bare(42)), ckpt(9),
                List.of(delta), Set.of(), List.of());
        assertThat(result.deltasDeleted()).isZero();
        assertThat(backing.stat(keys.keyFor(1))).isNotEmpty();
    }

    /** A store whose delete always refuses. */
    private static final class FailingDeleteStore extends ForwardingBinStore {
        FailingDeleteStore(io.github.huyz0.os.biningester.binstore.BinStore delegate) {
            super(delegate);
        }

        @Override
        public void delete(List<String> keys) throws java.io.IOException {
            throw new java.io.IOException("the store is unreachable");
        }
    }
}
