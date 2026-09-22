// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A takeover reads the chain BELOW what recovery replayed, once (M8.42, FR-9).
 *
 * <p>⚠️ **A REPLAY STOPS AT A CHECKPOINT, AND A CHECKPOINT IS NOT THE SEGMENT
 * INDEX** (ADR-0033), so a new term's keep list lacked every delta below it:
 * a segment named only there was never judged, and the orphan sweep had to
 * stay out of every hour before the term began or it would delete committed
 * data. What survives below the boundary is exactly what matters, because
 * chain GC deletes a delta only once every segment it names is gone.
 *
 * <p>⚠️ **THE COST IS PER TAKEOVER, NEVER PER PASS**: one LIST per page of the
 * chain prefix and one GET per surviving chain entry below the boundary, paid
 * here and not again.
 */
class ChainBackfillTest {

    private static final String PREFIX = "bucket";
    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    private final MemoryBinStore store = new MemoryBinStore();

    private CommitDelta commitDelta(long epoch, long sequence) {
        return new CommitDelta(sequence, List.of(new SegmentCommit(
                "bucket/data/seg-" + epoch + "-" + sequence,
                List.of(new RunCommit(STREAM, 1, sequence)),
                new SegmentCommit.Attribution("poda", "i1", sequence))));
    }

    private void delta(long epoch, long sequence) throws Exception {
        CommitDelta delta = commitDelta(epoch, sequence);
        store.put(new LogKeys(PREFIX, epoch).keyFor(sequence), Body.ofBytes(delta.encode()));
    }

    private void chain() throws Exception {
        // epoch 3: CONTINUE, two commits, a SEAL; epoch 4: CONTINUE and commits
        store.put(new LogKeys(PREFIX, 3).keyFor(0),
                Body.ofBytes(new Continue(0, 2, 9).encode()));
        delta(3, 1);
        delta(3, 2);
        store.put(new LogKeys(PREFIX, 3).keyFor(3), Body.ofBytes(new Seal(3, 4).encode()));
        store.put(new LogKeys(PREFIX, 4).keyFor(0),
                Body.ofBytes(new Continue(0, 3, 3).encode()));
        delta(4, 1);
        delta(4, 2);
        delta(4, 3);
        // a checkpoint lives under the same prefix and is not a chain entry
        store.put(new LogKeys(PREFIX, 4).checkpointKeyFor(2),
                Body.ofBytes(new Checkpoint(2, Map.of(), Map.of()).encode()));
    }

    @Test
    void itReturnsEVERYSurvivingCommitBELOWTheBoundaryACROSSEpochsInOrder() throws Exception {
        chain();

        List<ChainGc.DeltaAt> below = ChainBackfill.below(store, PREFIX, 4, 2);

        assertThat(below).extracting(d -> d.epoch() + "/" + d.sequence())
                .as("⚠️ COMMITS ONLY -- no CONTINUE, no SEAL, no checkpoint -- strictly below "
                        + "(4, 2), the predecessor's first")
                .containsExactly("3/1", "3/2", "4/1");
    }

    @Test
    void aDeltaCHAINGCHasDeletedIsSimplyNotThere() throws Exception {
        chain();
        store.delete(List.of(new LogKeys(PREFIX, 3).keyFor(1)));

        assertThat(ChainBackfill.below(store, PREFIX, 4, 2))
                .extracting(d -> d.epoch() + "/" + d.sequence())
                .as("a gap is chain GC's work, not a stop: every delta after it is still read")
                .containsExactly("3/2", "4/1");
    }

    @Test
    void itCostsONEListPerPageAndONEGetPerEntryBelowTheBoundaryAndNOTHINGMore() throws Exception {
        chain();
        CountingBinStore counted = new CountingBinStore(store);

        List<ChainGc.DeltaAt> below = ChainBackfill.below(counted, PREFIX, 4, 2);

        assertThat(counted.counts().lists())
                .as("one page holds this whole chain").isEqualTo(1);
        assertThat(below).hasSize(3);
        assertThat(counted.counts().gets())
                .as("⚠️ ONE GET PER CHAIN ENTRY BELOW THE BOUNDARY -- the three commits, "
                        + "epoch 3's CONTINUE and SEAL, epoch 4's CONTINUE, which no key tells "
                        + "apart -- and none for a key at or after it, none for a checkpoint")
                .isEqualTo(6);
        assertThat(counted.counts().puts() + counted.counts().deletes()).isZero();
    }

    @Test
    void aGENESISBoundaryReadsNothingButTheLIST() throws Exception {
        chain();
        CountingBinStore counted = new CountingBinStore(store);

        assertThat(ChainBackfill.below(counted, PREFIX, 0, 0)).isEmpty();
        assertThat(counted.counts().gets()).isZero();
    }

    @Test
    void backfillStopsWhenTermIsDeposedBeforeReading() throws Exception {
        chain();
        AtomicBoolean serving = new AtomicBoolean(true);
        AtomicInteger gets = new AtomicInteger();
        BinStore deposing = new ForwardingBinStore(store) {
            @Override
            public ListPage list(String prefix, String startAfter, int maxKeys) throws java.io.IOException {
                ListPage page = super.list(prefix, startAfter, maxKeys);
                serving.set(false);
                return page;
            }

            @Override
            public java.io.InputStream get(String key) throws java.io.IOException {
                gets.incrementAndGet();
                return super.get(key);
            }
        };
        ChainMemory memory = new ChainMemory(100);
        memory.record(4, commitDelta(4, 2));

        var method = ChainBackfill.class.getMethod("inBackground", BinStore.class,
                String.class, ChainMemory.class, java.util.function.BooleanSupplier.class);
        java.util.function.BooleanSupplier live = serving::get;
        Thread backfill = (Thread) method.invoke(null,
                new Object[] {deposing, PREFIX, memory, live});
        backfill.join(5_000);

        assertThat(backfill.isAlive()).isFalse();
        assertThat(gets).as("a deposed term must not read a listed page").hasValue(0);
        assertThat(memory.snapshot().deltas()).hasSize(1);
        assertThat(memory.snapshot().fromFloor()).isFalse();
    }

    @Test
    void backfillDiscardsTheWholeWalkWhenTermIsDeposedDuringRead() throws Exception {
        chain();
        AtomicBoolean serving = new AtomicBoolean(true);
        AtomicInteger gets = new AtomicInteger();
        BinStore deposing = new ForwardingBinStore(store) {
            @Override
            public java.io.InputStream get(String key) throws java.io.IOException {
                java.io.InputStream in = super.get(key);
                if (gets.incrementAndGet() == 6) {
                    serving.set(false);
                }
                return in;
            }
        };
        ChainMemory memory = new ChainMemory(100);
        memory.record(4, commitDelta(4, 2));

        var method = ChainBackfill.class.getMethod("inBackground", BinStore.class,
                String.class, ChainMemory.class, java.util.function.BooleanSupplier.class);
        java.util.function.BooleanSupplier live = serving::get;
        Thread backfill = (Thread) method.invoke(null,
                new Object[] {deposing, PREFIX, memory, live});
        backfill.join(5_000);

        assertThat(backfill.isAlive()).isFalse();
        assertThat(gets).as("the in-flight read is allowed to finish").hasValue(6);
        assertThat(memory.snapshot().deltas())
                .as("a deposed walk must not apply a partial result").hasSize(1);
        assertThat(memory.snapshot().fromFloor()).isFalse();
    }
}
