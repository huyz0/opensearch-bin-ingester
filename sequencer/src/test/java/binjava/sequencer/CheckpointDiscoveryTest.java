// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.Checkpoint.StreamOffsets;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Finding the newest checkpoint without listing (M4.8c, ADR-0034).
 *
 * <p>⚠️ ZERO {@code list} IS NOT THE REAL BOUND, and asserting it alone passes
 * two wrong implementations the task row names by hand: deriving the cursor from
 * the chain head as a BACKWARD WALK costs zero LISTs and one GET per checkpoint,
 * and {@code stat}-probing the sequence space costs zero LISTs and one stat per
 * probe. The discriminator is TOTAL REQUESTS ACROSS ALL VERBS, CONSTANT in the
 * number of checkpoints — measured at two sizes an order of magnitude apart and
 * asserted EQUAL.
 *
 * <p>⚠️ THE LARGE WORKLOAD IS NOT WHAT KILLS THE NAIVE `list`-AND-TAKE-LAST, and
 * the row is careful to say so: against a strict zero-LIST assertion that dies
 * at ONE checkpoint. What 5,000 buys is defence against the bound being weakened
 * back to a PAGE COUNT — which is the defect M4.0's rounds 2 and 3 each caught,
 * a 50-checkpoint workload fitting inside one 1,000-key page — plus a second
 * sample point for the constant.
 *
 * <p>⚠️ SEEDED AS KEYS, NOT BODIES. At tens of kilobytes each, 5,000 real
 * checkpoints would be a fixture of about 190 MiB. Discovery reads exactly one
 * object, so the rest need only EXIST.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CheckpointDiscoveryTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey RA = new RunKey(A, 0);

    /** A clock the test advances by hand, one tick per call. */
    private static final class ManualTicker implements CheckpointWriter.Ticker {
        private final SynchronousQueue<Object> gate = new SynchronousQueue<>();

        @Override
        public void awaitNextTick() throws InterruptedException {
            gate.take();
        }

        void tick() throws InterruptedException {
            gate.put(new Object());
        }
    }

    /**
     * ⚠️ RENDEZVOUS ON THE TICK BEING PROCESSED, not on handing it over — the
     * same reason {@code CheckpointWriterTriggerTest} gives, kept with the copy
     * because losing it is what makes a later tidy back to a bare
     * {@code ticker.tick()} look harmless. The writer TAKES from the queue
     * before it does the work, so returning from {@code tick()} proves only
     * delivery; asserting there races the writer and passes against one that
     * never retries.
     */
    private static void tickAndAwait(ManualTicker ticker, CheckpointWriter writer)
            throws Exception {
        long before = writer.ticksProcessed();
        ticker.tick();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (writer.ticksProcessed() == before) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("the tick was delivered but never processed");
            }
            Thread.onSpinWait();
        }
    }

    private static Checkpoint checkpointAt(long sequence) {
        return new Checkpoint(sequence, Map.of(RA, new StreamOffsets(sequence * 10, 0)),
                Map.of("poda", sequence));
    }

    private static void put(MemoryBinStore store, String key, byte[] body) throws IOException {
        store.put(key, new Body(body.length, () -> new ByteArrayInputStream(body)));
    }

    /**
     * {@code count} checkpoint KEYS, and the pointer aimed at the newest.
     *
     * <p>⚠️ The seq-keyed objects carry one byte each: nothing reads them, and
     * they exist so an implementation that walks or lists them is SLOWER here
     * and identical everywhere else.
     */
    private static MemoryBinStore seeded(int count) throws IOException {
        MemoryBinStore store = new MemoryBinStore();
        LogKeys keys = new LogKeys("bins", 1);
        for (int i = 0; i < count; i++) {
            put(store, keys.checkpointKeyFor(i), new byte[] {0});
        }
        byte[] newest = checkpointAt(count - 1).encode();
        put(store, keys.checkpointKeyFor(count - 1), newest);
        put(store, keys.latestCheckpointKey(), newest);
        return store;
    }

    @Test
    void discoveryCostsTheSAMEAtFiveHundredCheckpointsAndAtFiveThousand() throws Exception {
        CountingBinStore few = new CountingBinStore(seeded(500));
        // ⚠️ THE RESULT IS ASSERTED, not discarded: an implementation issuing
        // ZERO requests and returning empty satisfies every count below.
        assertThat(CheckpointCursor.newest(few, "bins", 1)).isPresent();
        StoreCounts small = few.counts();

        CountingBinStore many = new CountingBinStore(seeded(5_000));
        assertThat(CheckpointCursor.newest(many, "bins", 1)).isPresent();
        StoreCounts large = many.counts();

        // ⚠️ TOTAL, ALL VERBS. A LIST bound alone admits the backward walk and
        // the stat probe; a GET bound alone admits the LIST.
        assertThat(large.total())
                .as("discovery is constant in the number of checkpoints, ten times over")
                .isEqualTo(small.total());
        assertThat(large.lists())
                .as("and issues no list at all -- AC5's stated bound")
                .isZero();
        assertThat(large.total())
                .as("a small constant, not merely equal to itself")
                .isLessThanOrEqualTo(4);
    }

    @Test
    void discoveryReturnsTheNEWESTCheckpointRatherThanAnyOther() throws Exception {
        MemoryBinStore store = seeded(5_000);

        Optional<Checkpoint> found = CheckpointCursor.newest(store, "bins", 1);

        assertThat(found).isPresent();
        assertThat(found.get().sequence())
                .as("the newest, not the first key under the prefix and not the last one listed")
                .isEqualTo(4_999);
        assertThat(found.get().streams().get(RA).nextOffset()).isEqualTo(49_990);
    }

    @Test
    void discoveryIsEmptyOnAChainThatHasCheckpointedNothing() throws Exception {
        // ⚠️ AN ABSENT POINTER IS NOT AN ERROR: every chain looks like this until
        // its first trigger fires, and `BinStore.get` cannot tell absent from
        // unreachable -- the SPI says so in as many words -- so discovery must
        // ask `stat` rather than catching an IOException and calling it empty.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());

        assertThat(CheckpointCursor.newest(store, "bins", 1)).isEmpty();
        assertThat(store.counts().lists()).isZero();
        assertThat(store.counts().total())
                .as("and costs no more than the warm path")
                .isLessThanOrEqualTo(2);
    }

    @Test
    void thePointerADVANCESAndIsWrittenExactlyOncePerTrigger() throws Exception {
        // ⚠️ "OVERWRITTEN ON EVERY SUCCESSFUL CHECKPOINT" IS THE MECHANISM, and
        // nothing pinned it: the seam test drives ONE commit, which
        // `putIfAbsent` satisfies as well as `put`. Under that mutant the
        // pointer freezes at the epoch's first checkpoint, discovery still costs
        // one stat and one get, every cost assertion stays green -- and M4.9
        // quietly replays the whole term.
        // ⚠️ AND THE POINTER'S OWN PUT RATE IS INVISIBLE to `checkpointKeys()`,
        // which filters `.ckpt`, so a per-stream pointer PUT would breach
        // non-negotiable 6 with the suite green.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, log, "bins", 1, Duration.ofSeconds(30), frozen)) {
            commitOne(log, writer, "poda", 0, 3, 1);
            long afterFirst = CheckpointCursor.newest(store, "bins", 1)
                    .orElseThrow().sequence();

            // ⚠️ THREE STREAMS AND TWO REQUESTS IN ONE DELTA, so the count below
            // separates "once per trigger" from once per stream, per pod AND per
            // request. With one request the last of those was numerically
            // identical to per-trigger and the mutant survived the whole build.
            commitOne(log, writer, "podb", 1, 4, 3, 2);
            long afterSecond = CheckpointCursor.newest(store, "bins", 1)
                    .orElseThrow().sequence();

            assertThat(afterSecond)
                    .as("the pointer moved with the second checkpoint")
                    .isGreaterThan(afterFirst);
            assertThat(afterSecond).isEqualTo(log.nextSequence());
            assertThat(store.pointerKeys())
                    .as("one pointer PUT per trigger -- not per stream, not per pod, not per request")
                    .hasSize(2);
        }
    }

    @Test
    void discoveryReadsITSOwnEpochsPointerAndNotEpochOnes() throws Exception {
        // ⚠️ A HARDCODED EPOCH IN THE READER survives every other test here,
        // because they all run at epoch 1 -- the same mutant M4.8b2's review
        // caught on the WRITER, reproduced one commit later on the reader.
        MemoryBinStore store = new MemoryBinStore();
        put(store, new LogKeys("bins", 1).latestCheckpointKey(), checkpointAt(111).encode());
        put(store, new LogKeys("bins", 2).latestCheckpointKey(), checkpointAt(222).encode());

        assertThat(CheckpointCursor.newest(store, "bins", 2).orElseThrow().sequence())
                .as("epoch 2's own pointer, not epoch 1's")
                .isEqualTo(222);
        assertThat(CheckpointCursor.newest(store, "bins", 1).orElseThrow().sequence())
                .isEqualTo(111);
    }

    @Test
    void theWriterPutsThePointerUnderITSOwnEpochsPrefix() throws Exception {
        // ⚠️ THE THIRD INSTANCE OF ONE MUTANT FAMILY IN THREE COMMITS. M4.8b2's
        // review caught a hardcoded epoch in the checkpoint KEY; this task's
        // round 1 caught it in the READER; and it survived here on the WRITER's
        // POINTER, because `aCheckpointIsWrittenUnderITSOwnEpochsPrefix` asserts
        // on `checkpointKeys()`, which filters `.ckpt` and cannot see `/LATEST`.
        // ⚠️ THE CONSEQUENCE IS SILENT: after any failover the pointer lands
        // under epoch 1's prefix, `newestCheckpoint` for the live epoch returns
        // empty and M4.9 replays the whole term -- while epoch 1's own pointer
        // is clobbered with this epoch's offsets.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog successor = new CommitLog(store, "bins", 2);
        successor.recover();
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, successor, "bins", 1, Duration.ofSeconds(30), frozen)) {
            commitOne(successor, writer, "podb", 0, 2, 1);

            assertThat(store.pointerKeys())
                    .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                    .as("epoch 2's pointer, under epoch 2's prefix")
                    .isEqualTo(new LogKeys("bins", 2).latestCheckpointKey());
            assertThat(CheckpointCursor.newest(store, "bins", 2))
                    .as("and the live epoch's discovery finds it")
                    .isPresent();
        }
    }

    @Test
    void aFailedPOINTERWriteLeavesTheCheckpointUNWRITTENSoALaterTriggerRedoesBoth()
            throws Exception {
        // ⚠️ A CHECKPOINT COSTS TWO PUTs, and ADR-0034 records that the pointer
        // goes first so a half-done checkpoint is retried whole. Nothing could
        // fail the SECOND PUT until this commit added `failNextPointerPut`:
        // both other injectors are `.ckpt`-scoped, and measured, hoisting
        // `written = true` above the pointer PUT survived the entire suite.
        // ⚠️ THE RETRY IS DRIVEN BY A TICK, NOT BY ANOTHER COMMIT, and a first
        // draft of this test used a commit and MEASURED THE MUTANT SURVIVING --
        // the same insufficiency the wedge test on the checked path records,
        // reproduced here. A second commit re-arms `dirty` on its own, so it
        // passes whether or not the failed pointer write left the flag set.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, log, "bins", 1, Duration.ofSeconds(30), ticker)) {
            store.failNextPointerPut(new IOException("the store is unreachable"));
            commitOne(log, writer, "poda", 0, 3, 1);
            assertThat(CheckpointCursor.newest(store, "bins", 1))
                    .as("the pointer never landed, so discovery finds nothing yet")
                    .isEmpty();

            tickAndAwait(ticker, writer);

            assertThat(CheckpointCursor.newest(store, "bins", 1))
                    .as("the checkpoint was NOT marked written, so the next TICK redid both")
                    .isPresent();
        }
    }

    @Test
    void theInvariantCheckerToleratesCheckpointsBesideTheChain() throws Exception {
        // ⚠️ THE LANDMINE M4.13 WOULD HAVE STEPPED ON. `Invariants.readChain`
        // decoded EVERY key under the log prefix, so the simulation suite is
        // green only because checkpointing never triggers in it -- review
        // measured seven simulation tests failing with "not a chain entry: bad
        // magic" the moment a pointer PUT happens per commit. The 1,000-seed run
        // IS this milestone's completion condition.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        // ⚠️ OPENED, not just committed to. A chain whose slot 0 is a delta
        // rather than a CONTINUE is malformed, and the checker says so --
        // `epoch 1 carries committed deltas but never wrote a CONTINUE`. A first
        // draft of this test skipped it and failed on the fixture, which is the
        // checker earning its keep rather than a false alarm.
        log.open(0, 0);
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, log, "bins", 1, Duration.ofSeconds(30), frozen)) {
            commitOne(log, writer, "poda", 0, 3, 1);
            commitOne(log, writer, "poda", 1, 4, 1);
        }

        assertThat(Invariants.checkChain(store, "bins", 1))
                .as("checkpoints and the pointer are not chain entries, and not violations")
                .isEmpty();
    }

    @Test
    void thePointerKeyIsNOTAValidChainEntryKey() {
        // ⚠️ IF IT WERE, THE POINTER WOULD BE THE M4.8b1 OUTAGE ITSELF: a
        // canonical entry key is admitted by `isEntryKey`, taken by `chainEnd`
        // as the chain end, and decoded by `applyChain` -- and its BCKP magic
        // then fails every takeover, forever. Measured: pointing
        // `latestCheckpointKey()` at `keyFor(...)` survives every other test in
        // this commit.
        LogKeys keys = new LogKeys("bins", 1);

        assertThat(keys.isEntryKey(keys.latestCheckpointKey()))
                .as("the recovery filter must refuse the pointer")
                .isFalse();
        assertThat(keys.latestCheckpointKey())
                .as("under the chain's prefix, so the filter is what protects it")
                .startsWith(keys.logPrefix())
                .doesNotEndWith(".delta");
    }

    @Test
    void anUNREACHABLEStoreIsNotReportedAsAChainThatNeverCheckpointed() throws Exception {
        // ⚠️ THIS IS WHY DISCOVERY ASKS `stat` RATHER THAN CATCHING `get`, and
        // nothing pinned it until this test: `BinStore` has no not-found type
        // and defines IOException as "the store is unreachable", so a caught
        // `get` reports an OUTAGE as a chain with no checkpoint. M4.9 would then
        // replay the entire term believing that was the truth -- slow, and
        // exactly the unbounded recovery checkpoints exist to remove.
        // ⚠️ Measured surviving before this test existed.
        RecordingBinStore store = new RecordingBinStore(seeded(3));
        store.failEveryGet(new IOException("the store is unreachable"));

        assertThatThrownBy(() -> CheckpointCursor.newest(store, "bins", 1))
                .as("an outage propagates; it is not silently an empty chain")
                .isInstanceOf(IOException.class);
    }

    /**
     * One commit — one delta — carrying {@code streams} streams.
     *
     * <p>⚠️ THE STREAM COUNT IS A PARAMETER because a pointer PUT issued once
     * per STREAM is invisible to a fixture that only ever commits one. Measured
     * surviving before this took more than RA.
     */
    private static void commitOne(CommitLog log, CheckpointWriter writer, String pod,
            long flushSeq, int records, int streams) throws IOException {
        commitOne(log, writer, pod, flushSeq, records, streams, 1);
    }

    /**
     * ONE commit — one delta — carrying {@code streams} streams across
     * {@code requests} batched requests.
     *
     * <p>⚠️ BOTH COUNTS ARE PARAMETERS because a pointer PUT issued per STREAM
     * and one issued per REQUEST are separately invisible to a fixture that
     * takes one of each. Measured: with a single request per {@code commitAll},
     * a per-request pointer PUT survived the WHOLE BUILD — and the only other
     * test that varies request count asserts on {@code checkpointKeys()}, which
     * filters {@code .ckpt} and cannot see the pointer.
     *
     * <p>⚠️ ONE {@code commitAll} IS ONE DELTA however many requests it batches,
     * which is M4.7's whole point; a pointer PUT per request would undo it.
     */
    private static void commitOne(CommitLog log, CheckpointWriter writer, String pod,
            long flushSeq, int records, int streams, int requests) throws IOException {
        java.util.List<CommitRequest> batch = new java.util.ArrayList<>();
        for (int request = 0; request < requests; request++) {
            Map<RunKey, Integer> counts = new LinkedHashMap<>();
            counts.put(RA, records);
            for (int extra = 1; extra < streams; extra++) {
                counts.put(new RunKey(new UUID(0x5150, extra), extra), records);
            }
            batch.add(new CommitRequest(pod + request, flushSeq, "seg/" + pod + "/" + request,
                    counts));
        }
        java.util.List<CommitRequest> requestList = java.util.List.copyOf(batch);
        log.commitAll(requestList);
        writer.observe(requestList);
    }

    @Test
    void theWriterMAINTAINSThePointerSoDiscoveryFindsWhatItJustWrote() throws Exception {
        // ⚠️ THE SEAM IS NOT THE WIRING. Everything above seeds the pointer by
        // hand and would pass in full if `CheckpointWriter` never wrote one --
        // in which case discovery finds nothing in production, for ever.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        try (CheckpointWriter writer =
                new CheckpointWriter(store, log, "bins", 1, Duration.ofSeconds(30), frozen)) {
            Map<RunKey, Integer> counts = new LinkedHashMap<>();
            counts.put(RA, 3);
            java.util.List<CommitRequest> requests =
                    java.util.List.of(new CommitRequest("poda", 7, "seg/0", counts));
            log.commitAll(requests);
            writer.observe(requests);

            Optional<Checkpoint> found = CheckpointCursor.newest(store, "bins", 1);
            assertThat(found).isPresent();
            assertThat(found.get().sequence()).isEqualTo(log.nextSequence());
            assertThat(found.get().pods()).containsEntry("poda", 7L);
        }
    }
}
