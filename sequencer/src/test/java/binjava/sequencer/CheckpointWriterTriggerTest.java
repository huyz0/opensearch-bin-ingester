// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * WHEN a checkpoint is written, and how many objects that costs (M4.8b2).
 *
 * <p>⚠️ ASSERTED BY RECORDED PUT, never by the object appearing. A writer with a
 * constant {@code seq} overwrites one key and satisfies every "it is there"
 * assertion and every LIST; {@link RecordingBinStore} keeps the PUTs as a
 * sequence so a repeat is visible.
 *
 * <p>⚠️ THE ROW'S TWO QUIET-CLUSTER CRITERIA CONTRADICT EACH OTHER. It asks for
 * "the SAME key for a T tick with no deltas between" AND for "a cluster which
 * commits and then stops eventually stops writing": if the writer goes quiet
 * there is no second write to carry the same key. The row settles it itself, by
 * naming "a dirty flag that is never cleared" as a mutant the quiet-cluster test
 * must kill — so it already assumes the flag, and the same-key clause describes
 * a writer without one.
 *
 * <p>⚠️ AN EARLIER VERSION OF THIS PARAGRAPH ARGUED IT FROM NFR-2 AND AN OBJECT
 * COUNT GROWING WITH UPTIME, and both halves were measured false: NFR-2 is zero
 * requests from CONSUMERS, and an unconditional tick rewrites the SAME key while
 * {@code seq} is unchanged, so the PUT count grows and the object count does
 * not. It is corrected here because the production javadoc retracted it and this
 * copy was missed — the duplicate-fact failure this repository keeps repeating.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CheckpointWriterTriggerTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey RA = new RunKey(A, 0);
    private static final Duration T = Duration.ofSeconds(30);

    /** A clock that never fires, so a K test cannot pass for a T reason. */
    private static CheckpointWriter.Ticker frozen() {
        return () -> new CountDownLatch(1).await();
    }

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
     * ⚠️ RENDEZVOUS ON THE TICK BEING PROCESSED, not on handing it over. The
     * writer takes from the queue BEFORE it does the work, so returning from
     * {@code tick()} proves only that the tick was delivered — asserting a PUT
     * count at that moment races the writer and passes against a writer that
     * never writes.
     */
    private static void tickAndAwait(ManualTicker ticker, CheckpointWriter writer) throws Exception {
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

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static void commit(CommitLog log, CheckpointWriter writer, String pod, long flushSeq)
            throws IOException {
        List<CommitRequest> requests =
                List.of(new CommitRequest(pod, "i1", flushSeq, "seg/" + pod + "/" + flushSeq, counts(RA, 2)));
                writer.observe(requests, log.commitAll(requests).sequence());
    }

    private record Fixture(RecordingBinStore store, CommitLog log) { }

    private static Fixture fixture() {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        return new Fixture(store, new CommitLog(store, "bins", 1));
    }

    @Test
    void noCheckpointBeforeTheKthDeltaAndExactlyOneAtIt() throws Exception {
        // ⚠️ SAMPLED AT THE BOUNDARY, both sides, because a count alone misses
        // `K -> K-1` whenever floor(N/K) == floor(N/(K-1)).
        Fixture f = fixture();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 3, T, frozen())) {
            commit(f.log(), writer, "poda", 0);
            commit(f.log(), writer, "poda", 1);
            assertThat(f.store().checkpointKeys())
                    .as("nothing is written before the Kth delta")
                    .isEmpty();

            commit(f.log(), writer, "poda", 2);
            assertThat(f.store().checkpointKeys())
                    .as("exactly one is written AT the Kth, with the clock frozen")
                    .hasSize(1);
        }
    }

    @Test
    void theKTriggerKeepsItsPeriodAcrossSeveralCheckpoints() throws Exception {
        // ⚠️ NO OTHER K TEST COMMITS PAST THE FIRST PERIOD, so deleting
        // `deltasSinceCheckpoint = 0` passes them all: after the first
        // checkpoint the counter stays at or above K and EVERY later commit
        // writes one -- a thousandfold the intended rate at the shipped default.
        Fixture f = fixture();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 2, T, frozen())) {
            for (int delta = 0; delta < 4; delta++) {
                commit(f.log(), writer, "poda", delta);
            }
            assertThat(f.store().checkpointKeys())
                    .as("four deltas at K=2 is two checkpoints, not three and not four")
                    .hasSize(2);
        }
    }

    @Test
    void noCheckpointBeforeTheFirstTickAndExactlyOneAfterIt() throws Exception {
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1_000_000, T, ticker)) {
            commit(f.log(), writer, "poda", 0);
            assertThat(f.store().checkpointKeys())
                    .as("a delta below K writes nothing on its own")
                    .isEmpty();

            tickAndAwait(ticker, writer);
            assertThat(f.store().checkpointKeys())
                    .as("the tick is the other trigger, and it writes exactly one")
                    .hasSize(1);
        }
    }

    @Test
    void theTickTriggerWritesOncePerPeriodWithDeltasBetweenAndNoMore() throws Exception {
        // ⚠️ TWO-SIDED OVER SEVERAL PERIODS: a one-sided bound misses
        // `T.multipliedBy(2)`, which baselines/review.txt records surviving twice.
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1_000_000, T, ticker)) {
            for (int period = 0; period < 3; period++) {
                commit(f.log(), writer, "poda", period);
                tickAndAwait(ticker, writer);
            }
            assertThat(f.store().checkpointKeys())
                    .as("one checkpoint per period, no more and no fewer")
                    .hasSize(3);
        }
    }

    @Test
    void anIdleClusterWritesNothingHoweverManyTicksPass() throws Exception {
        // ⚠️ TIME-DRIVEN, not request-driven: construct-commit-nothing-assert-zero
        // leaves fire-every-T-unconditionally alive, because no tick ever runs.
        // The clock must be advanced past several periods with zero deltas.
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 3, T, ticker)) {
            tickAndAwait(ticker, writer);
            tickAndAwait(ticker, writer);
            tickAndAwait(ticker, writer);

            assertThat(f.store().checkpointKeys())
                    .as("an idle cluster is not a billed cluster (NFR-2)")
                    .isEmpty();
        }
    }

    @Test
    void aClusterThatCommitsThenGoesQuietStopsWriting() throws Exception {
        // ⚠️ THIS FALLS BETWEEN the idle test and "seconds/T": a dirty flag that
        // is set but never CLEARED passes both of those and still writes forever.
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1_000_000, T, ticker)) {
            commit(f.log(), writer, "poda", 0);
            tickAndAwait(ticker, writer);
            assertThat(f.store().checkpointKeys()).hasSize(1);

            tickAndAwait(ticker, writer);
            tickAndAwait(ticker, writer);
            assertThat(f.store().checkpointKeys())
                    .as("no new deltas, so no new checkpoints -- the flag is CLEARED on write")
                    .hasSize(1);
        }
    }

    @Test
    void theCheckpointCountScalesWithDeltasAndNeverWithStreamsOrPods() throws Exception {
        // ⚠️ NON-NEGOTIABLE 6. A per-stream or per-pod PUT is invisible to every
        // other assertion here, and a single-pod stream-varied test cannot see
        // the per-pod form, so BOTH dimensions are varied while the count is held.
        Fixture few = fixture();
        try (CheckpointWriter writer =
                new CheckpointWriter(few.store(), few.log(), "bins", 1, T, frozen())) {
            List<CommitRequest> one =
                    List.of(new CommitRequest("poda", "i1", 0, "seg/a", counts(RA, 1)));
            writer.observe(one, few.log().commitAll(one).sequence());
        }

        Fixture many = fixture();
        Map<RunKey, Integer> wide = new LinkedHashMap<>();
        for (int i = 0; i < 40; i++) {
            wide.put(new RunKey(new UUID(7, i), i), 1);
        }
        try (CheckpointWriter writer =
                new CheckpointWriter(many.store(), many.log(), "bins", 1, T, frozen())) {
            List<CommitRequest> batch = List.of(
                    new CommitRequest("poda", "i1", 0, "seg/a", wide),
                    new CommitRequest("podb", "i1", 0, "seg/b", wide),
                    new CommitRequest("podc", "i1", 0, "seg/c", wide),
                    new CommitRequest("podd", "i1", 0, "seg/d", wide));
            writer.observe(batch, many.log().commitAll(batch).sequence());
        }

        assertThat(many.store().checkpointKeys().size())
                .as("40 streams and 4 pods cost the same PUTs as 1 stream and 1 pod")
                .isEqualTo(few.store().checkpointKeys().size());
    }

    @Test
    void anUNCHECKEDStoreFailureDoesNotTurnTheRateIntoOnePerDelta() throws Exception {
        // ⚠️ THE SIBLING PATH OF THE IOException CASE, and it behaved WORSE. The
        // counter reset lived only in `catch (IOException)`, so an unchecked
        // throw escaped `writeIfDirty`, was swallowed by `observe`, and left the
        // counter at or above K -- after which every commit issued another PUT.
        // Review measured 7 attempted PUTs for 9 deltas at K = 3.
        Fixture f = fixture();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 3, T, frozen())) {
            f.store().failEveryCheckpointPutUnchecked(new IllegalStateException("store is broken"));
            for (int delta = 0; delta < 9; delta++) {
                commit(f.log(), writer, "poda", delta);
            }

            assertThat(f.store().checkpointKeys())
                    .as("nine deltas at K=3 attempt three checkpoints, however many fail")
                    .hasSize(3);
        }
    }

    @Test
    void anUNCHECKEDFailureLeavesTheWriterDirtySoALaterTickStillWrites() throws Exception {
        // ⚠️ THE RATE TEST ABOVE DOES NOT COVER THIS. `catch (RuntimeException e)
        // { dirty = false; }` survives it, because every commit re-arms the
        // flag -- the same insufficiency the checked-path wedge test records for
        // its own sibling. Only a TICK with no commit behind it sees the flag.
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1, T, ticker)) {
            f.store().failEveryCheckpointPutUnchecked(new IllegalStateException("store is broken"));
            commit(f.log(), writer, "poda", 0);
            assertThat(f.store().checkpointKeys()).hasSize(1);

            f.store().stopFailingCheckpointPuts();
            tickAndAwait(ticker, writer);

            assertThat(f.store().checkpointKeys())
                    .as("the unchecked failure left the writer dirty, so the tick retried")
                    .hasSize(2);
        }
    }

    @Test
    void aCheckpointNeverOVERWRITESAnObjectAlreadyAtItsKey() throws Exception {
        // ⚠️ NOTHING DISTINGUISHED `putIfAbsent` FROM `put` before this test, so
        // the SPEC's choice of primitive was unpinned. The reachable case is
        // narrow -- this writer retrying a PUT that reported IOException and had
        // landed -- but the primitive is what makes that retry safe rather than
        // a blind overwrite.
        Fixture f = fixture();
        byte[] sentinel = "already here".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String key = new LogKeys("bins", 1).checkpointKeyFor(1);
        f.store().putIfAbsent(key, new binjava.binstore.Body(sentinel.length,
                () -> new java.io.ByteArrayInputStream(sentinel)));

        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1, T, frozen())) {
            commit(f.log(), writer, "poda", 0);

            // ⚠️ THE SENTINEL SURVIVING IS NOT ENOUGH ON ITS OWN: a writer that
            // PUT somewhere else, or never PUT at all, satisfies it too.
            // Measured -- `pendingSequence = nextSequence() - 1` left this test
            // green. Binding the key is what makes the read below mean anything.
            assertThat(f.store().checkpointKeys())
                    .as("the writer aimed at exactly this key, after the sentinel took it")
                    .containsExactly(key, key);
            try (java.io.InputStream in = f.store().get(key)) {
                assertThat(in.readAllBytes())
                        .as("the object already at the key is left exactly as it was")
                        .isEqualTo(sentinel);
            }
        }
    }

    @Test
    void aZeroTriggerIsRefusedAtConstructionRatherThanAtRuntime() {
        Fixture f = fixture();
        // ⚠️ K = 0 means "every zero deltas", which is a checkpoint per commit at
        // best and a division by zero at worst; T = 0 is a tick loop that never
        // sleeps. Both are configuration errors and belong at construction, where
        // a deployment fails to start rather than billing until someone notices.
        assertThatThrownBy(() ->
                new CheckpointWriter(f.store(), f.log(), "bins", 0, T, frozen()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("everyDeltas");
        assertThatThrownBy(() ->
                new CheckpointWriter(f.store(), f.log(), "bins", 3, Duration.ZERO, frozen()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("everyInterval");
    }

    @Test
    void aFailedCheckpointPutDoesNotWedgeTheWriterForever() throws Exception {
        // ⚠️ THE NAMED ASSERTION IS THAT A LATER TRIGGER STILL WRITES. Swallowing
        // the IOException and never writing again passes any test that merely
        // checks the commit survived, which is why that is not what is asserted.
        // ⚠️ THE RETRY IS DRIVEN BY A TICK, NOT BY ANOTHER COMMIT. A second
        // commit re-arms the dirty flag on its own, so it would pass even if the
        // catch block CLEARED the flag -- which is the line whose comment claims
        // to be load-bearing. Only a tick with no commit behind it tests that.
        Fixture f = fixture();
        ManualTicker ticker = new ManualTicker();
        try (CheckpointWriter writer =
                new CheckpointWriter(f.store(), f.log(), "bins", 1, T, ticker)) {
            f.store().failNextCheckpointPut(new IOException("the store is unreachable"));
            commit(f.log(), writer, "poda", 0);
            assertThat(f.store().checkpointKeys())
                    .as("the failed attempt was issued and recorded")
                    .hasSize(1);

            tickAndAwait(ticker, writer);
            assertThat(f.store().checkpointKeys())
                    .as("the dirty flag survived the failure, so the next TICK retries")
                    .hasSize(2);
        }
    }
}
