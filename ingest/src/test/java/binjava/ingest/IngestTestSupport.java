// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.OpType;
import binjava.format.SegmentRecord;
import binjava.security.Principal;
import binjava.sequencer.LeaseManager;
import binjava.sequencer.Sequencer;
import binjava.sequencer.TestSequencers;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Fixtures shared by the DefaultIngest tests.
 *
 * <p>⚠️ Extracted when {@code DefaultIngestTest} reached the 500-line limit.
 * code-structure.md rule 1: split it, never raise the limit.
 */
final class IngestTestSupport {

    private IngestTestSupport() {
    }

    static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    static final String PREFIX = "bins/cluster-a";
    static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "audit"));

    /**
     * ⚠️ RE-EXPORTED from the shared fixture, not redeclared: a second copy of
     * the TTL is a second thing to change, and
     * {@code closeReleasesTheLeaseSoASuccessorSequencesWithoutWaitingOutTheTtl}
     * names it in its own failure message.
     */
    static final Duration LEASE_TTL = TestSequencers.TTL;

    static final int SEAL_REDRIVE_BUDGET = TestSequencers.SEAL_REDRIVE_BUDGET;

    /** ⚠️ Long enough that the trigger never fires, so a test drives the flush. */
    static final Duration NEVER = Duration.ofHours(1);

    static List<SegmentRecord> docs(int n) {
        List<SegmentRecord> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new SegmentRecord("doc-" + i, OpType.INDEX, OptionalLong.of(1),
                    ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    static DefaultIngest ingest(CountingBinStore store, SubscriptionHub hub,
            Duration flushInterval) throws IOException {
        return new DefaultIngest(pinnedIntervalConfig(flushInterval, 8L << 20),
                store, PREFIX, "pod1", sequencer(store, "pod1"), hub, Clock.systemUTC(),
                index -> LOGS);
    }

    /**
     * A REAL sequencer, deliberately, not {@code FakeSequencer} — see
     * {@link TestSequencers} for the narrow reason: THIS class's own request
     * deltas. Five {@code isEqualTo(4)} assertions in {@code DefaultIngestTest}
     * and one {@code isEqualTo(5)} in {@code IngestShutdownTest} count the commit
     * delta among the objects a flush writes, and a fake writes nothing.
     *
     * <p>⚠️ NOT because of {@code IntervalPutRateTest}, which an earlier draft
     * claimed: that test constructs no {@code Sequencer} and its own javadoc says
     * a commit-log append is a separate request its table excludes.
     *
     * <p>⚠️ The lease object and the chain's opening CONTINUE are written HERE,
     * at construction, so they land before any test captures its baseline. Every
     * request assertion over ingest is a DELTA (`total() - base`) captured INSIDE
     * the try block, which is what makes that safe; an absolute count would have
     * to change.
     */
    static Sequencer sequencer(BinStore store, String podId) throws IOException {
        return TestSequencers.leased(store, PREFIX, podId);
    }

    /** ⚠️ Exposed so a test can attempt a SECOND acquisition against the same store. */
    static LeaseManager leases(BinStore store, String podId) {
        return TestSequencers.leases(store, PREFIX, podId);
    }

    /**
     * ⚠️ On a CALLER-SUPPLIED CLOCK, which any test asserting about expiry must
     * use — a `Clock.fixed` challenger cannot see a held lease lapse, so its
     * empty result means "held" rather than "the machine was quick".
     */
    static LeaseManager leases(BinStore store, String podId, Clock clock) {
        return TestSequencers.leases(store, PREFIX, podId, clock);
    }

    /**
     * ⚠️ M3: {@code IngestConfig} now validates {@code intervalCeiling >=
     * intervalFloor}, and the default ceiling (5 s) is smaller than {@link
     * #NEVER} (1 hour) -- this project's own tests still don't wire any
     * adaptive behaviour (M3.2/M3.3's job), so every fixture here PINS the
     * range at a single value rather than picking an arbitrary ceiling that
     * happens to satisfy the constructor.
     */
    static IngestConfig pinnedIntervalConfig(Duration interval, long maxSegmentBytes) {
        return pinnedIntervalConfig(interval, maxSegmentBytes,
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }

    static IngestConfig pinnedIntervalConfig(Duration interval, long maxSegmentBytes,
            long maxQueuedPushBytes) {
        return new IngestConfig(interval, maxSegmentBytes, "cluster-a", maxQueuedPushBytes,
                interval, IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD,
                IngestConfig.DEFAULT_INTERVAL_LENGTHEN_DELAY,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
    }

    static DefaultIngest ingest(CountingBinStore store) throws IOException {
        return ingest(store, new SubscriptionHub(), NEVER);
    }

    /** Starts an append on its own thread; it will block until a flush carries it. */
    static CompletableFuture<AppendResult> appendAsync(DefaultIngest ingest,
            String index, int partition, int count) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(PRINCIPAL, index, partition, docs(count)::forEach);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /** ⚠️ Spins on a bounded deadline rather than sleeping (testing.md rule 15). */
    static void awaitPending(DefaultIngest ingest, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ingest.pendingAppends() < n) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + ingest.pendingAppends() + " of " + n
                        + " appends reached the accumulator");
            }
            Thread.onSpinWait();
        }
    }

    static AppendResult appendOnce(DefaultIngest ingest, String index, int partition,
            int count) throws Exception {
        CompletableFuture<AppendResult> f = appendAsync(ingest, index, partition, count);
        awaitPending(ingest, 1);
        ingest.flushNow();
        return f.get(10, TimeUnit.SECONDS);
    }

    static void awaitPush(List<SubscriptionHub.Push> seen) throws Exception {
        // ⚠️ The push is submitted off the ingest lock, so it may land just after
        // append returns: append promises DURABILITY, delivery is its own step.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen.isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no push arrived");
            }
            Thread.onSpinWait();
        }
    }

    static byte[] read(CountingBinStore store, String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return in.readAllBytes();
        }
    }
}
