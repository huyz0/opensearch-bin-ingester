// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.OpType;
import binjava.format.SegmentRecord;
import binjava.security.Principal;
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
                store, PREFIX, "pod1", hub, Clock.systemUTC(), index -> LOGS);
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
