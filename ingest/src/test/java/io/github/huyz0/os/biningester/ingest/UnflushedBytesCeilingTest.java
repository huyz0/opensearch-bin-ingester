// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A pod buffers at most {@value DefaultIngest#UNFLUSHED_SEGMENTS} segments'
 * worth of records not yet durable; past it an append WAITS, and resumes when
 * the flush in flight completes (M11.7 review P1, ADR-0079, NFR-6).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class UnflushedBytesCeilingTest {

    /** At the floor exactly: 4 segments = {@link DefaultIngest#MIN_UNFLUSHED_BYTES}. */
    private static final int SEGMENT =
            (int) (DefaultIngest.MIN_UNFLUSHED_BYTES / DefaultIngest.UNFLUSHED_SEGMENTS);

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, int payload,
            AtomicInteger buffered) {
        return append(ingest, payload, buffered, new Thread[1]);
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, int payload,
            AtomicInteger buffered, Thread[] runner) {
        return CompletableFuture.supplyAsync(() -> {
            runner[0] = Thread.currentThread();
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                        sink -> sink.accept(new SegmentRecord("r" + payload + "-"
                                + System.nanoTime(), OpType.INDEX, OptionalLong.empty(),
                                new byte[payload])), buffered::incrementAndGet);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
    }

    private static void await(AtomicInteger count, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (count.get() < n && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(count.get()).isEqualTo(n);
    }

    @Test
    void pastTheCeilingAnAppendWaitsForTheFlushInFlightAndThenBuffers() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger heldPuts = new AtomicInteger();
        BinStore held = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        heldPuts.incrementAndGet();
                        release.await();
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        CountingBinStore store = new CountingBinStore(held);
        AtomicInteger buffered = new AtomicInteger();
        List<CompletableFuture<AppendResult>> appends = new ArrayList<>();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofMillis(20), SEGMENT), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {
            // 1.5 segments: due at once, flushed, and held at its data PUT.
            appends.add(append(ingest, SEGMENT * 3 / 2, buffered));
            await(buffered, 1);
            await(heldPuts, 1);
            // 3 segments more: admitted (1.5 < 4), which takes the pod to 4.5.
            appends.add(append(ingest, SEGMENT * 3, buffered));
            await(buffered, 2);

            Thread[] third = new Thread[1];
            appends.add(append(ingest, 10, buffered, third));
            // ⚠️ UNTIL THE THIRD IS PARKED ON A CONDITION -- the only one an
            // appender awaits in DefaultIngest is the ceiling's -- not merely
            // WAITING, which a thread queued for the lock itself also reads as.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!(third[0] != null && LockSupport.getBlocker(third[0])
                    instanceof AbstractQueuedSynchronizer.ConditionObject)
                    && buffered.get() < 3 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(third[0] != null && LockSupport.getBlocker(third[0])
                    instanceof AbstractQueuedSynchronizer.ConditionObject)
                    .as("the third append is parked at the ceiling").isTrue();
            assertThat(buffered)
                    .as("⚠️ AT 4.5 SEGMENTS NOT DURABLE, THE THIRD WAITS AND BUFFERS NOTHING")
                    .hasValue(2);

            release.countDown();
            await(buffered, 3);
            for (CompletableFuture<AppendResult> a : appends) {
                a.get(30, TimeUnit.SECONDS);
            }
        }
    }
}
