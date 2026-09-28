// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Both sides of the unflushed ceiling's "a flush will come" guard, on a real
 * {@link DefaultIngest} (M12.3 review T1): the guard is
 * {@code flushes.isQueued() || !pending.isEmpty()}, computed there, which the
 * T0 {@link UnflushedCeilingOverlapTest} replaces with a constant.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class UnflushedCeilingGuardTest {

    /** A 256 KiB segment makes the ceiling its 1 MiB floor (4 segments, ADR-0079). */
    private static final long SEGMENT = 256L << 10;
    private static final byte[] CHUNK = new byte[64 << 10];
    /** Virtual threads, never the ForkJoin common pool a busy suite can starve (M11.25). */
    private static final java.util.concurrent.Executor VIRTUAL =
            task -> Thread.ofVirtual().start(task);

    /**
     * ⚠️ NOBODY WAITS AND NOTHING IS QUEUED (M11.7 P4): a source that throws
     * part-way leaves its records buffered past the ceiling with no waiter to
     * flush them. An append waiting there would wait for ever; it proceeds, and
     * its own records bring the flush.
     */
    @Test
    void anAppendProceedsPastTheCeilingWhenNoFlushWillCome() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = ingest(store)) {
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    (byte) 0, sink -> {
                        for (int i = 0; i < 18; i++) { // 1.125 MiB: past the 1 MiB ceiling
                            sink.accept(record("left-" + i));
                        }
                        throw new IOException("the body turned out malformed");
                    }, () -> { }))
                    .isInstanceOf(IOException.class);
            assertThat(ingest.flushQueued()).as("the premise: nothing queued").isFalse();
            assertThat(ingest.pendingAppends()).as("the premise: nobody waits").isZero();

            appendOne(ingest, "next").get(10, TimeUnit.SECONDS);
        }
    }

    /** ⚠️ A FLUSH IS QUEUED: an append past the ceiling waits for it rather than buffering more. */
    @Test
    void anAppendPastTheCeilingWaitsForTheQueuedFlush() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        MemoryBinStore memory = new MemoryBinStore();
        BinStore held = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        release.await();
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        try (DefaultIngest ingest = ingest(held)) {
            CompletableFuture<AppendResult> first = CompletableFuture.supplyAsync(() -> {
                try {
                    return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                            sink -> {
                                for (int i = 0; i < 18; i++) {
                                    sink.accept(record("first-" + i));
                                }
                            }, () -> { });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, VIRTUAL);
            awaitTrue(ingest::flushQueued, "the premise: the 1.125 MiB flush is queued, held");

            AtomicInteger buffered = new AtomicInteger();
            CompletableFuture<AppendResult> second = CompletableFuture.supplyAsync(() -> {
                try {
                    return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                            sink -> { // a whole segment, so its own records bring the flush
                                for (int i = 0; i < 4; i++) {
                                    sink.accept(record("second-" + i));
                                }
                            }, buffered::incrementAndGet);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, VIRTUAL);

            awaitTrue(ingest::waitingForRoom, "the second append waits for room");
            assertThat(buffered).as("and has buffered nothing").hasValue(0);
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertThat(buffered).hasValue(1);
        }
    }

    private static DefaultIngest ingest(BinStore store) throws IOException {
        return new DefaultIngest(IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER,
                SEGMENT), store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, segmentKey -> { },
                new IndexCostLedger());
    }

    private static CompletableFuture<AppendResult> appendOne(DefaultIngest ingest, String id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                        sink -> sink.accept(record(id)), () -> { });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, VIRTUAL);
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), CHUNK);
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(what + ": not within 10 s");
            }
            Thread.onSpinWait();
        }
    }
}
