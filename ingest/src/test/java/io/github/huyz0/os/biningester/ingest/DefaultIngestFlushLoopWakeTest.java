// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The flush loop waits at most until the earliest lane deadline ONLY WHEN IT
 * COULD FLUSH THEN (M10.7 review): with a waiter and no flush queued.
 *
 * <p>⚠️ WAKES ARE COUNTED AS READS OF THE INJECTED CLOCK, which the loop takes
 * only to compute a deadline. A buffer past its deadline answers 0, so a loop
 * computing it in a state where it cannot flush wakes at the 1 ms minimum --
 * ~1000 times a second, each a clock read -- where the correct loop polls at a
 * quarter of the floor and reads nothing.
 *
 * <p>⚠️ THE WINDOW IS REAL TIME, and that is intrinsic rather than a sleep in
 * disguise (testing.md rule 15): the loop's wait is a {@code Condition} timeout
 * on the JVM's clock, and what is measured is how often it fires while nothing
 * happens. The window ends early once the spin is proven, so the failing build
 * pays little of it.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestFlushLoopWakeTest {

    /** Idle observation window; the floor/4 poll wakes about 3 times in it. */
    private static final long WINDOW_MILLIS = 600;
    /** Generous for the correct loop (0 reads), far under a 1 ms spin (~600). */
    private static final long MAX_READS = 50;

    /** An injected clock that counts its reads and moves only when told. */
    private static final class CountingClock extends Clock {
        final AtomicLong millis = new AtomicLong();
        final AtomicLong reads = new AtomicLong();

        @Override
        public long millis() {
            reads.incrementAndGet();
            return millis.get();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    /** A sequencer whose commit waits on a latch, holding a flush IN FLIGHT. */
    private record Held(Sequencer delegate, CountDownLatch release) implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return delegate.commitAll(requests);
        }

        @Override
        public long epoch() {
            return delegate.epoch();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** A pinned 400 ms interval: every lane's deadline is 400 ms, the poll 100 ms. */
    private static IngestConfig config() {
        return IngestTestSupport.pinnedIntervalConfig(Duration.ofMillis(400), 8L << 20);
    }

    private static SegmentRecord rec() {
        return new SegmentRecord("d", OpType.INDEX, OptionalLong.of(1),
                "{}".getBytes(StandardCharsets.UTF_8));
    }

    /** Clock reads during an idle window; ends early past {@link #MAX_READS}. */
    private static long readsWhileIdle(CountingClock clock) {
        clock.reads.set(0);
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WINDOW_MILLIS);
        while (System.nanoTime() < end && clock.reads.get() <= MAX_READS) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        }
        return clock.reads.get();
    }

    private static CompletableFuture<AppendResult> appendAsync(DefaultIngest ingest,
            int partition) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", partition,
                        sink -> sink.accept(rec()));
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    @Test
    void aRECORDLeftBufferedByAThrowingAppendDoesNOTSpinTheLoop() throws Exception {
        CountingClock clock = new CountingClock();
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(config(), store, IngestTestSupport.PREFIX,
                "pod1", IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                clock, index -> IngestTestSupport.LOGS)) {
            // ⚠️ ONE RECORD BUFFERED AND NOBODY WAITING: the source throws after
            // it, so the append fails and registers no Pending.
            Assertions.assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL,
                    "logs", 0, sink -> {
                        sink.accept(rec());
                        throw new IOException("the body turned out malformed");
                    })).isInstanceOf(IOException.class);
            assertThat(ingest.pendingAppends()).as("PREMISE: no waiter").isZero();
            clock.millis.set(TimeUnit.HOURS.toMillis(1));

            assertThat(readsWhileIdle(clock))
                    .as("past its deadline with no waiter, the loop polls; it does not spin")
                    .isLessThanOrEqualTo(MAX_READS);
        }
    }

    @Test
    void aDUEBufferBehindAFlushINFLIGHTDoesNOTSpinTheLoop() throws Exception {
        CountingClock clock = new CountingClock();
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CountDownLatch release = new CountDownLatch(1);
        try (DefaultIngest ingest = new DefaultIngest(config(), store, IngestTestSupport.PREFIX,
                "pod1", new Held(IngestTestSupport.sequencer(store, "pod1"), release),
                new SubscriptionHub(), clock, index -> IngestTestSupport.LOGS)) {
            CompletableFuture<AppendResult> first = appendAsync(ingest, 0);
            IngestTestSupport.awaitPending(ingest, 1);
            CompletableFuture<Void> flushing = CompletableFuture.runAsync(() -> {
                try {
                    ingest.flushNow();
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!ingest.flushQueued() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(ingest.flushQueued()).as("PREMISE: a flush is in flight").isTrue();

            CompletableFuture<AppendResult> second = appendAsync(ingest, 1);
            IngestTestSupport.awaitPending(ingest, 1);
            clock.millis.set(TimeUnit.HOURS.toMillis(1));

            try {
                assertThat(readsWhileIdle(clock))
                        .as("a due buffer that cannot flush until the one in flight "
                                + "completes waits for its signal; it does not spin")
                        .isLessThanOrEqualTo(MAX_READS);
            } finally {
                release.countDown();
            }
            flushing.get(10, TimeUnit.SECONDS);
            first.get(10, TimeUnit.SECONDS);
            ingest.flushNow();
            second.get(10, TimeUnit.SECONDS);
        }
    }
}
