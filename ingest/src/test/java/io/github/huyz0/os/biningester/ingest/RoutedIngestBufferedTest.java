// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The front door's {@code buffered} callback reaches the ingester that
 * buffers on EVERY {@link RoutedIngest} path -- the explicit partition, the
 * routed write to a known index, and the write that waited in the pending
 * pool -- which is the production path the front door is handed (M11.7
 * review T1).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RoutedIngestBufferedTest {

    private static final String UUID_1 = "AAAAAAAAQACAAAAAAAAAqg";

    /** Runs the callback it is handed, and says so: a buffering ingester's part. */
    private static final class Buffering implements Ingest {
        final AtomicInteger forwarded = new AtomicInteger();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) {
            throw new AssertionError("⚠️ THE CALLBACK FORM MUST BE THE ONE CALLED");
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws IOException {
            records.forEachRecord(r -> { });
            forwarded.incrementAndGet();
            buffered.run();
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public boolean acceptsLane(byte lane) {
            return lane == 0;
        }

        @Override
        public void close() {
        }
    }

    private static Ingest.RecordSource one() {
        return sink -> sink.accept(new SegmentRecord("a", OpType.INDEX, OptionalLong.of(1),
                new byte[8]));
    }

    @Test
    void everyPathHandsTheCallbackToTheIngesterThatBuffers() throws Exception {
        Buffering delegate = new Buffering();
        IndexCatalog catalog = new IndexCatalog();
        RoutedIngest routed = new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), Duration.ofSeconds(10), 1 << 20),
                Duration.ofSeconds(10), Clock.systemUTC());
        AtomicInteger told = new AtomicInteger();

        CompletableFuture<AppendResult> pooled = CompletableFuture.supplyAsync(() -> {
            try {
                return routed.appendRouted(IngestTestSupport.PRINCIPAL, "logs", "tenant-1",
                        (byte) 0, one(), told::incrementAndGet);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.pendingBatches() < 1 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(routed.pendingBatches()).as("the premise: waiting in the pool").isEqualTo(1);
        routed.register(new IndexRegistration(UUID_1, "logs", List.of(), 4, 4, 1, 1));
        pooled.get(10, TimeUnit.SECONDS);
        assertThat(told).as("⚠️ THE POOLED WRITE's CALLBACK, once it was placed").hasValue(1);

        routed.appendRouted(IngestTestSupport.PRINCIPAL, "logs", "tenant-2", (byte) 0, one(),
                told::incrementAndGet);
        assertThat(told).as("the routed write to a known index").hasValue(2);

        routed.append(IngestTestSupport.PRINCIPAL, "logs", 3, (byte) 0, one(),
                told::incrementAndGet);
        assertThat(told).as("the explicit partition").hasValue(3);
        assertThat(delegate.forwarded).as("each through the callback form").hasValue(3);
    }
}
