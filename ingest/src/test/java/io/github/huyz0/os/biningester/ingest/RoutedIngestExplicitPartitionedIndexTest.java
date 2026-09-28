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
 * An explicit-partition write waiting for its index is released by a
 * registration with {@code routing_partition_size > 1} (M10.30 review T1):
 * that setting refuses the ROUTED mode only, and an explicit partition needs
 * no hash (SPI § 4b) -- so the explicit wait resolves the index as an explicit
 * write does, never as a routed one.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RoutedIngestExplicitPartitionedIndexTest {

    @Test
    void anExplicitWriteWaitingForAnIndexWithRoutingPartitionSizeOverOneIsWritten()
            throws Exception {
        AtomicInteger written = new AtomicInteger();
        Ingest delegate = new Ingest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource records) throws java.io.IOException {
                records.forEachRecord(r -> written.incrementAndGet());
                return new AppendResult(1, 0L, 0L);
            }

            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    byte lane, RecordSource records, Runnable buffered) throws IOException {
                try { // the removed default's behaviour: buffered once the append returns (M12.2)
                    return append(principal, index, partition, lane, records);
                } finally {
                    buffered.run();
                }
            }

            @Override
            public AppendResult appendRouted(Principal principal, String indexOrAlias,
                    String routing, byte lane, RecordSource records, Runnable buffered)
                    throws IOException {
                try { // the removed default's behaviour: buffered once the append returns (M12.2)
                    return appendRouted(principal, indexOrAlias, routing, lane, records);
                } finally {
                    buffered.run();
                }
            }

            @Override
            public String concreteIndex(String indexOrAlias) {
                return indexOrAlias; // no catalog in this double (M12.2)
            }

            @Override
            public void close() {
            }
        };
        IndexCatalog catalog = new IndexCatalog();
        Duration wait = Duration.ofSeconds(20);
        RoutedIngest routed = new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), wait, 1 << 20), wait, Clock.systemUTC());

        CompletableFuture<AppendResult> result = new CompletableFuture<>();
        Thread writer = Thread.ofPlatform().start(() -> {
            try {
                result.complete(routed.append(IngestTestSupport.PRINCIPAL, "logs", 2, (byte) 0,
                        sink -> sink.accept(new SegmentRecord("a", OpType.INDEX,
                                OptionalLong.of(1), new byte[8]))));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (writer.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(writer.getState()).as("the premise: waiting for the registration")
                .isEqualTo(Thread.State.TIMED_WAITING);
        catalog.register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs", List.of(),
                4, 4, 1, 2));

        result.get(10, TimeUnit.SECONDS);
        assertThat(written)
                .as("⚠️ WRITTEN, NOT REFUSED 400: routing_partition_size bars the routed mode, "
                        + "and this write names its partition")
                .hasValue(1);
    }
}
