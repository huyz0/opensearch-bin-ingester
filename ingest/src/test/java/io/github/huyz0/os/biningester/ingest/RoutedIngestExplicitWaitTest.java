// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * The explicit-partition path waits for an unknown index's registration and
 * then checks the partition against it -- the same bound a registered index
 * is held to (M10.30).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RoutedIngestExplicitWaitTest {

    private static final class Counting extends ForwardingIngest {
        final AtomicInteger called = new AtomicInteger();
        volatile String index;

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws java.io.IOException {
            called.incrementAndGet();
            this.index = index;
            records.forEachRecord(r -> { });
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public void close() {
        }
    }

    private static Ingest.RecordSource one() {
        return sink -> sink.accept(new SegmentRecord("a", OpType.INDEX, OptionalLong.of(1),
                new byte[8]));
    }

    private static Thread appendIn(RoutedIngest routed, String index, int partition,
            CompletableFuture<AppendResult> result) {
        return Thread.ofPlatform().start(() -> {
            try {
                result.complete(routed.append(IngestTestSupport.PRINCIPAL, index, partition,
                        (byte) 0, one()));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
    }

    private static RoutedIngest routed(Ingest delegate, IndexCatalog catalog, Duration wait) {
        return new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), wait, 1 << 20), wait, Clock.systemUTC());
    }

    @Test
    void aRegistrationArrivingDuringTheWaitIsHeldToItsShardCountAndItsConcreteName()
            throws Exception {
        Counting delegate = new Counting();
        IndexCatalog catalog = new IndexCatalog();
        RoutedIngest routed = routed(delegate, catalog, Duration.ofSeconds(20));

        CompletableFuture<AppendResult> tooHigh = new CompletableFuture<>();
        CompletableFuture<AppendResult> viaAlias = new CompletableFuture<>();
        Thread first = appendIn(routed, "logs", 4, tooHigh);
        Thread second = appendIn(routed, "logs-alias", 3, viaAlias);
        // ⚠️ UNTIL BOTH ARE PARKED ON THE REGISTRATION -- a timed wait is the
        // only one this path has -- not for a length of wall time.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((first.getState() != Thread.State.TIMED_WAITING
                || second.getState() != Thread.State.TIMED_WAITING)
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(first.getState()).isEqualTo(Thread.State.TIMED_WAITING);
        assertThat(second.getState()).isEqualTo(Thread.State.TIMED_WAITING);
        assertThat(delegate.called).as("⚠️ NEITHER IS PASSED THROUGH WHILE THE INDEX IS UNKNOWN")
                .hasValue(0);
        catalog.register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                List.of("logs-alias"), 4, 4, 1, 1));

        assertThatThrownBy(() -> tooHigh.get(10, TimeUnit.SECONDS))
                .cause().isInstanceOf(PlacementRefusedException.class)
                .hasMessageContaining("partition 4");
        viaAlias.get(10, TimeUnit.SECONDS);
        assertThat(delegate.called).as("only the partition that exists").hasValue(1);
        assertThat(delegate.index).as("the concrete name, as a routed write").isEqualTo("logs");
    }

    @Test
    void anIndexThatNeverRegistersIsRefusedAsTheRoutedPathRefusesIt() {
        Counting delegate = new Counting();
        RoutedIngest routed = routed(delegate, new IndexCatalog(), Duration.ofMillis(50));

        assertThatThrownBy(() -> routed.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                one())).isInstanceOf(RegistrationTimeoutException.class)
                .hasMessageContaining("logs");
        assertThat(delegate.called).hasValue(0);
    }
}
