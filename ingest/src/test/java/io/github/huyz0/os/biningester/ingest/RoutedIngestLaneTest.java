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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The routing layer carries the producer's lane to the ingest it wraps
 * (M10.6, ADR-0074) -- on every one of its three paths, because a lane dropped
 * on any one of them is a request silently demoted to 0.
 */
class RoutedIngestLaneTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "logs-000001"));

    /** Records the lane each append arrived with. */
    private static final class LaneRecorder implements Ingest {
        final List<Byte> lanes = new CopyOnWriteArrayList<>();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws IOException {
            return append(principal, index, partition, (byte) 0, records);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records) throws IOException {
            int[] n = {0};
            records.forEachRecord(r -> n[0]++);
            lanes.add(lane);
            return new AppendResult(n[0], 0L, n[0] - 1L);
        }

        @Override
        public boolean acceptsLane(byte lane) {
            return LaneSet.defaults().contains(lane);
        }

        @Override
        public void close() {
        }
    }

    private static Ingest.RecordSource one() {
        return sink -> sink.accept(new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                new byte[] {'{', '}'}));
    }

    private static RoutedIngest routed(LaneRecorder delegate) {
        Duration timeout = Duration.ofSeconds(10);
        return new RoutedIngest(delegate, new IndexCatalog(),
                new PendingPool(Clock.systemUTC(), timeout, 1 << 20), timeout,
                Clock.systemUTC());
    }

    private static IndexRegistration logs() {
        return new IndexRegistration(UUID.randomUUID().toString(), "logs-000001", List.of("logs"), 8, 8, 1,
                1);
    }

    @Test
    void theLaneSurvivesTheExplicitTheRoutedAndTheWaitingPath() throws Exception {
        LaneRecorder delegate = new LaneRecorder();
        RoutedIngest routed = routed(delegate);

        CompletableFuture<AppendResult> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return routed.appendRouted(PRINCIPAL, "logs", "tenant-a", (byte) -2, one());
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.pendingBatches() == 0 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        routed.register(logs());
        waiting.get(10, TimeUnit.SECONDS);

        routed.appendRouted(PRINCIPAL, "logs", "tenant-b", (byte) 1, one());
        routed.append(PRINCIPAL, "logs", 3, (byte) 2, one());

        assertThat(delegate.lanes).containsExactly((byte) -2, (byte) 1, (byte) 2);
    }
}
