// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * An inactive lane is refused by the routing layer BEFORE the pending pool
 * (M10.6, ADR-0074, M10 criterion 8).
 *
 * <p>⚠️ ON THE WAITING PATH ABOVE ALL: a routed write to an index not yet
 * registered is pooled and held for the registration timeout. Pooled with an
 * inactive lane it spends pool space the genuine waiters share and ends as a
 * 503, which a producer retries for ever, for a request that could only ever
 * be a 400.
 */
class RoutedIngestInactiveLaneTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));

    /**
     * Schedules lanes -2..2 and records nothing: nothing should reach it.
     *
     * <p>⚠️ IT OVERRIDES THE LANE-TAKING APPEND, and takes any lane it is
     * handed (M11.9, H4; M10.6 review T4): inheriting {@code Ingest}'s default,
     * which refuses any lane but 0 itself, a {@code RoutedIngest} that dropped
     * its own check was covered by the fake's.
     */
    private static final class ActiveMinusTwoToTwo implements Ingest {
        int appends;

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) {
            appends++;
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records) {
            appends++;
            return new AppendResult(1, 0L, 0L);
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

    @Test
    void anInactiveLaneOnAnUnregisteredIndexIsRefusedAtOnceAndPoolsNothing() throws IOException {
        ActiveMinusTwoToTwo delegate = new ActiveMinusTwoToTwo();
        Duration longWait = Duration.ofSeconds(45);
        IndexCatalog catalog = new IndexCatalog();
        RoutedIngest routed = new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), longWait, 1 << 20), longWait,
                Clock.systemUTC());

        long start = System.nanoTime();
        assertThatThrownBy(() -> routed.appendRouted(PRINCIPAL, "logs", "tenant-a", (byte) 3,
                one()))
                .isInstanceOf(PlacementRefusedException.class)
                .hasMessageContaining("lane 3");
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .as("refused, not held for the registration timeout")
                .isLessThan(Duration.ofSeconds(30));
        assertThat(routed.pendingBatches()).isZero();

        // ⚠️ A REGISTERED index for the explicit form: an unregistered one now
        // waits for its registration (M10.30), and the lane must be the only
        // reason left to refuse.
        catalog.register(new io.github.huyz0.os.biningester.format.IndexRegistration(
                "AAAAAAAAQACAAAAAAAAAqg", "metrics", java.util.List.of(), 4, 4, 1, 1));
        assertThatThrownBy(() -> routed.append(PRINCIPAL, "metrics", 1, (byte) -3, one()))
                .isInstanceOf(PlacementRefusedException.class)
                .hasMessageContaining("lane -3");
        assertThat(delegate.appends).as("nothing reached the ingest").isZero();
        assertThat(routed.acceptsLane((byte) 2)).as("and the query is the delegate's").isTrue();
    }
}
