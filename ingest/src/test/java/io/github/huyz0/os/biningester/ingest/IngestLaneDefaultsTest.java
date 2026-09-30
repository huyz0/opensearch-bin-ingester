// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * {@link Ingest}'s lane defaults, for an implementation that schedules no
 * lanes (M11.9, H4; M10 review F6, M10.6 review T5 and T6): lane 0 passes
 * through, any other lane is REFUSED as a placement -- never demoted to lane
 * 0 while answering 202 -- and {@code acceptsLane} says so before any work.
 * Both real implementations override these, which is exactly why a fake
 * that inherits them can hide a lane dropped on the way.
 */
class IngestLaneDefaultsTest {

    /** Implements only the lane-less forms, as a fake that forgot lanes does. */
    private static final class Laneless extends ForwardingIngest {
        final AtomicInteger appends = new AtomicInteger();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) {
            appends.incrementAndGet();
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

    @Test
    void laneZeroPassesThroughAndAnyOtherLaneIsRefusedAsAPlacement() throws Exception {
        Laneless ingest = new Laneless();

        ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0, one());
        assertThat(ingest.appends).as("lane 0 reaches the lane-less append").hasValue(1);
        for (byte lane : new byte[] {2, -1, Byte.MIN_VALUE}) {
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, lane,
                    one()))
                    .as("⚠️ LANE %d REFUSED, NOT DEMOTED TO 0 WITH A 202", lane)
                    .isInstanceOf(PlacementRefusedException.class)
                    .hasMessageContaining("lane " + lane);
            assertThatThrownBy(() -> ingest.appendRouted(IngestTestSupport.PRINCIPAL, "logs",
                    "tenant", lane, one()))
                    .isInstanceOf(PlacementRefusedException.class)
                    .hasMessageContaining("lane " + lane);
        }
        assertThat(ingest.appends).as("nothing refused reached the append").hasValue(1);
        assertThatThrownBy(() -> ingest.appendRouted(IngestTestSupport.PRINCIPAL, "logs",
                "tenant", (byte) 0, one()))
                .as("routed lane 0 reaches the routed default, which has no catalog")
                .isInstanceOf(PlacementRefusedException.class)
                .hasMessageContaining("no index catalog");
    }

    @Test
    void acceptsLaneSaysLaneZeroOnly() {
        Laneless ingest = new Laneless();
        assertThat(ingest.acceptsLane((byte) 0)).isTrue();
        assertThat(ingest.acceptsLane((byte) 1)).isFalse();
        assertThat(ingest.acceptsLane((byte) -1)).isFalse();
    }
}
