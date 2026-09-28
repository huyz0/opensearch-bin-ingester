// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The per-lane deadline arrays grow past their first 8 slots (M11.9, H4;
 * M10.7 review T5): a caller that bypasses the active set's cap of 8 is not
 * refused by an index out of bounds, and the ninth lane's deadline is kept.
 */
class AccumulatorLaneGrowthTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);
    private static final long FLOOR = 250;
    private static final long CEILING = 5_000;

    /** ⚠️ ADVANCED, never slept on. */
    private static final class TestClock extends Clock {
        private long millis;

        @Override
        public long millis() {
            return millis;
        }

        void advance(long by) {
            millis += by;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
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

    private static SegmentRecord rec() {
        return new SegmentRecord("d", OpType.INDEX, OptionalLong.of(1),
                "{}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aNinthBufferedLaneIsHeldAndItsDeadlineKept() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(new IngestConfig(Duration.ofMillis(FLOOR), 8L << 20,
                "cluster-a", IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES,
                Duration.ofMillis(CEILING), IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY), clock);
        a.add(KEY, rec(), (byte) 0);
        a.drain();
        assertThat(a.currentInterval()).as("PREMISE: lengthened to the ceiling")
                .isEqualTo(Duration.ofMillis(CEILING));

        for (int lane = -8; lane <= -1; lane++) {
            a.add(KEY, rec(), (byte) lane);
        }
        assertThat(a.millisUntilDue()).as("eight negative lanes: due at the ceiling")
                .isEqualTo(CEILING);
        clock.advance(100);
        a.add(KEY, rec(), (byte) 1);

        assertThat(a.millisUntilDue())
                .as("⚠️ THE NINTH LANE, +1, IN THE GROWN SLOT: due at 5000 / 2 from its own "
                        + "append, sooner than the eight at the ceiling")
                .isEqualTo(CEILING / 2);
        clock.advance(CEILING / 2 - 1);
        assertThat(a.isFlushDue()).isFalse();
        clock.advance(1);
        assertThat(a.isFlushDue()).isTrue();
    }
}
