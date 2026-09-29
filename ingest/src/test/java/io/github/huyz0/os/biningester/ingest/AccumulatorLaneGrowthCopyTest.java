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
 * M12.19c (M11.9 T1): when the per-lane arrays grow past 8, the FIRST EIGHT
 * lanes' entries are carried into the grown arrays. The growth case that
 * existed started its clock at 0 with every lane at the ceiling, so a growth
 * that dropped the first eight -- a fresh array for a copy -- lost nothing it
 * could see. Here the eight are buffered at a non-zero time on lanes due
 * sooner than the ninth, so it is theirs that decides when the buffer is due.
 */
class AccumulatorLaneGrowthCopyTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ac"), 0);
    private static final long CEILING = 5_000;

    /** ⚠️ ADVANCED, never slept on. */
    private static final class TestClock extends Clock {
        private long millis = 1_000;

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
    void theFirstEightLanesSurviveTheGrowthThatTheNinthCauses() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(new IngestConfig(Duration.ofMillis(250), 8L << 20,
                "cluster-a", IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES,
                Duration.ofMillis(CEILING), IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY), clock);
        a.add(KEY, rec(), (byte) 0);
        a.drain();
        assertThat(a.currentInterval()).as("PREMISE: lengthened to the ceiling")
                .isEqualTo(Duration.ofMillis(CEILING));

        for (int lane = 1; lane <= 8; lane++) {
            a.add(KEY, rec(), (byte) lane);
        }
        long dueOfTheEight = a.millisUntilDue();
        assertThat(dueOfTheEight)
                .as("PREMISE: the eight are due sooner than a -1 lane, and not within 100 ms")
                .isBetween(101L, CEILING - 101);
        clock.advance(100);
        a.add(KEY, rec(), (byte) -1); // the ninth distinct lane: the arrays grow here

        assertThat(a.millisUntilDue())
                .as("⚠️ STILL THE EIGHT's DEADLINE, 100 ms nearer: carried through the "
                        + "growth, not dropped for the ninth lane's ceiling")
                .isEqualTo(dueOfTheEight - 100);
    }
}
