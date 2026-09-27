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
 * Per-lane flush deadlines and their counted cost (M10 criterion 9; ADR-0074
 * decisions 3 and 4), T0 on an injected clock.
 *
 * <p>⚠️ EVERY DEADLINE IS MEASURED FROM THAT LANE'S OLDEST BUFFERED RECORD.
 * The floor is 250 ms and the ceiling 5 s throughout, so a positive lane's
 * {@code interval ÷ 2^l} and the floor that bounds it are different numbers
 * and a mutation dropping either one moves a deadline this file pins.
 */
class AccumulatorLaneTest {

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

    /** ⚠️ A ZERO lengthen delay, so one near-empty drain lengthens to the ceiling. */
    private static IngestConfig config() {
        return new IngestConfig(Duration.ofMillis(FLOOR), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, Duration.ofMillis(CEILING),
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
    }

    private static SegmentRecord rec() {
        return new SegmentRecord("d", OpType.INDEX, OptionalLong.of(1),
                "{}".getBytes(StandardCharsets.UTF_8));
    }

    /** An empty accumulator whose adaptive interval has lengthened to the ceiling. */
    private static Accumulator lengthened(TestClock clock) throws Exception {
        Accumulator a = new Accumulator(config(), clock);
        a.add(KEY, rec(), (byte) 0);
        a.drain();
        assertThat(a.currentInterval())
                .as("PREMISE: the interval lengthened to the ceiling")
                .isEqualTo(Duration.ofMillis(CEILING));
        return a;
    }

    @Test
    void aPOSITIVELaneAloneIsDueAtTheIntervalOver2ToTheLANDNotBefore() throws Exception {
        for (int lane = 1; lane <= 2; lane++) {
            TestClock clock = new TestClock();
            Accumulator a = lengthened(clock);
            long due = CEILING >> lane;
            a.add(KEY, rec(), (byte) lane);
            clock.advance(due - 1);
            assertThat(a.isFlushDue())
                    .as("lane +%d is NOT due one tick before %d ms", lane, due).isFalse();
            clock.advance(1);
            assertThat(a.isFlushDue())
                    .as("lane +%d IS due at exactly interval ÷ 2^%d = %d ms, not the "
                            + "adaptive %d ms", lane, lane, due, CEILING).isTrue();
        }
    }

    @Test
    void aPOSITIVELaneIsNEVERDueBeforeTheFLOOR() {
        // ⚠️ At the floor interval, 250 ÷ 4 = 62 ms; the floor is what stops a
        // +2 lane spending 4x the PUTs of a pod already at its fastest.
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(), clock);
        a.add(KEY, rec(), (byte) 2);
        clock.advance(FLOOR - 1);
        assertThat(a.isFlushDue()).as("max(floor, 250 ÷ 4): not due at 249 ms").isFalse();
        clock.advance(1);
        assertThat(a.isFlushDue()).as("due at the floor").isTrue();
    }

    @Test
    void aNEGATIVELaneAloneIsDueONLYAtTheCEILING() {
        for (int lane = -2; lane <= -1; lane++) {
            TestClock clock = new TestClock();
            // ⚠️ A FRESH accumulator, at the FLOOR interval: a negative lane that
            // followed the adaptive interval would be due at 250 ms here.
            Accumulator a = new Accumulator(config(), clock);
            a.add(KEY, rec(), (byte) lane);
            clock.advance(FLOOR);
            assertThat(a.isFlushDue())
                    .as("lane %d is not due at the adaptive interval", lane).isFalse();
            clock.advance(CEILING - FLOOR - 1);
            assertThat(a.isFlushDue())
                    .as("lane %d is not due one tick before the ceiling", lane).isFalse();
            clock.advance(1);
            assertThat(a.isFlushDue())
                    .as("lane %d IS due at the ceiling -- its anti-starvation bound", lane)
                    .isTrue();
        }
    }

    @Test
    void aSUSTAINEDPositiveStreamStillFLUSHESAndCarriesItsNegativeRecordsWithinTheCEILING()
            throws Exception {
        // ⚠️ WHAT THIS MEASURES, named for it (M10.7 review): the flush that
        // carries a negative record here is the POSITIVE lane's -- one
        // accumulator drains every lane at once -- so it pins that a lane's
        // clock starts at its OLDEST record, which is what keeps a sustained
        // stream flushing at all. The negative lane's own ceiling deadline is
        // `aNEGATIVELaneAloneIsDueONLYAtTheCEILING`'s. A +1 append every 10 ms
        // and a -1 append every second, for three ceilings: a clock restarted
        // at every append is never due under this stream, and the negative
        // records would wait for ever.
        TestClock clock = new TestClock();
        Accumulator a = lengthened(clock);
        long start = clock.millis();
        long oldestNegative = -1;
        long worst = 0;
        int drains = 0;
        for (long t = 0; t <= 3 * CEILING; t += 10) {
            clock.millis = start + t;
            if (a.isFlushDue()) {
                a.drain();
                drains++;
                if (oldestNegative >= 0) {
                    worst = Math.max(worst, clock.millis() - oldestNegative);
                    oldestNegative = -1;
                }
            }
            a.add(KEY, rec(), (byte) 1);
            if (t % 1_000 == 0) {
                a.add(KEY, rec(), (byte) -1);
                if (oldestNegative < 0) {
                    oldestNegative = clock.millis();
                }
            }
        }
        if (oldestNegative >= 0) {
            worst = Math.max(worst, clock.millis() - oldestNegative);
        }
        assertThat(drains).as("PREMISE: the stream flushed at all").isPositive();
        assertThat(worst)
                .as("no buffered negative record waits longer than the ceiling")
                .isLessThanOrEqualTo(CEILING);
    }

    @Test
    void aLanePLUS2TrickleAtTheCEILINGFlushesFOURTimesPerCeilingAndALane0TrickleONCE()
            throws Exception {
        // ⚠️ THE COST, COUNTED (ADR-0074 decision 4): a record every 10 ms for
        // one ceiling. ≤ 4 is NFR-1's amended bound for +2; exactly 4 is the
        // schedule (1250, 2500, 3750, 5000), so dropping 2^l (1 flush) and
        // flushing at the floor (20) both fail here.
        assertThat(flushesInOneCeiling((byte) 2)).as("a +2 trickle").isEqualTo(4);
        assertThat(flushesInOneCeiling((byte) 0)).as("a lane-0 trickle").isEqualTo(1);
    }

    private static int flushesInOneCeiling(byte lane) throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = lengthened(clock);
        long start = clock.millis();
        int flushes = 0;
        for (long t = 0; t <= CEILING; t += 10) {
            clock.millis = start + t;
            if (a.isFlushDue()) {
                a.drain();
                flushes++;
            }
            a.add(KEY, rec(), lane);
        }
        assertThat(a.currentInterval())
                .as("PREMISE: still at the ceiling interval").isEqualTo(Duration.ofMillis(CEILING));
        return flushes;
    }

    @Test
    void theFLUSHSpacingIsTheIntervalOver2ToTheHIGHESTBufferedPositiveLane() throws Exception {
        // ⚠️ ADR-0075 §3: the governor's expected rate follows the lanes, so a
        // +2 trickle's four flushes per ceiling never read as a regression.
        TestClock clock = new TestClock();
        Accumulator a = lengthened(clock);
        assertThat(a.flushSpacing()).as("empty: the adaptive interval")
                .isEqualTo(Duration.ofMillis(CEILING));
        a.add(KEY, rec(), (byte) -1);
        assertThat(a.flushSpacing()).as("a negative lane never shortens it")
                .isEqualTo(Duration.ofMillis(CEILING));
        a.add(KEY, rec(), (byte) 1);
        assertThat(a.flushSpacing()).isEqualTo(Duration.ofMillis(CEILING / 2));
        a.add(KEY, rec(), (byte) 2);
        assertThat(a.flushSpacing()).isEqualTo(Duration.ofMillis(CEILING / 4));
        a.add(KEY, rec(), (byte) 1);
        assertThat(a.flushSpacing()).as("the HIGHEST lane, not the latest")
                .isEqualTo(Duration.ofMillis(CEILING / 4));
        a.drain();
        assertThat(a.flushSpacing()).as("drained: back to the adaptive interval")
                .isEqualTo(Duration.ofMillis(CEILING));

        Accumulator fresh = new Accumulator(config(), new TestClock());
        fresh.add(KEY, rec(), (byte) 2);
        assertThat(fresh.flushSpacing()).as("never below the floor")
                .isEqualTo(Duration.ofMillis(FLOOR));
    }

    @Test
    void theWAKEIsTheEARLIESTLaneDeadlineOnTheInjectedClock() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = lengthened(clock);
        assertThat(a.millisUntilDue()).as("empty: never").isEqualTo(Long.MAX_VALUE);
        a.add(KEY, rec(), (byte) 0);
        assertThat(a.millisUntilDue()).isEqualTo(CEILING);
        clock.advance(100);
        a.add(KEY, rec(), (byte) 2);
        assertThat(a.millisUntilDue())
                .as("the +2 record's deadline, 1250 ms after IT arrived, is the earliest")
                .isEqualTo(CEILING / 4);
        a.add(KEY, rec(), (byte) -1);
        assertThat(a.millisUntilDue()).as("a negative lane is later still")
                .isEqualTo(CEILING / 4);
        clock.advance(CEILING);
        assertThat(a.millisUntilDue()).as("past due: zero, never negative").isZero();
    }
}
