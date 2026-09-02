// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M3.4; M3 SPEC acceptance criterion 4. Proves the adaptive interval (M3.3)
 * against a MODELED traffic rate, not hand-fed flushes -- the SPEC's own T1
 * row exists because AccumulatorTest's tests each drive one flush at a time
 * and could not by themselves prove an interval that lengthens and then holds
 * under a sustained stream.
 *
 * <p>⚠️ 1 MiB/s per pod is ADR-0017's own worked example (its $15.55/month
 * headline number). At that rate the SIZE trigger never binds once the
 * interval is at its 5 s ceiling: 5 s worth of traffic is ~5 MiB, under the
 * 8 MiB target, so the timer is what fires, every time -- the same reasoning
 * the SPEC gives for why 8 MiB at 1 MiB/s (8 s) would be slower than the
 * ceiling if the size trigger ever got there first.
 *
 * <p>⚠️ ONLY {@link SegmentPublisher}, no {@link binjava.sequencer.CommitLog}: "one flush is one
 * PUT" (SegmentPublisher's own javadoc) is exactly the quantity ADR-0016's
 * PUT/s table counts and this test measures -- a commit-log append is a
 * SEPARATE request the cost table does not fold into that figure, and
 * including it here would measure something the SPEC never claimed.
 */
class IntervalPutRateTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    /** ⚠️ ADVANCED, never slept on: a real clock cannot fit L0's 90-second budget. */
    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override
        public long millis() {
            return millis;
        }

        void advance(Duration d) {
            millis += d.toMillis();
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

    // ⚠️ 8 ticks/second so 1 MiB/8 = 131072 divides EXACTLY -- no rounding
    // drift accumulating across the thousands of ticks a 2-minute lengthen
    // delay needs.
    private static final Duration TICK = Duration.ofMillis(125);
    private static final int BYTES_PER_TICK = (1 << 20) / 8;

    // estimatedFramedBytes = 1 (flags) + 5 + id.length + 5 + payload.length,
    // for a version-less record with a 1-byte id -- see Accumulator's own
    // static method. Sized so each tick's append contributes exactly
    // BYTES_PER_TICK, matching the modeled 1 MiB/s by the SAME accounting the
    // production flush trigger itself uses.
    private static final byte[] PAYLOAD = new byte[BYTES_PER_TICK - 11];

    @Test
    @Timeout(30)
    void theSustainedRunSettlesToTheCeilingsPutRate() throws Exception {
        TestClock clock = new TestClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");
        Accumulator accumulator = new Accumulator(config, clock);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        RunKey stream = new RunKey(INDEX, 0);

        long startMillis = clock.millis();
        long settledAtMillis = -1;
        long windowStartMillis = -1;
        long putsAtWindowStart = 0;
        Duration measureWindow = Duration.ofSeconds(60);
        // ⚠️ A bound on SIMULATED time, not wall-clock: if the interval never
        // lengthens at all, the loop must fail with a clear message rather
        // than spin until @Timeout kills the JVM thread. Kept close to the
        // correct path's own ~3 min (2 min lengthenDelay + the 60 s measure
        // window), not generous -- MemoryBinStore never frees a PUT, and at
        // the FLOOR's 250 ms cadence a "never lengthens" bug would otherwise
        // keep flushing (and retaining ~256 KiB segments) for the whole
        // bound, risking an OutOfMemoryError under :ingest:test's 512m heap
        // well before this assertion ever gets to fire -- found by running
        // this exact mutation, not assumed.
        Duration giveUpAfter = Duration.ofMinutes(4);

        while (true) {
            accumulator.add(stream,
                    new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(), PAYLOAD));
            clock.advance(TICK);
            if (accumulator.isFlushDue()) {
                publisher.publish(accumulator);
            }

            if (settledAtMillis < 0 && accumulator.currentInterval().equals(config.intervalCeiling())) {
                settledAtMillis = clock.millis();
                windowStartMillis = clock.millis();
                putsAtWindowStart = store.counts().puts();
            }
            if (windowStartMillis >= 0
                    && clock.millis() - windowStartMillis >= measureWindow.toMillis()) {
                break;
            }
            if (clock.millis() - startMillis > giveUpAfter.toMillis()) {
                throw new AssertionError(
                        "interval never settled at the ceiling within " + giveUpAfter);
            }
        }

        // ⚠️ Settles close to the SPEC's own 2-minute lengthenDelay, not
        // merely "eventually" -- catches a wrongly-scaled delay constant that
        // a bare "did it ever settle" check would miss.
        assertThat(Duration.ofMillis(settledAtMillis - startMillis))
                .as("settles once fillRatio has sustained low for the configured lengthen delay")
                .isCloseTo(config.intervalLengthenDelay(), Duration.ofSeconds(1));
        assertThat(accumulator.currentInterval())
                .as("stays at the ceiling once settled -- a ~0.625 fillRatio at 1 MiB/s over 5s "
                        + "is a MIDDLE-band reading, which must hold the interval, not shorten it")
                .isEqualTo(config.intervalCeiling());

        long putsInWindow = store.counts().puts() - putsAtWindowStart;
        double elapsedSeconds = measureWindow.toMillis() / 1000.0;
        double measuredRate = putsInWindow / elapsedSeconds;
        double target = 1.0 / config.intervalCeiling().toSeconds();
        assertThat(measuredRate)
                .as("acceptance criterion 4: within +/-20%% of 1/intervalCeiling = %s PUT/s, not "
                        + "the fixed-250ms rate's 4 PUT/s", target)
                .isCloseTo(target, withinPercentage(20));
    }
}
