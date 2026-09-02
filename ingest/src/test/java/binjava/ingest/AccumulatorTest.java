// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The flush trigger: 250 ms or 8 MiB, whichever comes first. */
class AccumulatorTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    /** ⚠️ ADVANCED, never slept on: a real clock cannot fit L0's 90-second budget. */
    private static final class TestClock extends Clock {
        private long millis;

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

    private static SegmentRecord record(String id, String body) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static IngestConfig config(Duration flush, long maxBytes) {
        // ⚠️ M3: pins intervalCeiling to intervalFloor -- this suite doesn't
        // (yet) test any adaptive behaviour, and some fixtures pass an hour
        // to disable the timer, which the default 5 s ceiling would reject.
        return IngestTestSupport.pinnedIntervalConfig(flush, maxBytes);
    }

    @Test
    void anEmptyAccumulatorIsNeverDue() {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        assertThat(a.isFlushDue()).isFalse();
        // ⚠️ A timer that fired on an idle stream would PUT once per interval
        // forever — the exact shape criterion 3 says must produce zero requests.
        clock.advance(Duration.ofHours(1));
        assertThat(a.isFlushDue()).as("still nothing to write").isFalse();
        assertThat(a.isEmpty()).isTrue();
    }

    @Test
    void aFlushIsDueAtExactlyTheInterval() {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        assertThat(a.add(new RunKey(A, 0), record("1", "{}"))).isFalse();

        clock.advance(Duration.ofMillis(249));
        assertThat(a.isFlushDue()).as("one millisecond short").isFalse();
        clock.advance(Duration.ofMillis(1));
        // ⚠️ AT the interval, not after it. Strictly-greater adds a clock tick to
        // every flush — a latency floor invisible in the code and in the config.
        assertThat(a.isFlushDue()).as("exactly at 250 ms").isTrue();
    }

    @Test
    void theTimerStartsAtTheFirstAppendNotTheLast() {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("1", "{}"));

        // a steady trickle: one record every 100 ms
        for (int i = 0; i < 3; i++) {
            clock.advance(Duration.ofMillis(100));
            a.add(new RunKey(A, 0), record("r" + i, "{}"));
        }
        // ⚠️ 300 ms have passed since the FIRST append. If the timer restarted
        // on each append it would be 100 ms and this segment would never flush,
        // leaving latency unbounded while the buffer slowly filled.
        assertThat(a.isFlushDue()).isTrue();
    }

    @Test
    void aFlushIsDueAtExactlyTheSizeLimit() {
        TestClock clock = new TestClock();
        // a small cap so the boundary is reachable in one record
        Accumulator a = new Accumulator(config(Duration.ofHours(1), 32), clock);
        assertThat(a.add(new RunKey(A, 0), record("1", "abc"))).as("well under").isFalse();
        assertThat(a.add(new RunKey(A, 0), record("2", "0123456789012345"))).isTrue();
        // ⚠️ Size, not time: the clock has not moved at all.
        assertThat(clock.millis()).isZero();
    }

    @Test
    void theSizeEstimateCountsFramingNotJustThePayload() {
        // ⚠️ The id, the version and the length prefixes are bytes in the object
        // too. Counting only payloads overshoots the segment size by whatever
        // framing costs, which on small documents is most of it.
        SegmentRecord tinyPayloadLongId =
                new SegmentRecord("x".repeat(500), OpType.INDEX, OptionalLong.of(1),
                        "{}".getBytes(StandardCharsets.UTF_8));
        assertThat(Accumulator.estimatedFramedBytes(tinyPayloadLongId))
                .as("a 500-byte id is 500 bytes of segment").isGreaterThan(500);
    }

    @Test
    void drainYieldsAReadableSegmentAndResetsTheAccumulator() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("a1", "{\"n\":1}"));
        a.add(new RunKey(B, 7), record("b1", "{\"n\":2}"));
        clock.advance(Duration.ofMillis(250));

        byte[] segment = a.drain().orElseThrow();
        SegmentReader r = SegmentReader.open(segment);
        // ⚠️ MANY STREAMS, ONE SEGMENT — the economic argument. Two indices went
        // into one object, so one PUT covers both.
        assertThat(r.directory()).hasSize(2);
        assertThat(r.read(r.find(new RunKey(A, 0)).orElseThrow())).hasSize(1);
        assertThat(r.read(r.find(new RunKey(B, 7)).orElseThrow())).hasSize(1);

        assertThat(a.isEmpty()).as("drained").isTrue();
        assertThat(a.bufferedBytes()).isZero();
        assertThat(a.isFlushDue()).as("and the timer restarted").isFalse();
    }

    @Test
    void drainingAnEmptyAccumulatorYieldsNothingRatherThanAnEmptySegment() throws Exception {
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), new TestClock());
        // ⚠️ An empty segment would still cost a PUT. Nothing buffered means
        // nothing written, which is what makes an idle cluster free.
        assertThat(a.drain()).isEmpty();
    }

    @Test
    void theSegmentIsStampedWithTheFirstAppendNotTheDrain() throws Exception {
        TestClock clock = new TestClock();
        clock.advance(Duration.ofMillis(1_000));
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        byte[] segment = a.drain().orElseThrow();
        // ⚠️ The segment's createdAt describes when its OLDEST record arrived,
        // which is what a reader needs to answer pointerFromTimestampMillis.
        assertThat(SegmentReader.open(segment).createdAtMillis()).isEqualTo(1_000L);
    }

    @Test
    void aSecondDrainAfterMoreRecordsIsIndependentOfTheFirst() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        a.drain().orElseThrow();

        clock.advance(Duration.ofMillis(10));
        a.add(new RunKey(A, 0), record("2", "{}"));
        assertThat(a.isFlushDue()).as("the new segment's timer starts fresh").isFalse();
        clock.advance(Duration.ofMillis(250));
        assertThat(a.isFlushDue()).isTrue();
        assertThat(SegmentReader.open(a.drain().orElseThrow()).directory()).hasSize(1);
    }

    @Test
    void lastFillRatioIsZeroBeforeAnyDrain() {
        // ⚠️ M3.2; ADR-0016 §2b: "nothing measured yet" must not look like a
        // real, low-fillRatio flush -- 0.0 is only ever produced here, never
        // by a genuine drain (a real segment always has SOME bytes).
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), new TestClock());
        assertThat(a.lastFillRatio()).isZero();
    }

    @Test
    void lastFillRatioSurvivesAnEmptyDrainAfterARealOne() throws Exception {
        // ⚠️ test-reviewer round 1 (M3.2): `drain()` returns empty BEFORE
        // touching `lastFillRatio` when nothing is buffered -- this proves
        // that path genuinely leaves a PRIOR real measurement in place
        // rather than resetting it, which the javadoc's own "0.0 before any
        // segment has ever been drained" wording implies but no test
        // previously exercised (every other test's first drain follows an
        // empty accumulator, never a drain call AFTER a real one).
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        byte[] segment = a.drain().orElseThrow();
        double realRatio = a.lastFillRatio();
        assertThat(realRatio).as("fixture must produce a genuine, non-zero measurement")
                .isNotZero();

        // ⚠️ Nothing buffered now -- drain() takes the early-return path.
        assertThat(a.drain()).as("nothing to write").isEmpty();
        assertThat(a.lastFillRatio()).as("the prior real measurement, not reset to 0")
                .isEqualTo(realRatio);
    }

    @Test
    void drainComputesFillRatioFromTheRealSegmentBytesNotTheEstimate() throws Exception {
        // ⚠️ M3.2; ADR-0016 §2/§2b: fillRatio = actualSegmentBytes /
        // targetSegmentSize, measured against the segment SegmentWriter
        // actually produced -- not `bufferedBytes()`'s pre-flush framing
        // estimate, which never counts the preamble/directory/footer that
        // are real bytes in the object too.
        TestClock clock = new TestClock();
        long targetSize = 1 << 20;
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), targetSize), clock);
        a.add(new RunKey(A, 0), record("1", "{\"n\":1}"));
        clock.advance(Duration.ofMillis(250));

        byte[] segment = a.drain().orElseThrow();
        assertThat(a.lastFillRatio()).isEqualTo((double) segment.length / targetSize);
    }

    @Test
    void fillRatioCanExceedOneAtTheSizeTriggerBoundary() throws Exception {
        // ⚠️ The size trigger fires on the ESTIMATE reaching the target; the
        // REAL segment (preamble + directory + footer + framing) is always
        // at least as large, so fillRatio at that exact boundary is >= 1.0,
        // never artificially capped at exactly 1.0.
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofHours(1), 32), clock);
        a.add(new RunKey(A, 0), record("1", "abc"));
        assertThat(a.add(new RunKey(A, 0), record("2", "0123456789012345"))).as("size trigger fires")
                .isTrue();

        byte[] segment = a.drain().orElseThrow();
        assertThat(a.lastFillRatio()).as("real bytes exceed the 32-byte target once framed")
                .isGreaterThan(1.0)
                .isEqualTo((double) segment.length / 32);
    }

    @Test
    void lastFillRatioUpdatesOnEachDrainIndependently() throws Exception {
        TestClock clock = new TestClock();
        Accumulator a = new Accumulator(config(Duration.ofMillis(250), 1 << 20), clock);
        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        byte[] first = a.drain().orElseThrow();
        double firstRatio = a.lastFillRatio();
        assertThat(firstRatio).isEqualTo((double) first.length / (1 << 20));

        // ⚠️ A second, LARGER segment must overwrite the first ratio, not
        // average with it or get stuck at the first drain's value.
        a.add(new RunKey(A, 0), record("2", "a longer body than the first one, by design"));
        a.add(new RunKey(B, 0), record("3", "another record so this segment is genuinely bigger"));
        clock.advance(Duration.ofMillis(250));
        byte[] second = a.drain().orElseThrow();
        assertThat(second.length).as("fixture must actually differ in size").isNotEqualTo(first.length);
        assertThat(a.lastFillRatio())
                .as("reflects the SECOND drain, not stuck at the first")
                .isEqualTo((double) second.length / (1 << 20))
                .isNotEqualTo(firstRatio);
    }

    // -- M3.3: the adaptive interval itself (ADR-0016 §2/§2b; ADR-0017) --

    private static IngestConfig adaptiveConfig(long maxBytes, Duration floor, Duration ceiling,
            double lowThreshold, double highThreshold, Duration lengthenDelay,
            Duration shortenDelay) {
        return new IngestConfig(floor, maxBytes, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, ceiling, lowThreshold, highThreshold,
                lengthenDelay, shortenDelay);
    }

    @Test
    void theIntervalStartsAtTheFloor() {
        // ⚠️ ADR-0017 point 2: every pod starts at the cheapest-latency,
        // safest point and earns the right to lengthen, rather than starting
        // anywhere else or requiring a coordinated initial value.
        IngestConfig cfg = adaptiveConfig(1 << 20, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, new TestClock());
        assertThat(a.currentInterval()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void theIntervalDoesNotLengthenBeforeTheSustainedDelayElapses() throws Exception {
        TestClock clock = new TestClock();
        IngestConfig cfg = adaptiveConfig(1 << 20, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, clock);

        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        a.drain();
        assertThat(a.lastFillRatio()).as("fixture must genuinely be low").isLessThan(0.4);
        assertThat(a.currentInterval()).as("streak just started, not yet 2 minutes")
                .isEqualTo(Duration.ofMillis(250));

        clock.advance(Duration.ofMinutes(2).minusMillis(1));
        a.add(new RunKey(A, 0), record("2", "{}"));
        a.drain();
        assertThat(a.currentInterval()).as("one millisecond short of the sustained delay")
                .isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void theIntervalLengthensToTheCeilingOnceFillRatioSustainsLowForTheFullDelay() throws Exception {
        TestClock clock = new TestClock();
        IngestConfig cfg = adaptiveConfig(1 << 20, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, clock);

        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        a.drain(); // the streak starts here

        clock.advance(Duration.ofMinutes(2)); // exactly the sustained delay, cumulative
        a.add(new RunKey(A, 0), record("2", "{}"));
        a.drain();
        assertThat(a.lastFillRatio()).as("still genuinely low").isLessThan(0.4);
        assertThat(a.currentInterval()).as("sustained for the full delay -- lengthens")
                .isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void theIntervalShortensToTheFloorImmediatelyOnceFillRatioReachesTheHighThreshold()
            throws Exception {
        // ⚠️ ADR-0016's own asymmetry: shortening reacts FAST (the M3 SPEC's
        // default shortenDelay is ZERO -- react on the very next flush,
        // protecting latency the moment volume rises, no debounce). A
        // target of 1000 bytes keeps both a tiny record (low fillRatio) and
        // an ~850-byte one (high fillRatio) trivially constructible against
        // the same config, on the same accumulator.
        TestClock clock = new TestClock();
        IngestConfig cfg = adaptiveConfig(1000, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, clock);

        // First, earn a lengthened interval, so shortening has something
        // real to undo rather than trivially staying at the floor.
        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        a.drain();
        assertThat(a.lastFillRatio()).as("setup fixture must genuinely be low").isLessThan(0.4);
        clock.advance(Duration.ofMinutes(2));
        a.add(new RunKey(A, 0), record("2", "{}"));
        a.drain();
        assertThat(a.currentInterval()).as("setup: lengthened").isEqualTo(Duration.ofSeconds(5));

        // Now a single high-fillRatio flush must shorten back to the floor
        // immediately -- no sustained delay required, unlike lengthening.
        a.add(new RunKey(A, 0), record("3", "x".repeat(850)));
        clock.advance(Duration.ofSeconds(5));
        a.drain();
        assertThat(a.lastFillRatio()).as("this flush is genuinely high").isGreaterThanOrEqualTo(0.9);
        assertThat(a.currentInterval()).as("shortened back to the floor immediately, no delay")
                .isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void aMiddleBandFillRatioResetsTheLengthenStreakWithoutChangingTheInterval() throws Exception {
        // ⚠️ Neither threshold condition has been CONTINUOUSLY true if a
        // middle-band observation lands in between -- the streak must
        // restart, not merely pause, or a fillRatio that dips low, drifts to
        // the middle for a while, then dips low again would lengthen based
        // on ACCUMULATED (not sustained) low time.
        TestClock clock = new TestClock();
        IngestConfig cfg = adaptiveConfig(1 << 20, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, clock);

        a.add(new RunKey(A, 0), record("1", "{}"));
        clock.advance(Duration.ofMillis(250));
        a.drain(); // low streak starts
        assertThat(a.lastFillRatio()).isLessThan(0.4);

        clock.advance(Duration.ofMinutes(1)); // half the sustained delay
        // ⚠️ A record sized to land in the middle band (neither <=0.4 nor >=0.9
        // of the 1 MiB target) -- resets the streak this drain observes.
        a.add(new RunKey(A, 0), record("2", "x".repeat(600_000)));
        a.drain();
        assertThat(a.lastFillRatio()).as("genuinely in the middle band")
                .isGreaterThan(0.4).isLessThan(0.9);
        assertThat(a.currentInterval()).isEqualTo(Duration.ofMillis(250));

        // Only 1 minute has passed since the RESET (the middle-band drain),
        // not the 2 minutes required -- must NOT have lengthened even though
        // 2+ minutes have passed since the very first (pre-reset) low drain.
        clock.advance(Duration.ofMinutes(1).plusMillis(1));
        a.add(new RunKey(A, 0), record("3", "{}"));
        a.drain();
        assertThat(a.lastFillRatio()).isLessThan(0.4);
        assertThat(a.currentInterval()).as("the streak restarted at the middle-band drain, not before")
                .isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void theIntervalNeverExceedsTheCeilingNorDropsBelowTheFloorUnderOscillation() throws Exception {
        // ⚠️ Adversarial: fillRatio alternating low/high every flush must
        // never push the interval outside [floor, ceiling], and (since
        // shortenDelay is zero but lengthenDelay is 2 minutes) an
        // oscillation faster than 2 minutes per cycle should never actually
        // lengthen at all -- only shorten-or-stay-at-floor.
        TestClock clock = new TestClock();
        IngestConfig cfg = adaptiveConfig(1 << 20, Duration.ofMillis(250), Duration.ofSeconds(5),
                0.4, 0.9, Duration.ofMinutes(2), Duration.ZERO);
        Accumulator a = new Accumulator(cfg, clock);

        for (int i = 0; i < 10; i++) {
            boolean high = i % 2 == 0;
            // ⚠️ Genuinely >= 0.9 of the 1 MiB target once real segment
            // overhead is added -- 900,000 bytes measures only ~0.86, which
            // would silently never reach the high branch at all and make
            // this loop's own "high" half a no-op. Verify, don't guess.
            SegmentRecord r = high
                    ? record("h" + i, "x".repeat(1_000_000))
                    : record("l" + i, "{}");
            a.add(new RunKey(A, 0), r);
            clock.advance(Duration.ofSeconds(1));
            a.drain();
            if (high) {
                assertThat(a.lastFillRatio()).as("iteration %d must genuinely be high", i)
                        .isGreaterThanOrEqualTo(0.9);
            } else {
                assertThat(a.lastFillRatio()).as("iteration %d must genuinely be low", i)
                        .isLessThanOrEqualTo(0.4);
            }
            assertThat(a.currentInterval())
                    .as("iteration %d, fillRatio=%f", i, a.lastFillRatio())
                    .isGreaterThanOrEqualTo(Duration.ofMillis(250))
                    .isLessThanOrEqualTo(Duration.ofSeconds(5));
        }
        // ⚠️ Every low observation's streak was reset by the very next
        // (high) observation before 2 minutes could ever elapse -- the
        // interval must have stayed at the floor throughout, never earning
        // a lengthen it was never continuously entitled to.
        assertThat(a.currentInterval()).isEqualTo(Duration.ofMillis(250));
    }
}
