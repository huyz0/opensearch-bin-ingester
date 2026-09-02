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
}
