// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Buffers records across streams until a flush is due (M1.8).
 *
 * <p>⚠️ ONE ACCUMULATOR BUNDLES MANY STREAMS INTO ONE SEGMENT. That is the whole
 * economic argument: a PUT costs the same whether it carries one index's records
 * or a thousand, so request rate scales with FLUSHES, not with indices,
 * partitions or records (non-negotiable 6). An accumulator per partition would
 * restore exactly the cost Kafka has.
 *
 * <p>⚠️ THE TIMER STARTS AT THE FIRST APPEND OF A SEGMENT, not at the last. If
 * it restarted on every append, a steady trickle would never flush at all and
 * latency would be unbounded while the segment slowly filled.
 *
 * <p>⚠️ The clock is INJECTED. Business logic touches no clock directly
 * (non-negotiable 7), and criterion 3's 1,600 idle consumers advance an injected
 * clock across 3,000 poll intervals rather than sleeping — a test that slept
 * could not fit L0's 90-second budget.
 */
public final class Accumulator {

    private final IngestConfig config;
    private final Clock clock;

    private SegmentWriter writer = new SegmentWriter();
    private long bufferedBytes;
    private long firstAppendMillis = -1;
    private double lastFillRatio;

    /**
     * ⚠️ M10.7; ADR-0074 decision 3: the OLDEST buffered record's append time
     * PER BUFFERED LANE: {@code bufferedLanes[i]} was first appended at
     * {@code oldestByLane[i]}, for the first {@code laneCount} slots. The
     * oldest, never the latest, for the reason the class comment gives for
     * {@link #firstAppendMillis}: a lane whose clock restarted on every append
     * would never be due under a steady stream of that lane.
     * ⚠️ ONLY THE LANES ACTUALLY BUFFERED, because both {@link #add} (through
     * {@link #isFlushDue()}) and every flush-loop wake scan them under the
     * ingest lock: a slot per possible {@code i8} made that 256 per RECORD. The
     * active set is at most 8 (FR-18), so the scan is at most 8 and is over
     * lanes, never over records, streams or indices; the arrays grow only for
     * a caller that bypasses the active set.
     */
    private byte[] bufferedLanes = new byte[8];
    private long[] oldestByLane = new long[8];
    private int laneCount;

    /**
     * ⚠️ The highest lane buffered, {@link Byte#MIN_VALUE} when empty. VOLATILE
     * for the same reason as {@link #currentInterval}: {@link #flushSpacing()}
     * is read by the cost governor off the ingest lock (ADR-0075 §3).
     */
    private volatile byte highestLane = Byte.MIN_VALUE;

    // ⚠️ M3.3; ADR-0016 §2/§2b, carried forward by ADR-0017 -- see the M3
    // SPEC's own Design section for why "lengthen when sustained LOW,
    // shorten when HIGH" is the correct direction, not the reverse a naive
    // reading of ADR-0016's own (withdrawn, writer-count) "scale up"/"scale
    // down" table would suggest.
    // ⚠️ VOLATILE: the cost governor reads it off the ingest lock (M10.11).
    private volatile Duration currentInterval;
    private long lowStreakStartMillis = -1;
    private long highStreakStartMillis = -1;

    public Accumulator(IngestConfig config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        // ⚠️ Every pod starts at the floor -- the cheapest-latency, safest
        // point -- and earns the right to lengthen only once its OWN
        // fillRatio proves it sustains a low load (ADR-0017 point 2: no
        // coordination, no shared starting signal).
        this.currentInterval = config.intervalFloor();
    }

    /**
     * Buffers one record.
     *
     * @return true when a flush is now due
     */
    public boolean add(RunKey key, SegmentRecord record) {
        return add(key, record, (byte) 0);
    }

    /** Buffers one record of priority lane {@code lane} (ADR-0074). */
    public boolean add(RunKey key, SegmentRecord record, byte lane) {
        long now = clock.millis();
        if (firstAppendMillis < 0) {
            firstAppendMillis = now;
        }
        noteLane(lane, now);
        if (lane > highestLane) {
            highestLane = lane;
        }
        writer.add(key, record, now, lane);
        // ⚠️ An ESTIMATE of the framed size, not the payload length: the id, the
        // version and the length prefixes are bytes in the object too, and a
        // trigger that counted only payloads would overshoot the segment size by
        // however much framing costs.
        bufferedBytes += estimatedFramedBytes(record);
        return isFlushDue();
    }

    static long estimatedFramedBytes(SegmentRecord record) {
        return 1                                              // flags
                + 5 + record.id().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + (record.version().isPresent() ? 10 : 0)
                + 5 + record.payload().length;
    }

    /** Whether the buffered records should be written now. */
    public boolean isFlushDue() {
        if (isEmpty()) {
            // ⚠️ An empty accumulator is NEVER due. A timer that fired on an
            // idle stream would issue a PUT per interval forever — the exact
            // shape criterion 3 asserts must not happen.
            return false;
        }
        if (bufferedBytes >= config.maxSegmentBytes()) {
            return true;
        }
        // ⚠️ `<= 0` remaining, not `< 0`: at EXACTLY the deadline the flush is
        // due. Strictly-greater delays every flush by one clock tick, which at
        // 250 ms is a latency floor nobody would find by reading the code.
        // ⚠️ M3.3: every deadline is taken from this instance's OWN adapted
        // interval, not the config's floor straight (see `deadlineMillis`).
        return millisUntilDue() <= 0;
    }

    /**
     * ⚠️ M10.7; ADR-0074 decision 3: how long lane {@code lane}'s oldest record
     * may wait. A lane-0-only buffer is due at the adaptive interval exactly as
     * before M10 (ADR-0017), and every other lane is measured against the same
     * adapted value:
     * <ul>
     * <li>{@code +l}: {@code max(floor, interval ÷ 2^l)} -- ⚠️ never the floor
     *     straight, which ADR-0074 rejects at 20x NFR-1, and never below it;
     * <li>{@code 0}: the adaptive interval;
     * <li>negative: the CEILING, whatever the interval -- its anti-starvation
     *     bound, and NFR-7's divisor (ADR-0063). A negative lane that waited
     *     only for something else to flush would have no bound at all.
     * </ul>
     */
    private long deadlineMillis(int lane) {
        long interval = currentInterval.toMillis();
        if (lane > 0) {
            return Math.max(config.intervalFloor().toMillis(), interval >> lane);
        }
        return lane == 0 ? interval : config.intervalCeiling().toMillis();
    }

    /**
     * The flush spacing this buffer is scheduled at, for the cost governor
     * (ADR-0075 §3): {@code max(floor, adaptiveInterval ÷ 2^L)} where L is the
     * highest POSITIVE lane buffered, and the adaptive interval when none is.
     * ⚠️ A negative lane never shortens it: its deadline is the ceiling, so it
     * never makes a flush sooner than lane 0 would.
     */
    public Duration flushSpacing() {
        int l = Math.max(0, highestLane);
        long floor = config.intervalFloor().toMillis();
        return Duration.ofMillis(Math.max(floor, currentInterval.toMillis() >> l));
    }

    /**
     * Millis on the injected clock until the EARLIEST buffered lane's deadline,
     * {@code 0} once one has passed, {@link Long#MAX_VALUE} when empty -- what
     * the flush loop waits for, so a +2 record wakes it at its own deadline
     * rather than at the next poll. ⚠️ The size trigger is not in it: that one
     * is checked by the append that crosses it.
     */
    public long millisUntilDue() {
        if (isEmpty()) {
            return Long.MAX_VALUE;
        }
        long now = clock.millis();
        long earliest = Long.MAX_VALUE;
        for (int i = 0; i < laneCount; i++) {
            earliest = Math.min(earliest,
                    oldestByLane[i] + deadlineMillis(bufferedLanes[i]) - now);
        }
        return Math.max(0, earliest);
    }

    /** Records {@code lane}'s first append at {@code now}; a later one keeps the oldest. */
    private void noteLane(byte lane, long now) {
        for (int i = 0; i < laneCount; i++) {
            if (bufferedLanes[i] == lane) {
                return;
            }
        }
        if (laneCount == bufferedLanes.length) {
            bufferedLanes = java.util.Arrays.copyOf(bufferedLanes, laneCount * 2);
            oldestByLane = java.util.Arrays.copyOf(oldestByLane, laneCount * 2);
        }
        bufferedLanes[laneCount] = lane;
        oldestByLane[laneCount] = now;
        laneCount++;
    }

    /** This instance's own current flush interval -- the floor until fillRatio earns otherwise. */
    public Duration currentInterval() {
        return currentInterval;
    }

    /** Creates the next empty buffer without resetting the adaptive interval state. */
    Accumulator emptyCopy() {
        Accumulator next = new Accumulator(config, clock);
        next.adoptAdaptiveStateFrom(this);
        return next;
    }

    /** Carries the detached buffer's measured fill state back to the active buffer. */
    void adoptAdaptiveStateFrom(Accumulator other) {
        currentInterval = other.currentInterval;
        lowStreakStartMillis = other.lowStreakStartMillis;
        highStreakStartMillis = other.highStreakStartMillis;
        lastFillRatio = other.lastFillRatio;
    }

    public boolean isEmpty() {
        return writer.isEmpty();
    }

    public long bufferedBytes() {
        return bufferedBytes;
    }

    /**
     * Takes the buffered segment and starts a new one.
     *
     * @return the segment bytes, or empty if nothing was buffered
     */
    public java.util.Optional<byte[]> drain() throws IOException {
        if (isEmpty()) {
            return java.util.Optional.empty();
        }
        byte[] segment = writer.toByteArray(firstAppendMillis);
        // ⚠️ M3.2; ADR-0016 §2/§2b, carried forward by ADR-0017: measured
        // against the segment's REAL serialised length, not `bufferedBytes`'s
        // pre-flush estimate -- framing (preamble, directory, footer) is real
        // bytes in the object that the estimate never counted, so fillRatio
        // can genuinely exceed 1.0 when the size trigger fires right at the
        // boundary.
        lastFillRatio = (double) segment.length / config.maxSegmentBytes();
        adaptInterval(clock.millis());
        writer = new SegmentWriter();
        bufferedBytes = 0;
        firstAppendMillis = -1;
        laneCount = 0;
        highestLane = Byte.MIN_VALUE;
        return java.util.Optional.of(segment);
    }

    /**
     * M3.3; ADR-0016 §2/§2b, carried forward by ADR-0017. Per pod, no
     * coordination: {@code lastFillRatio} sustained at or below {@code
     * fillRatioLowThreshold} for {@code intervalLengthenDelay} lengthens the
     * interval to the ceiling; sustained at or above {@code
     * fillRatioHighThreshold} for {@code intervalShortenDelay} shortens it
     * to the floor. A binary jump, not a multi-step ramp -- ADR-0016/17
     * describe a RANGE and a direction, never an intermediate stepping
     * function, and the sustained-delay requirement is what already makes
     * lengthening the slow half of the asymmetry; a ramp on top of that
     * would be an invented detail the corpus never specifies. A `fillRatio`
     * strictly between the two thresholds resets BOTH streaks -- neither
     * condition has been continuously true, so neither should fire once the
     * OTHER band is reached again later.
     */
    private void adaptInterval(long nowMillis) {
        if (lastFillRatio >= config.fillRatioHighThreshold()) {
            lowStreakStartMillis = -1;
            if (highStreakStartMillis < 0) {
                highStreakStartMillis = nowMillis;
            }
            if (nowMillis - highStreakStartMillis >= config.intervalShortenDelay().toMillis()) {
                currentInterval = config.intervalFloor();
            }
        } else if (lastFillRatio <= config.fillRatioLowThreshold()) {
            highStreakStartMillis = -1;
            if (lowStreakStartMillis < 0) {
                lowStreakStartMillis = nowMillis;
            }
            if (nowMillis - lowStreakStartMillis >= config.intervalLengthenDelay().toMillis()) {
                currentInterval = config.intervalCeiling();
            }
        } else {
            lowStreakStartMillis = -1;
            highStreakStartMillis = -1;
        }
    }

    /**
     * {@code fillRatio = actualSegmentBytes ÷ targetSegmentSize} from the
     * most recent {@link #drain()} -- {@code 0.0} before any segment has ever
     * been drained. The control signal ADR-0016 §2b names, measured locally
     * with no coordination (ADR-0017 point 2): this instance's own flushes,
     * nothing gossiped, nothing summed across pods.
     */
    public double lastFillRatio() {
        return lastFillRatio;
    }
}
