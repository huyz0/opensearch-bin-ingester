// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.time.Duration;
import java.util.Objects;

/**
 * What the ingester needs to run. Validated at construction, never later.
 *
 * <p>⚠️ THE FLUSH INTERVAL IS THE PRIMARY COST DIAL. Cost rule R1b prices it:
 * 250 ms costs about $311/month where 5 s costs $15.55, because PUTs scale with
 * flushes and nothing else. M1 fixed it at 250 ms KNOWINGLY, and this record
 * was where that number was stated rather than buried in a constructor call.
 *
 * <p>⚠️ M3 (ADR-0016 §2, carried forward by ADR-0017): the interval is no
 * longer one fixed value -- {@code intervalFloor} and {@code intervalCeiling}
 * bound a RANGE {@code Accumulator} adapts within, per pod, per its own
 * {@code fillRatio} (this record's own {@code fillRatioLowThreshold}/{@code
 * fillRatioHighThreshold} and the two hysteresis delays). {@code
 * intervalFloor} is the SAME 250ms M1 shipped -- renamed, not moved, since
 * its role is now "the shortest the interval may become" rather than "the
 * only value it ever has." This record itself computes nothing; it is
 * validated shape only, per this class's own javadoc ("validated at
 * construction, never later"). The adaptive LOGIC is {@code Accumulator}'s
 * job (M3.2/M3.3).
 *
 * @param intervalFloor the shortest the flush interval may ever become; the
 *     latency floor, and (before M3) the fixed cost dial M1 shipped
 * @param maxSegmentBytes flush early once a segment reaches this size, so a busy
 *     stream does not wait on the timer
 * @param trustDomain which domain this ingester writes for; segments are NEVER
 *     bundled across domains (ADR-0021), so this is not a label but a boundary
 * @param maxQueuedPushBytes how many BYTES of undelivered segment pushes may be
 *     held for slow subscribers. ⚠️ java-style rule 7: the bound is
 *     CONFIGURATION. ⚠️ And it is expressed in BYTES, not in a count of pushes:
 *     an earlier version bounded the COUNT and derived the byte figure from
 *     {@code maxSegmentBytes}, which is wrong because that is a flush TRIGGER,
 *     not a segment cap -- {@code Accumulator} checks it only after a caller's
 *     whole record list is buffered, so one 32 MiB `_bulk` body makes one
 *     ~32 MiB segment. Eight of those would have retained ~256 MiB against the
 *     256 MB heap criterion 8 budgets: the very OutOfMemoryError the bound
 *     exists to prevent
 * @param intervalCeiling the longest the flush interval may ever grow to
 *     (M3; ADR-0016 §2: "250 ms -> 30 s, capped by a per-lane latency
 *     ceiling" -- no lanes exist yet, so this is a single global value, not
 *     derived from any per-lane deadline; see the M3 SPEC's own Scope)
 * @param fillRatioLowThreshold {@code fillRatio} at or below this, sustained
 *     for {@code intervalLengthenDelay}, lengthens the interval toward the
 *     ceiling (ADR-0016 §2b's hysteresis band, carried forward by ADR-0017)
 * @param fillRatioHighThreshold {@code fillRatio} at or above this shortens
 *     the interval toward the floor, reacting within {@code
 *     intervalShortenDelay}
 * @param intervalLengthenDelay how long {@code fillRatio} must stay at or
 *     below {@code fillRatioLowThreshold} before the interval lengthens --
 *     the SLOW half of ADR-0016's "scale up fast, scale down slow" asymmetry,
 *     translated to a single pod's own interval (see the M3 SPEC's Design
 *     section for why this is the slow direction, not the fast one)
 * @param intervalShortenDelay how long {@code fillRatio} must stay at or
 *     above {@code fillRatioHighThreshold} before the interval shortens --
 *     the FAST half of the same asymmetry; the M3 SPEC's own chosen default
 *     is zero (react on the very next flush, protecting latency immediately
 *     once volume rises, with no debounce)
 */
public record IngestConfig(Duration intervalFloor, long maxSegmentBytes, String trustDomain,
        long maxQueuedPushBytes, Duration intervalCeiling, double fillRatioLowThreshold,
        double fillRatioHighThreshold, Duration intervalLengthenDelay,
        Duration intervalShortenDelay) {

    /** M1's fixed operating point, now the floor of the adaptive range (M3). */
    public static final Duration DEFAULT_INTERVAL_FLOOR = Duration.ofMillis(250);

    /** 8 MiB, the size the memory budget counts 50 of in the buffer pool. */
    public static final long DEFAULT_MAX_SEGMENT_BYTES = 8L * 1024 * 1024;

    /**
     * 64 MiB of undelivered pushes — a quarter of the 256 MB heap criterion 8
     * budgets, and a bound on the quantity that actually consumes it.
     */
    public static final long DEFAULT_MAX_QUEUED_PUSH_BYTES = 64L * 1024 * 1024;

    /** ADR-0017's own headline number: $15.55/month at this ceiling, at 1 MiB/s per pod. */
    public static final Duration DEFAULT_INTERVAL_CEILING = Duration.ofSeconds(5);

    /** ADR-0016 §2b's hysteresis band, carried forward by ADR-0017. */
    public static final double DEFAULT_FILL_RATIO_LOW_THRESHOLD = 0.4;

    public static final double DEFAULT_FILL_RATIO_HIGH_THRESHOLD = 0.9;

    /** The M3 SPEC's own chosen default: 2 minutes of sustained low fillRatio before growing. */
    public static final Duration DEFAULT_INTERVAL_LENGTHEN_DELAY = Duration.ofMinutes(2);

    /** The M3 SPEC's own chosen default: react on the very next flush, no debounce. */
    public static final Duration DEFAULT_INTERVAL_SHORTEN_DELAY = Duration.ZERO;

    public IngestConfig {
        Objects.requireNonNull(intervalFloor, "intervalFloor");
        Objects.requireNonNull(trustDomain, "trustDomain");
        Objects.requireNonNull(intervalCeiling, "intervalCeiling");
        Objects.requireNonNull(intervalLengthenDelay, "intervalLengthenDelay");
        Objects.requireNonNull(intervalShortenDelay, "intervalShortenDelay");
        if (trustDomain.isBlank()) {
            throw new IllegalArgumentException("a trust domain is never blank");
        }
        if (intervalFloor.isNegative() || intervalFloor.isZero()) {
            // ⚠️ Zero means "flush every append", which is one PUT per record --
            // the shape non-negotiable 6 forbids outright, and the bill would
            // scale with records rather than with segments.
            throw new IllegalArgumentException("intervalFloor must be positive: " + intervalFloor);
        }
        if (maxSegmentBytes <= 0) {
            throw new IllegalArgumentException("maxSegmentBytes must be positive: " + maxSegmentBytes);
        }
        if (maxQueuedPushBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxQueuedPushBytes must be positive: " + maxQueuedPushBytes);
        }
        if (intervalCeiling.compareTo(intervalFloor) < 0) {
            throw new IllegalArgumentException(
                    "intervalCeiling must be at least intervalFloor: " + intervalCeiling
                            + " < " + intervalFloor);
        }
        if (!(fillRatioLowThreshold > 0.0) || !(fillRatioLowThreshold < 1.0)) {
            throw new IllegalArgumentException(
                    "fillRatioLowThreshold must be in (0, 1): " + fillRatioLowThreshold);
        }
        if (!(fillRatioHighThreshold > 0.0) || !(fillRatioHighThreshold <= 1.0)) {
            throw new IllegalArgumentException(
                    "fillRatioHighThreshold must be in (0, 1]: " + fillRatioHighThreshold);
        }
        if (fillRatioLowThreshold >= fillRatioHighThreshold) {
            throw new IllegalArgumentException(
                    "fillRatioLowThreshold must be below fillRatioHighThreshold: "
                            + fillRatioLowThreshold + " >= " + fillRatioHighThreshold);
        }
        if (intervalLengthenDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "intervalLengthenDelay must not be negative: " + intervalLengthenDelay);
        }
        if (intervalShortenDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "intervalShortenDelay must not be negative: " + intervalShortenDelay);
        }
    }

    /**
     * The four-argument form, defaulting the adaptive-interval fields to M3's
     * own chosen operating point.
     *
     * <p>⚠️ A convenience, not a second set of semantics: it exists so this
     * task's own field additions did not silently rewrite every existing call
     * site's meaning (the same reasoning M1's own three-argument form gave for
     * defaulting {@code maxQueuedPushBytes}).
     */
    public IngestConfig(Duration intervalFloor, long maxSegmentBytes, String trustDomain,
            long maxQueuedPushBytes) {
        this(intervalFloor, maxSegmentBytes, trustDomain, maxQueuedPushBytes,
                DEFAULT_INTERVAL_CEILING, DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                DEFAULT_FILL_RATIO_HIGH_THRESHOLD, DEFAULT_INTERVAL_LENGTHEN_DELAY,
                DEFAULT_INTERVAL_SHORTEN_DELAY);
    }

    /** The three-argument form, defaulting the push bound too. */
    public IngestConfig(Duration intervalFloor, long maxSegmentBytes, String trustDomain) {
        this(intervalFloor, maxSegmentBytes, trustDomain, DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }

    /** M1's operating point for one trust domain, with M3's default adaptive range. */
    public static IngestConfig defaults(String trustDomain) {
        return new IngestConfig(DEFAULT_INTERVAL_FLOOR, DEFAULT_MAX_SEGMENT_BYTES, trustDomain,
                DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }
}
