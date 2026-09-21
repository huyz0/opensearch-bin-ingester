// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.time.Duration;
import java.util.Objects;

/**
 * How long data is kept and how often a node looks for some to collect (M8.5,
 * FR-9, NFR-13).
 *
 * <p>⚠️ **THE FLOOR IS 6 HOURS BECAUSE NFR-13 SAYS SO**, and it is the consumer
 * outage budget: how long OpenSearch can be down before data is lost (research
 * 09 §7). ⚠️ M8.4 LEFT A 1-HOUR FLOOR IN {@code Assembly} as a placeholder
 * constant, which would have been a sixth of the agreed budget the moment
 * anything read it. It is replaced here rather than carried.
 *
 * <p>⚠️ **THE CEILING IS A CONFIGURED GUESS AND SAYS SO**, the same standing as
 * the lease TTL's default: research 09 §7 makes {@code maxRetention} a COST
 * bound and an incident rather than a routine event, and names no number.
 * Seven days is long enough that crossing it means a consumer has been gone
 * for a week, which is the situation the ceiling alarm exists to shout about.
 *
 * @param minRetention the floor: nothing younger is deleted, read or not
 * @param maxRetention the ceiling: anything older is deleted, read or not, and
 *     an alarm fires
 * @param reportTimeout how long a shard copy may be silent before its stream's
 *     watermark stops being trusted
 * @param copyExpiry how long a silent copy is remembered before it is retired
 * @param passInterval how often the retention loop ticks
 */
public record RetentionConfig(Duration minRetention, Duration maxRetention,
        Duration reportTimeout, Duration copyExpiry, Duration passInterval) {

    /** NFR-13: the consumer outage budget. */
    public static final Duration DEFAULT_MIN_RETENTION = Duration.ofHours(6);

    /** A configured guess -- see the class javadoc. */
    public static final Duration DEFAULT_MAX_RETENTION = Duration.ofDays(7);

    /** Research 09 §9: a copy silent this long makes its stream's minimum stale. */
    public static final Duration DEFAULT_REPORT_TIMEOUT = Duration.ofMinutes(1);

    /**
     * ⚠️ **TWICE THE FLOOR, BECAUSE IT MUST OUTLIVE THE FLOOR.** A copy retired
     * inside the retention window is a copy whose data is deleted while it is
     * still entitled to read it -- {@code WatermarkTable} refuses that ordering
     * at construction, and this default is chosen so that it never has to.
     */
    public static final Duration DEFAULT_COPY_EXPIRY = DEFAULT_MIN_RETENTION.multipliedBy(2);

    public static RetentionConfig defaults() {
        return new RetentionConfig(DEFAULT_MIN_RETENTION, DEFAULT_MAX_RETENTION,
                DEFAULT_REPORT_TIMEOUT, DEFAULT_COPY_EXPIRY,
                io.github.huyz0.os.biningester.ingest.RetentionLoop.DEFAULT_PASS_INTERVAL);
    }

    public RetentionConfig {
        requirePositive(minRetention, "minRetention");
        requirePositive(maxRetention, "maxRetention");
        requirePositive(reportTimeout, "reportTimeout");
        requirePositive(copyExpiry, "copyExpiry");
        requirePositive(passInterval, "passInterval");
        // ⚠️ THE CROSS-FIELD RULES ARE THE RECORDS' THAT OWN THEM --
        // `RetentionRule`, `RetentionObservable` and `WatermarkTable` each
        // refuse their own inversion. They are checked HERE as well only so
        // that `ServerProperties` reports them before the graph is half-built,
        // with the operator's key names; the messages below name the setting,
        // which the owners' messages cannot.
        if (maxRetention.compareTo(minRetention) <= 0) {
            throw new IllegalArgumentException("retention.max " + maxRetention
                    + " is not longer than retention.min " + minRetention
                    + " -- a ceiling at or below the floor deletes everything the moment it "
                    + "passes the floor");
        }
        if (copyExpiry.compareTo(minRetention) <= 0) {
            throw new IllegalArgumentException("retention.copy-expiry " + copyExpiry
                    + " is not longer than retention.min " + minRetention
                    + " -- a copy retired inside the retention window loses data it is "
                    + "still entitled to read");
        }
        if (copyExpiry.compareTo(reportTimeout) <= 0) {
            throw new IllegalArgumentException("retention.copy-expiry " + copyExpiry
                    + " is not longer than retention.report-timeout " + reportTimeout);
        }
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
