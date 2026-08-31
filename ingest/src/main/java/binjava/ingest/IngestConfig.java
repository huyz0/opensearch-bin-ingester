// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.time.Duration;
import java.util.Objects;

/**
 * What the ingester needs to run. Validated at construction, never later.
 *
 * <p>⚠️ THE FLUSH INTERVAL IS THE PRIMARY COST DIAL. Cost rule R1b prices it:
 * 250 ms costs about $311/month where 5 s costs $15.55, because PUTs scale with
 * flushes and nothing else. M1 fixes it at 250 ms KNOWINGLY — the adaptive loop
 * is M3 — and this record is where that number is stated rather than buried in
 * a constructor call.
 *
 * @param flushInterval how long a partly-filled segment waits before it is
 *     written; the latency floor, and the cost dial
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
 */
public record IngestConfig(Duration flushInterval, long maxSegmentBytes, String trustDomain,
        long maxQueuedPushBytes) {

    /** M1's fixed operating point (ADR-0017 defers the adaptive loop to M3). */
    public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(250);

    /** 8 MiB, the size the memory budget counts 50 of in the buffer pool. */
    public static final long DEFAULT_MAX_SEGMENT_BYTES = 8L * 1024 * 1024;

    /**
     * 64 MiB of undelivered pushes — a quarter of the 256 MB heap criterion 8
     * budgets, and a bound on the quantity that actually consumes it.
     */
    public static final long DEFAULT_MAX_QUEUED_PUSH_BYTES = 64L * 1024 * 1024;

    public IngestConfig {
        Objects.requireNonNull(flushInterval, "flushInterval");
        Objects.requireNonNull(trustDomain, "trustDomain");
        if (trustDomain.isBlank()) {
            throw new IllegalArgumentException("a trust domain is never blank");
        }
        if (flushInterval.isNegative() || flushInterval.isZero()) {
            // ⚠️ Zero means "flush every append", which is one PUT per record --
            // the shape non-negotiable 6 forbids outright, and the bill would
            // scale with records rather than with segments.
            throw new IllegalArgumentException("flushInterval must be positive: " + flushInterval);
        }
        if (maxSegmentBytes <= 0) {
            throw new IllegalArgumentException("maxSegmentBytes must be positive: " + maxSegmentBytes);
        }
        if (maxQueuedPushBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxQueuedPushBytes must be positive: " + maxQueuedPushBytes);
        }
    }

    /**
     * The three-argument form, defaulting the push bound.
     *
     * <p>⚠️ A convenience, not a second operating point: it exists so adding the
     * bound did not silently rewrite every existing call site's meaning.
     */
    public IngestConfig(Duration flushInterval, long maxSegmentBytes, String trustDomain) {
        this(flushInterval, maxSegmentBytes, trustDomain, DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }

    /** M1's operating point for one trust domain. */
    public static IngestConfig defaults(String trustDomain) {
        return new IngestConfig(DEFAULT_FLUSH_INTERVAL, DEFAULT_MAX_SEGMENT_BYTES, trustDomain,
                DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }
}
