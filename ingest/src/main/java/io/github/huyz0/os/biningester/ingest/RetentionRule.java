// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Whether a segment may be deleted yet (M7.5, FR-9, NFR-13, research 09 §7).
 *
 * <p>The rule, normatively, with its parentheses:
 *
 * <pre>
 * delete(segment) iff
 *       (     age(segment) &gt; minRetention                   # the time floor
 *         AND every known copy of every stream in the segment
 *             reported within reportTimeout                  # else the min() is stale
 *         AND min(watermark) &gt; endOffset(stream) + safetyMargin )
 *   OR  age(segment) &gt; maxRetention                          # ceiling: delete AND ALARM
 * </pre>
 *
 * <p>⚠️ THE PARENTHESES ARE LOAD-BEARING. Research 09 §7 writes this without
 * them, and read with OR binding tighter the ceiling can only fire jointly with
 * the floor and the freshness clause — never on its own, which is the one thing
 * it exists to do. Storage then grows without bound behind a paused shard.
 *
 * <p>⚠️ THE WATERMARK IS A BRAKE, NEVER AN ACCELERATOR. It can only push
 * deletion LATER than the time floor. A rule that deleted a fully-consumed
 * twenty-minute-old segment would be faster, cheaper and wrong: the outage
 * budget IS {@code minRetention} (NFR-13), and the position a consumer reports
 * is the IN-MEMORY pointer, so a restart resumes from the last Lucene commit —
 * behind what it told us (ADR-0005).
 *
 * <p>⚠️ {@code safetyMargin} IS IN OFFSETS, AND SIZING IT NEEDS A RATE. Research
 * 09 §6.1 says to size it above the observed Lucene commit interval, which is a
 * TIME; the rule adds it to an OFFSET. The conversion is the number of records
 * a stream can commit within that interval, so measurement M5 (M7.14) yields
 * the interval and the margin follows from it and the stream's rate. Until that
 * measurement exists the value is a configured guess, and every report this
 * milestone produces says so rather than shipping a plausible default quietly.
 *
 * <p>⚠️ A SEGMENT IS SHARED BY EVERY STREAM IT BUNDLES — that sharing is what
 * this product IS — so EVERY stream in it must be past its own end offset.
 * Deleting because one stream's consumer is past it deletes the other streams'
 * unread records, and at 1,600 streams per segment that is the normal case.
 *
 * <p>⚠️ IT HOLDS NO STORE AND NO SOCKET, and reads the clock through a seam. It
 * decides; {@code SegmentGc} (M7.6) acts.
 */
public final class RetentionRule {

    /**
     * The margin the watermark must clear before a segment is collected
     * (measurement M5, M7.14).
     *
     * <p>⚠️ IT IS IN OFFSETS WHILE RESEARCH 09 §6.1 SIZES IT IN TIME. The
     * corpus says "above the observed Lucene commit interval"; the rule adds it
     * to an offset. The conversion is the number of records a stream can commit
     * within that interval — so the probe measures both, and this constant is
     * chosen above the WORST records-per-advance it observed.
     *
     * <p>⚠️ AND MEASUREMENT M6 CHANGED WHAT IT HAS TO COVER.
     * <a href="../../../../../../docs/internal/product/decisions/0051-the-plugin-can-read-the-committed-pointer-and-the-error-moves-to-the-safe-side.md">ADR-0051</a>:
     * the plugin CAN read the committed pointer out of the shard's last Lucene
     * commit (measured: {@code 0} at shard creation, {@code 59} after sixty
     * records). Reporting THAT rather than the in-memory pointer does not make
     * the margin's job smaller by much — the probe saw no advance at all within
     * three ten-second observations, because a commit follows the translog
     * flush policy rather than ingestion — but it moves the error to the SAFE
     * side: a pointer BEHIND what the shard indexed makes GC keep too long,
     * where one AHEAD makes it delete what a restart re-reads.
     *
     * <p>⚠️ 10,000 IS AN UPPER BOUND ON A GUESS, NOT A MEASUREMENT.
     * {@code CommitIntervalProbeIT} runs one shard on one node with a
     * deliberately small workload, so what it observes is a floor rather than a
     * production figure; a stream committing at Scenario A's rate through a
     * multi-second translog flush would advance by far more. The honest reading
     * is that this default is safe for the workload that has been measured and
     * that a deployment at real scale must raise it — which is why the probe
     * reports the number rather than asserting a particular one.
     */
    public static final long DEFAULT_SAFETY_MARGIN = 10_000;

    /**
     * Where a segment's records end, per stream it bundles.
     *
     * <p>⚠️ THE END OFFSET IS INCLUSIVE — the last offset this segment carries
     * for that stream, {@code firstOffset + recordCount - 1} from the commit
     * delta — while a consumer's position is EXCLUSIVE (ADR-0049). The rule's
     * strict {@code >} is what reconciles the two, and getting it wrong is one
     * record of silent loss per segment.
     */
    public record SegmentFacts(String key, Instant writtenAt, Map<RunKey, Long> endOffsets) {

        public SegmentFacts {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(writtenAt, "writtenAt");
            // ⚠️ COPIED, AND THE COPY PRESERVES THE CALLER'S ORDER --
            // `Checkpoint`'s own reason, one module over. `Map.copyOf`
            // randomises iteration per JVM, so a case asserting that EVERY
            // stream is judged rather than only the first would catch a
            // first-only rule on a coin toss: review MEASURED 4 survivals in 6
            // runs of exactly that mutation. Order is not semantic here; being
            // able to PIN it is.
            endOffsets = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(
                    Objects.requireNonNull(endOffsets, "endOffsets")));
            if (endOffsets.isEmpty()) {
                // ⚠️ NOT TRIVIALLY DELETABLE. A segment that indexes no stream
                // cannot be judged by a rule about streams, and "every stream
                // is ready" is vacuously true of none -- which deletes an
                // object whose commit simply has not been read yet.
                throw new IllegalArgumentException(
                        "a segment that indexes no stream cannot be judged: " + key);
            }
        }
    }

    /** What is known about a stream's consumers — {@link WatermarkTable#of}. */
    @FunctionalInterface
    public interface Watermarks {
        WatermarkTable.Watermark of(RunKey stream);
    }

    /**
     * Told when a segment is deleted at the ceiling.
     *
     * <p>⚠️ IT IS NOT OPTIONAL AND HAS NO NO-OP DEFAULT. Crossing
     * {@code maxRetention} means a consumer WILL lose data, and deleting
     * quietly is worse than the storage bill it prevents.
     */
    @FunctionalInterface
    public interface Alarm {
        void ceilingCrossed(String segmentKey, Duration age);
    }

    /**
     * Delete or keep, and which clause decided.
     *
     * <p>⚠️ {@code why} IS NOT DECORATION. An operator asking why storage is not
     * falling needs the clause: inside the floor, unfresh, and behind the
     * watermark are three different incidents with three different answers.
     */
    public record Verdict(boolean delete, boolean ceiling, String why) {
    }

    private final Clock clock;
    private final Duration minRetention;
    private final Duration maxRetention;
    private final long safetyMargin;
    private final Alarm alarm;
    private final Watermarks watermarks;

    /** The ceiling past which a segment is deleted whatever any consumer read. */
    public Duration maxRetention() {
        return maxRetention;
    }

    public RetentionRule(Clock clock, Duration minRetention, Duration maxRetention,
            long safetyMargin, Alarm alarm, Watermarks watermarks) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.minRetention = requirePositive(minRetention, "minRetention");
        this.maxRetention = requirePositive(maxRetention, "maxRetention");
        this.alarm = Objects.requireNonNull(alarm, "alarm");
        this.watermarks = Objects.requireNonNull(watermarks, "watermarks");
        if (safetyMargin < 0) {
            // ⚠️ A NEGATIVE MARGIN IS AN ACCELERATOR: it deletes segments the
            // slowest copy has not reached, which is the one thing the margin
            // exists to prevent.
            throw new IllegalArgumentException("safetyMargin is never negative: " + safetyMargin);
        }
        this.safetyMargin = safetyMargin;
        if (maxRetention.compareTo(minRetention) <= 0) {
            throw new IllegalArgumentException("maxRetention " + maxRetention
                    + " is not longer than minRetention " + minRetention
                    + " -- a ceiling at or below the floor deletes everything the moment it "
                    + "passes the floor, alarming each time");
        }
    }

    private static Duration requirePositive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(what + " is never " + value);
        }
        return value;
    }

    /** Whether this segment may be deleted now, and why. */
    public Verdict verdictFor(SegmentFacts segment) {
        Objects.requireNonNull(segment, "segment");
        Duration age = Duration.between(segment.writtenAt(), clock.instant());
        if (age.compareTo(maxRetention) > 0) {
            // ⚠️ THE CEILING FIRES ALONE, and before anything else is
            // consulted: a paused shard reports a frozen pointer indefinitely
            // (§6.5), so every other clause says keep forever.
            alarm.ceilingCrossed(segment.key(), age);
            return new Verdict(true, true,
                    "past maxRetention " + maxRetention + " at age " + age
                            + " -- a consumer that had not read this HAS LOST DATA");
        }
        if (age.compareTo(minRetention) <= 0) {
            return new Verdict(false, false,
                    "inside the time floor: age " + age + " is not past minRetention "
                            + minRetention + ", which IS the outage budget");
        }
        for (Map.Entry<RunKey, Long> end : segment.endOffsets().entrySet()) {
            WatermarkTable.Watermark mark = watermarks.of(end.getKey());
            // ⚠️ `mark == null` IS A CONTRACT ON THE SEAM, not on
            // `WatermarkTable`, which never returns one. A `Watermarks` that
            // answered null -- a map lookup, the obvious implementation -- would
            // otherwise NPE inside the rule that decides deletion.
            if (mark == null || !mark.known()) {
                return new Verdict(false, false, "no copy has reported on stream "
                        + end.getKey() + " -- which is not the same fact as no copy having "
                        + "read anything");
            }
            if (!mark.fresh()) {
                return new Verdict(false, false, "the watermark for stream " + end.getKey()
                        + " is STALE: a copy has been silent past reportTimeout, so the "
                        + "min() is not trustworthy and GC pauses");
            }
            long threshold = end.getValue() + safetyMargin;
            if (threshold < end.getValue()) {
                // ⚠️ OVERFLOW WRAPS NEGATIVE, AND EVERY WATERMARK IS GREATER
                // THAN A NEGATIVE -- so the arithmetic alone would delete a
                // segment nobody has read.
                return new Verdict(false, false, "endOffset " + end.getValue()
                        + " plus safetyMargin " + safetyMargin + " overflows, so no "
                        + "watermark can be compared against it");
            }
            if (mark.consumedUpTo() <= threshold) {
                return new Verdict(false, false, "the watermark for stream " + end.getKey()
                        + " is " + mark.consumedUpTo() + ", not past endOffset "
                        + end.getValue() + " plus safetyMargin " + safetyMargin);
            }
        }
        return new Verdict(true, false, "past minRetention " + minRetention
                + " and every stream's slowest copy is past its end offset plus "
                + safetyMargin);
    }
}
