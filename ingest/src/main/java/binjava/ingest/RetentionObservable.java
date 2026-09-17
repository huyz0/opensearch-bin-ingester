// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What an operator sees while retention is working (M7.13, FR-9, research 09
 * §9).
 *
 * <p>⚠️ THE ONE TILE WORTH A DASHBOARD IS {@link #outageBudget()}:
 * {@code minRetention − age(oldest unread data)}, which says how long this
 * cluster can stay broken before data is lost. It COUNTS DOWN, and it is the
 * WORST stream's rather than an average — one stream an hour from losing
 * records is the cluster's budget, and an average would read comfortable while
 * that stream lost them.
 *
 * <p>⚠️ IT GOES NEGATIVE RATHER THAN CLAMPING. A budget clamped at zero says
 * "out of time" for an hour over and for a day over alike, and the difference
 * is how much data is about to go.
 *
 * <p>⚠️ NO PER-STREAM, PER-INDEX OR PER-COPY METRIC LABEL. observability.md
 * rule 1 closes the allow-list because a {@code stream} label costs ~2,400,000
 * series and ~5.7 GiB of TSDB memory — telemetry heavier than the traffic it
 * describes, in a project that exists to make that traffic cheap. Identities
 * ride the EVENT (rule 2) and a bounded top-K, which is the shape M4.14
 * established.
 *
 * <p>⚠️ THE THREE ALARMS ARE RESEARCH 09 §9's, AND EACH FIRES ON ITS OWN. A
 * copy silent past {@code reportTimeout} freezes a watermark, so storage grows
 * and nobody knows why until the ceiling fires hours later; data APPROACHING
 * the ceiling is announced BEFORE it is deleted, because an alarm at the
 * deletion tells an operator about data that is already gone; and a paused
 * shard older than the floor is the one condition that GUARANTEES the ceiling
 * will fire (research 09 §6.5).
 *
 * <p>⚠️ AND THE SAME CONDITION DOES NOT ALARM TWICE IN A ROW, because an alarm
 * re-raised every sweep is a page every interval for one condition — and an
 * operator who silences that stops seeing the next one. De-duplication is not
 * suppression: a condition that clears and returns alarms again.
 *
 * <p>⚠️ IT HOLDS NO STORE. Telemetry that read the store would make observing
 * the cluster cost what running it costs.
 */
public final class RetentionObservable {

    /** What an alarm is about — a CLOSED set, so a dashboard can enumerate it. */
    public enum Kind {
        /** A shard copy has not reported within {@code reportTimeout}. */
        COPY_SILENT,
        /** Unread data is within an hour of {@code maxRetention}. */
        APPROACHING_CEILING,
        /** A paused stream's unread data is already past {@code minRetention}. */
        PAUSED_PAST_FLOOR
    }

    /** How close to the ceiling counts as approaching it. */
    private static final Duration CEILING_WARNING = Duration.ofHours(1);

    /** One shard copy of one stream — what a silence is about. */
    record CopyKey(RunKey stream, String shardCopy) {
    }

    /**
     * One raised condition.
     *
     * <p>⚠️ {@link #labels()} CARRIES THE KIND AND NOTHING ELSE. The stream is
     * on the event, where its cardinality costs one log line rather than a
     * series per stream forever.
     */
    public record Alarm(Kind kind, RunKey stream, String shardCopy, String detail) {

        public Alarm {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(stream, "stream");
            Objects.requireNonNull(detail, "detail");
        }

        /** An alarm about a stream rather than about one copy of it. */
        static Alarm of(Kind kind, RunKey stream, String detail) {
            return new Alarm(kind, stream, null, detail);
        }

        /** The metric labels this alarm may carry. */
        public Map<String, String> labels() {
            return Map.of("kind", kind.name());
        }
    }

    /** Where alarms go. */
    @FunctionalInterface
    public interface Sink {
        void raise(Alarm alarm);
    }

    private final Clock clock;
    private final Duration minRetention;
    private final Duration maxRetention;
    private final Duration reportTimeout;
    private final Sink sink;

    /** Per stream, when the oldest record nobody has read was written. */
    private final Map<RunKey, Instant> oldestUnread = new ConcurrentHashMap<>();

    /**
     * Per COPY, when it last reported.
     *
     * <p>⚠️ PER COPY, NOT PER STREAM, and review MEASURED why: keyed by stream,
     * two healthy copies keep the entry fresh while a third is wedged, so no
     * alarm fires — while {@link WatermarkTable} takes {@code min()} across ALL
     * known copies and computes freshness from the OLDEST report, so that
     * stream's watermark IS frozen. Research 09 §9 says "any COPY silent", and
     * the stream-keyed reading needs EVERY copy silent.
     */
    private final Map<CopyKey, Instant> lastReport = new ConcurrentHashMap<>();

    /** Per stream, since when it has been paused. */
    private final Map<RunKey, Instant> pausedSince = new ConcurrentHashMap<>();

    /** Conditions already raised and not yet cleared. */
    private final Map<String, Boolean> raised = new ConcurrentHashMap<>();

    public RetentionObservable(Clock clock, Duration minRetention, Duration maxRetention,
            Duration reportTimeout, Sink sink) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.minRetention = requirePositive(minRetention, "minRetention");
        this.maxRetention = requirePositive(maxRetention, "maxRetention");
        if (maxRetention.compareTo(minRetention) <= 0) {
            throw new IllegalArgumentException("maxRetention " + maxRetention
                    + " is not longer than minRetention " + minRetention);
        }
        this.reportTimeout = requirePositive(reportTimeout, "reportTimeout");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    private static Duration requirePositive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(what + " is never " + value);
        }
        return value;
    }

    /** When the oldest record no consumer of {@code stream} has read was written. */
    public void observeOldestUnread(RunKey stream, Instant writtenAt) {
        oldestUnread.put(Objects.requireNonNull(stream, "stream"),
                Objects.requireNonNull(writtenAt, "writtenAt"));
    }

    /** That a copy of {@code stream} reported just now. */
    public void observeReport(RunKey stream, String shardCopy) {
        lastReport.put(new CopyKey(Objects.requireNonNull(stream, "stream"),
                Objects.requireNonNull(shardCopy, "shardCopy")), clock.instant());
    }

    /**
     * That a copy is gone rather than quiet.
     *
     * <p>⚠️ WITHOUT THIS NOTHING IS EVER FORGOTTEN, and review measured what
     * that costs: an entry for a deleted index or a relocated copy stays for
     * ever, so its silence alarms for ever and — under the de-duplication rule
     * — the condition can never re-raise for a real incident.
     */
    public void deregisterCopy(RunKey stream, String shardCopy) {
        CopyKey copy = new CopyKey(Objects.requireNonNull(stream, "stream"),
                Objects.requireNonNull(shardCopy, "shardCopy"));
        lastReport.remove(copy);
        raised.remove(copyKey(copy));
    }

    /**
     * That {@code stream} has nothing unread — every record written has been
     * consumed, or the stream is gone.
     *
     * <p>⚠️ THE OTHER HALF OF FORGETTING. {@link #outageBudget()} is the WORST
     * stream's, so one entry left behind for a deleted index counts the whole
     * cluster down past zero while nothing is at risk.
     */
    public void observeNothingUnread(RunKey stream) {
        Objects.requireNonNull(stream, "stream");
        oldestUnread.remove(stream);
        raised.remove(key(Kind.APPROACHING_CEILING, stream));
        // ⚠️ THE PAUSED STATE GOES TOO, because this method's other meaning is
        // "the stream is gone" -- and a deleted index that was paused would
        // otherwise keep its entry and its de-duplication slot for ever, which
        // is per-index unbounded growth plus a condition that can never clear.
        pausedSince.remove(stream);
        raised.remove(key(Kind.PAUSED_PAST_FLOOR, stream));
    }

    /** That {@code stream} has been paused since {@code since}. */
    public void observePaused(RunKey stream, Instant since) {
        pausedSince.put(Objects.requireNonNull(stream, "stream"),
                Objects.requireNonNull(since, "since"));
    }

    /** That {@code stream} is consuming again. */
    public void observeResumed(RunKey stream) {
        pausedSince.remove(Objects.requireNonNull(stream, "stream"));
        raised.remove(key(Kind.PAUSED_PAST_FLOOR, stream));
    }

    /** Raises whatever is true now and has not already been raised. */
    public void sweep() {
        Instant now = clock.instant();
        for (Map.Entry<CopyKey, Instant> report : lastReport.entrySet()) {
            Duration quiet = Duration.between(report.getValue(), now);
            // ⚠️ STRICTLY PAST: a copy reporting on an exactly-regular cadence
            // is healthy, and alarming at the boundary pages on every one of
            // them.
            copyCondition(report.getKey(), quiet.compareTo(reportTimeout) > 0,
                    "copy " + report.getKey().shardCopy() + " has not reported for " + quiet
                            + ", so this stream's watermark is frozen at its position and "
                            + "its data cannot be collected");
        }
        for (Map.Entry<RunKey, Instant> unread : oldestUnread.entrySet()) {
            Duration age = Duration.between(unread.getValue(), now);
            // ⚠️ BEFORE, NOT AFTER. An alarm at the deletion tells an operator
            // about data that is already gone.
            boolean approaching = age.compareTo(maxRetention.minus(CEILING_WARNING)) > 0;
            condition(Kind.APPROACHING_CEILING, unread.getKey(), approaching,
                    "unread data is " + age + " old against a ceiling of " + maxRetention
                            + " -- past it, it is DELETED and the consumer has lost it");
        }
        for (Map.Entry<RunKey, Instant> paused : pausedSince.entrySet()) {
            Duration unreadAge = Duration.between(paused.getValue(), now);
            condition(Kind.PAUSED_PAST_FLOOR, paused.getKey(),
                    unreadAge.compareTo(minRetention) > 0,
                    "paused for " + unreadAge + ", which is past the " + minRetention
                            + " floor -- a paused shard reports a frozen pointer "
                            + "indefinitely and pins retention until the ceiling fires");
        }
    }

    private void copyCondition(CopyKey copy, boolean holds, String detail) {
        String key = copyKey(copy);
        if (!holds) {
            raised.remove(key);
            return;
        }
        if (raised.putIfAbsent(key, Boolean.TRUE) == null) {
            sink.raise(new Alarm(Kind.COPY_SILENT, copy.stream(), copy.shardCopy(), detail));
        }
    }

    private void condition(Kind kind, RunKey stream, boolean holds, String detail) {
        String key = key(kind, stream);
        if (!holds) {
            // ⚠️ CLEARING IS WHAT KEEPS DE-DUPLICATION FROM BECOMING
            // SUPPRESSION: the second incident is a second incident.
            raised.remove(key);
            return;
        }
        if (raised.putIfAbsent(key, Boolean.TRUE) == null) {
            sink.raise(Alarm.of(kind, stream, detail));
        }
    }

    private static String key(Kind kind, RunKey stream) {
        return kind + "/" + stream;
    }

    private static String copyKey(CopyKey copy) {
        return Kind.COPY_SILENT + "/" + copy.stream() + "/" + copy.shardCopy();
    }

    /**
     * How long this cluster can stay broken before data is lost, or null when
     * nothing is known.
     *
     * <p>⚠️ NULL RATHER THAN ZERO FOR AN EMPTY OBSERVABLE: a budget of zero
     * would page an operator about a cluster that has not started.
     */
    public Duration outageBudget() {
        Instant now = clock.instant();
        Duration worst = null;
        for (Instant writtenAt : oldestUnread.values()) {
            Duration budget = minRetention.minus(Duration.between(writtenAt, now));
            if (worst == null || budget.compareTo(worst) < 0) {
                worst = budget;
            }
        }
        return worst;
    }

    /** The K streams whose unread data is oldest, worst first. */
    public List<Map.Entry<RunKey, Duration>> oldestUnreadTopK(int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k is never " + k);
        }
        Instant now = clock.instant();
        List<Map.Entry<RunKey, Duration>> ages = new ArrayList<>();
        for (Map.Entry<RunKey, Instant> unread : oldestUnread.entrySet()) {
            ages.add(Map.entry(unread.getKey(), Duration.between(unread.getValue(), now)));
        }
        ages.sort(Comparator.comparing(Map.Entry<RunKey, Duration>::getValue).reversed());
        return List.copyOf(ages.subList(0, Math.min(k, ages.size())));
    }
}
