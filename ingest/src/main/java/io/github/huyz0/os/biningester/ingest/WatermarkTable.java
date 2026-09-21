// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Where every known copy of each stream has got to (M7.2, FR-9,
 * <a href="../../../../../../docs/internal/product/decisions/0005-no-consumer-offset-store.md">ADR-0005</a>).
 *
 * <p>⚠️ THIS IS NOT AN OFFSET STORE. Nothing resumes from it, nothing seeks
 * with it, and losing it costs nothing but a pause in GC — which is the correct
 * failure direction. It exists to answer one question: <b>may this segment be
 * deleted yet</b>, and it may only ever answer "not yet, keep it longer".
 *
 * <p>⚠️ FOUR RULES, AND EACH OF THE FIRST THREE HAS A SILENT DELETION BEHIND IT
 * (research 09 §6.2-6.4):
 *
 * <ol>
 * <li><b>{@code min()} is over ALL KNOWN copies, not the reporting ones.</b>
 * With {@code all_active = true} every copy consumes the partition
 * independently, so a lagging replica in another AZ is a consumer. A copy that
 * goes quiet <b>freezes</b> at its last value — dropping it from the
 * {@code min()} is how a node down for ten minutes comes back to deleted
 * data.</li>
 * <li><b>Silence past {@code reportTimeout} makes the whole answer UNFRESH</b>,
 * rather than removing the silent copy. An unfresh answer is one GC must not
 * delete on. ⚠️ Freshness is taken from the <b>oldest</b> report, never the
 * newest: the newest says "somebody spoke recently", which is true of a cluster
 * where one shard has been down for an hour.</li>
 * <li><b>A new allocation id retires nothing.</b> A relocation produces a new
 * id reporting for the same partition while the old one goes quiet, and both
 * legitimately exist mid-relocation. Retirement is by explicit deregistration
 * or by an expiry <b>longer than {@code minRetention}</b> — by which time the
 * data the copy was protecting is deletable on the time floor anyway.</li>
 * <li><b>A position that goes BACKWARDS is believed.</b> The reported pointer
 * is the in-memory one, so a copy that restarts resumes from its last Lucene
 * commit and reports lower than before. The later frame wins. A per-copy
 * {@code max()} — which is what the word "watermark" suggests — pins the value
 * at the pre-crash position and deletes exactly what the restarted shard is
 * about to read again. Monotonicity belongs to the retention RULE, never to
 * this feed.</li>
 * </ol>
 *
 * <p>⚠️ AN UNKNOWN STREAM IS UNKNOWN, NOT ZERO AND NOT MAX. Nobody has reported
 * is not the same fact as nobody has read: zero keeps everything and is
 * indistinguishable from a real zero, {@code Long.MAX_VALUE} deletes the whole
 * stream. {@link Watermark#consumedUpTo()} refuses to answer for a stream no
 * copy has reported on, so the caller cannot take a default by accident.
 *
 * <p>⚠️ IT HOLDS NO STORE, NO SOCKET AND NO WALL CLOCK. The clock is a seam;
 * observing a frame and reading an answer cost zero object-store requests
 * (NFR-2), which is what lets a node report every interval forever while idle.
 */
public final class WatermarkTable {

    /** One copy's last report: the position, and when it arrived. */
    private record Report(long consumedUpTo, Instant at) {
    }

    private final Clock clock;
    private final Duration reportTimeout;
    private final Duration copyExpiry;

    /** By stream, then by shard copy (the allocation id). */
    private final Map<RunKey, Map<String, Report>> byStream = new ConcurrentHashMap<>();

    /**
     * @param reportTimeout how long a copy may be silent before the answer for
     *     its stream stops being trustworthy
     * @param copyExpiry how long a copy may be silent before it is retired
     *     altogether — ⚠️ longer than {@code minRetention}, or a copy is
     *     dropped while its data is still inside the outage budget
     * @param minRetention the time floor, taken only to check {@code copyExpiry}
     *     against it
     */
    public WatermarkTable(Clock clock, Duration reportTimeout, Duration copyExpiry,
            Duration minRetention) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.reportTimeout = requirePositive(reportTimeout, "reportTimeout");
        this.copyExpiry = requirePositive(copyExpiry, "copyExpiry");
        requirePositive(minRetention, "minRetention");
        if (copyExpiry.compareTo(minRetention) <= 0) {
            // ⚠️ THE CONFIGURATION THAT QUIETLY TURNS THE OUTAGE BUDGET OFF. A
            // copy retired inside the retention window is a copy whose data is
            // deleted while it is still entitled to read it, and nothing
            // downstream can tell that from a copy that never existed.
            throw new IllegalArgumentException("copyExpiry " + copyExpiry
                    + " is not longer than minRetention " + minRetention
                    + " -- a copy retired inside the retention window loses data it is "
                    + "still entitled to read");
        }
        if (copyExpiry.compareTo(reportTimeout) <= 0) {
            // ⚠️ Otherwise the unfresh state is unreachable: a copy would be
            // gone before it could be called stale, and the brake in rule 2
            // would never engage.
            throw new IllegalArgumentException("copyExpiry " + copyExpiry
                    + " is not longer than reportTimeout " + reportTimeout
                    + " -- a copy that expires before it can be called stale makes the "
                    + "freshness brake unreachable");
        }
    }

    private static Duration requirePositive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(what + " is never " + value);
        }
        return value;
    }

    /**
     * Records one node's report for every stream it named.
     *
     * <p>⚠️ THE LATEST REPORT WINS PER COPY, INCLUDING A LOWER ONE. See rule 4.
     */
    public void observe(ConsumerProgress frame) {
        Objects.requireNonNull(frame, "frame");
        Instant now = clock.instant();
        for (ConsumerProgress.Entry entry : frame.entries()) {
            // ⚠️ A FRAME THAT CANNOT BE JOINED TO A STREAM THROWS RATHER THAN
            // BEING DROPPED: a dropped frame is a copy that goes silent for no
            // reason, and silence here is indistinguishable from a dead node.
            RunKey stream = RunKey.ofIndexUuid(entry.indexUuid(), entry.partition());
            byStream.computeIfAbsent(stream, k -> new ConcurrentHashMap<>())
                    .put(entry.shardCopy(), new Report(entry.consumedUpTo(), now));
        }
    }

    /**
     * Removes a copy that is known to be gone rather than quiet.
     *
     * <p>⚠️ REMOVING ONE NOBODY REPORTED IS NOT AN ERROR. This arrives from a
     * cluster-state change and may race the copy's first report; throwing would
     * turn an ordinary race into a failed applier thread.
     */
    public void deregister(RunKey stream, String shardCopy) {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(shardCopy, "shardCopy");
        Map<String, Report> copies = byStream.get(stream);
        if (copies == null) {
            return;
        }
        // ⚠️ THE EMPTY MAP IS LEFT IN PLACE RATHER THAN DETACHED. A
        // check-then-`remove(key, value)` is not atomic against a concurrent
        // `observe()`: the report lands in a map that is then unlinked, and the
        // copy looks silent when it is not. An empty map answers UNKNOWN
        // exactly as an absent one does, so keeping it costs one entry and
        // removes the race.
        copies.remove(shardCopy);
    }

    /**
     * What is known about one stream, with expired copies retired first.
     *
     * <p>⚠️ EXPIRY IS APPLIED HERE RATHER THAN BY A TIMER, so there is no path
     * that only fires if something schedules it — the defect
     * {@code PendingPool.expire()} carries (M6.24).
     */
    public Watermark of(RunKey stream) {
        Objects.requireNonNull(stream, "stream");
        Map<String, Report> copies = byStream.get(stream);
        if (copies == null) {
            return Watermark.unknown();
        }
        Instant now = clock.instant();
        // ⚠️ STRICTLY GREATER: a copy whose last report is EXACTLY copyExpiry
        // old is still known. The boundary is stated because retiring on `>=`
        // drops a copy one tick early, and one tick early is still data
        // deleted for a copy that was entitled to it.
        copies.entrySet().removeIf(e -> Duration.between(e.getValue().at(), now)
                .compareTo(copyExpiry) > 0);
        long min = Long.MAX_VALUE;
        Instant oldest = null;
        for (Report report : copies.values()) {
            min = Math.min(min, report.consumedUpTo());
            // ⚠️ THE OLDEST REPORT, NOT THE NEWEST -- rule 2.
            if (oldest == null || report.at().isBefore(oldest)) {
                oldest = report.at();
            }
        }
        if (oldest == null) {
            return Watermark.unknown();
        }
        boolean fresh = Duration.between(oldest, now).compareTo(reportTimeout) <= 0;
        return new Watermark(true, fresh, min);
    }

    /** How many streams any copy has reported on. */
    public int size() {
        return byStream.size();
    }

    /**
     * The answer for one stream.
     *
     * <p>⚠️ {@code known} AND {@code fresh} ARE DIFFERENT FACTS. Unknown means
     * no copy has ever reported, and nothing may be deleted on the watermark
     * clause at all. Known-but-unfresh means the copies are there and one has
     * been quiet too long, so the {@code min()} exists but must not be trusted.
     *
     * <p>⚠️ IT IS A CLASS AND NOT A RECORD, SO THE POSITION HAS NO UNGUARDED
     * ACCESSOR. A record would expose the backing field as {@code position()},
     * and whatever an unknown watermark held would become readable: zero keeps
     * everything and is indistinguishable from a real zero, and a later tidy to
     * {@code Long.MAX_VALUE} deletes the whole stream. The only way to read a
     * position is {@link #consumedUpTo()}, which refuses when there is none.
     */
    public static final class Watermark {

        private final boolean known;
        private final boolean fresh;
        private final long position;

        Watermark(boolean known, boolean fresh, long position) {
            if (!known && fresh) {
                throw new IllegalArgumentException("a stream nobody has reported on is "
                        + "never fresh -- freshness over no copies is vacuously true, and "
                        + "a vacuous true here says every copy reported when none did");
            }
            this.known = known;
            this.fresh = fresh;
            this.position = position;
        }

        static Watermark unknown() {
            return new Watermark(false, false, Long.MIN_VALUE);
        }

        /** Whether any copy has ever reported on this stream. */
        public boolean known() {
            return known;
        }

        /** Whether every known copy reported within {@code reportTimeout}. */
        public boolean fresh() {
            return fresh;
        }

        /**
         * The offset every known copy has consumed up to, EXCLUSIVE.
         *
         * @throws IllegalStateException if no copy has reported — ⚠️ a default
         *     here is the default that deletes
         */
        public long consumedUpTo() {
            if (!known) {
                throw new IllegalStateException("no copy has reported on this stream, which "
                        + "is not the same fact as no copy having read anything");
            }
            return position;
        }

        @Override
        public String toString() {
            return "Watermark[" + (known ? (fresh ? "fresh " : "STALE ") + position
                    : "unknown") + "]";
        }
    }
}
