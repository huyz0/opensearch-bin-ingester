// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.io.IOException;
import java.lang.System.Logger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The one job LIST keeps (M7.8, FR-9, research 06 §4, cost rules R2 and R15).
 *
 * <p>⚠️ A POD THAT DIED AFTER THE PUT AND BEFORE THE COMMIT leaves a segment
 * the commit log <b>by definition cannot know about</b>. {@link SegmentGc} is
 * driven from the log and so cannot see it; this sweep is the periodic
 * reconciliation that can, and it is the only place in this design where LIST
 * is legitimate.
 *
 * <p>⚠️ THE GRACE PERIOD MUST EXCEED THE MAXIMUM POSSIBLE COMMIT DELAY, AND
 * GETTING IT WRONG DELETES ACKNOWLEDGED DATA. An uncommitted segment is not
 * necessarily an orphan: its commit may still be in flight, and the degraded
 * {@code ctl/inbox/} path (M8's) is slower than the direct one. The five-minute
 * maximum commit delay remains a conservative stated bound, not a measured
 * worst case. The default is generous — one hour, research 06 §4 — and the case
 * that matters is the one that KEEPS.
 *
 * <p>⚠️ A COMMITTED SEGMENT IS NEVER A CANDIDATE, whatever its age. Only the
 * log knows whether its records have been read; a sweep that deleted by age
 * alone would delete live, referenced data the moment retention exceeded the
 * grace.
 *
 * <p>⚠️ ONE HOUR-PREFIX PER PASS, and that is what bounds the LIST rate under
 * R15's ~1/s ceiling. A sweep over the whole data prefix would issue a LIST per
 * 1,000 objects of the entire retention window in one burst — 43,200 objects an
 * hour at Scenario A, so 44 calls per hour-prefix and thousands for a window.
 *
 * <p>⚠️ IT READS NAMES, NEVER BODIES. No GET and no stat: the age is in the key
 * ({@link SegmentKey#timestampOf}), and a store's own last-modified time is not
 * the write time once an object has been copied by a lifecycle rule or a
 * restore.
 */
public final class OrphanSweep {

    private static final Logger LOG = java.lang.System.getLogger(OrphanSweep.class.getName());

    /** What a store returns per LIST, and therefore the most worth asking for. */
    private static final int MAX_PAGE = 1000;

    /**
     * The largest page a store returns, and therefore the one a production
     * sweep asks for (M8.5) -- a smaller page is the same objects in more
     * LIST calls.
     */
    public static final int MAX_PAGE_SIZE = MAX_PAGE;

    /**
     * How long an uncommitted segment is left alone (research 06 §4).
     *
     * <p>⚠️ NAMED HERE RATHER THAN LEFT TO EACH CALLER, because a caller
     * choosing its own from memory is how this becomes ten minutes on one
     * deployment. It is deliberately GENEROUS: it must exceed the maximum
     * possible commit delay including the degraded {@code ctl/inbox/} path
     * (M8's), which is slower than the direct one and does not exist yet to be
     * measured — so the number is an upper bound on a guess, not a measurement,
     * and shortening it is how acknowledged data is deleted.
     */
    public static final Duration DEFAULT_GRACE = Duration.ofHours(1);

    /**
     * The clock skew this sweep is sized against: M8's criterion 14, which
     * {@code ClockSkewIT} runs at (M8.52).
     */
    public static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(5);

    /**
     * The longest a PUT segment's commit is taken to be in flight (M8.52).
     *
     * <p>⚠️ A STATED CONSERVATIVE BOUND, NOT A MEASURED WORST CASE: a forwarded
     * commit times out at the lease TTL (seconds), while the degraded
     * {@code ctl/inbox/} path exists and is slower. Shortening this bound without
     * measuring the maximum in-flight delay is how acknowledged data is deleted.
     */
    public static final Duration MAX_COMMIT_DELAY = Duration.ofMinutes(5);

    /**
     * The shortest grace this sweep accepts: a slow writer and a fast sweeper
     * each shift a segment's age by the skew, so twice it, plus the commit
     * delay (M8.52).
     */
    public static final Duration MIN_GRACE = MAX_CLOCK_SKEW.multipliedBy(2).plus(MAX_COMMIT_DELAY);

    /**
     * Refuses a grace shorter than {@link #MIN_GRACE}, saying why.
     *
     * <p>⚠️ PUBLIC SO A CALLER THAT BUILDS ITS SWEEP LATE -- the retention loop,
     * inside a tick -- can refuse at startup instead.
     */
    public static void checkGrace(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        if (grace.compareTo(MIN_GRACE) < 0) {
            throw new IllegalArgumentException("grace " + grace + " is under " + MIN_GRACE
                    + ": a segment's age is its writer's clock against this sweep's, so a "
                    + MAX_CLOCK_SKEW + " clock skew each way eats twice that, and a commit may "
                    + "still be in flight for " + MAX_COMMIT_DELAY + " -- a shorter grace "
                    + "deletes a segment whose commit has not landed yet");
        }
    }

    /**
     * How often one hour-prefix is swept (research 06 §4).
     *
     * <p>⚠️ THE R15 CLAIM IS ABOUT THIS NUMBER. One pass costs one LIST per
     * 1,000 keys of one hour-prefix — 44 calls for a 43,200-object hour at
     * Scenario A — and a rate is calls divided by a period. At one pass an
     * hour that is ~0.012 LIST/s sustained, three orders below R15's ~1/s
     * ceiling; at one pass a minute it is 0.73/s, which is under the ceiling by
     * less than a factor of two. The period is part of the budget, not a
     * scheduling detail.
     */
    public static final Duration DEFAULT_PERIOD = Duration.ofHours(1);

    /** {@code <prefix>/data/yyyy/MM/dd/HH/} — one hour, and nothing wider. */
    private static final java.util.regex.Pattern HOUR_PREFIX =
            java.util.regex.Pattern.compile(".*/data/\\d{4}/\\d{2}/\\d{2}/\\d{2}/$");

    /** What one pass over one hour-prefix did. */
    public record Result(int deleted, int withinGrace, int committed, int unreadable) {
    }

    private final BinStore store;
    private final Clock clock;
    private final Duration grace;
    private final int pageSize;
    private final int deleteBatchSize;

    public OrphanSweep(BinStore store, Clock clock, Duration grace, int pageSize,
            int deleteBatchSize) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        checkGrace(grace);
        if (pageSize <= 0 || pageSize > MAX_PAGE) {
            // ⚠️ ASKING FOR MORE THAN A STORE RETURNS IS NOT A BIGGER PAGE, it
            // is the same page with a budget asserted against the number asked
            // for rather than the number returned -- which is not a budget.
            throw new IllegalArgumentException("pageSize is 1.." + MAX_PAGE + ", not "
                    + pageSize);
        }
        if (deleteBatchSize <= 0) {
            throw new IllegalArgumentException("deleteBatchSize is never " + deleteBatchSize);
        }
        this.grace = grace;
        this.pageSize = pageSize;
        this.deleteBatchSize = deleteBatchSize;
    }

    /**
     * One pass over one hour-prefix.
     *
     * @param committedKeys segment keys the commit log names — ⚠️ a KEEP list,
     *     and it fails OPEN: a caller that passed an incomplete set would have
     *     this sweep delete committed data, so it is the caller's whole chain
     *     or nothing
     */
    public Result sweep(String hourPrefix, Set<String> committedKeys) {
        Objects.requireNonNull(hourPrefix, "hourPrefix");
        Objects.requireNonNull(committedKeys, "committedKeys");
        if (!HOUR_PREFIX.matcher(hourPrefix).matches()) {
            // ⚠️ REFUSED RATHER THAN TRUSTED. `bucket/data/` is a legal prefix
            // to a store and walks the WHOLE retention window in one burst --
            // thousands of LIST calls where the budget is 44, and the R15 claim
            // in this class's javadoc would be false the first time a caller
            // passed one.
            throw new IllegalArgumentException("not an hour prefix: " + hourPrefix
                    + " -- one pass sweeps one hour, which is what bounds the LIST rate");
        }
        Instant now = clock.instant();
        List<String> doomed = new ArrayList<>();
        int withinGrace = 0;
        int committed = 0;
        int unreadable = 0;
        String startAfter = "";
        while (true) {
            ListPage page;
            try {
                page = store.list(hourPrefix, startAfter, pageSize);
            } catch (IOException failed) {
                LOG.log(Logger.Level.WARNING, () -> "the orphan sweep could not list "
                        + hourPrefix + "; the next pass tries again: " + failed);
                break;
            }
            for (ObjectStat object : page.objects()) {
                String key = object.key();
                if (committedKeys.contains(key)) {
                    committed++;
                    continue;
                }
                long writtenAt;
                try {
                    writtenAt = SegmentKey.timestampOf(key);
                } catch (IllegalArgumentException notASegment) {
                    unreadable++;
                    continue;
                }
                // ⚠️ STRICTLY PAST. An object exactly one grace old is still
                // inside it, and a sweep firing AT the boundary races the
                // commit it is waiting for.
                if (Duration.between(Instant.ofEpochMilli(writtenAt), now).compareTo(grace) > 0) {
                    doomed.add(key);
                } else {
                    withinGrace++;
                }
            }
            Optional<String> next = page.nextStartAfter();
            if (next.isEmpty()) {
                break;
            }
            startAfter = next.get();
        }
        return new Result(deleteInBatches(doomed), withinGrace, committed, unreadable);
    }

    private int deleteInBatches(List<String> keys) {
        int deleted = 0;
        for (int from = 0; from < keys.size(); from += deleteBatchSize) {
            List<String> batch = keys.subList(from, Math.min(from + deleteBatchSize, keys.size()));
            try {
                store.delete(batch);
                deleted += batch.size();
            } catch (IOException failed) {
                LOG.log(Logger.Level.WARNING, () -> "a batch of " + batch.size()
                        + " orphaned segments was not deleted; the next sweep finds them "
                        + "again: " + failed);
            }
        }
        return deleted;
    }
}
