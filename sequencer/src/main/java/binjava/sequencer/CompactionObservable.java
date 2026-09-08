// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.RunKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The distribution of commit-log index entries across streams (M4.14).
 *
 * <p>⚠️ THIS MEASURES; IT DOES NOT DECIDE. Q17 asks what threshold should
 * trigger compaction, and the honest answer at M4 is that nobody knows: it
 * depends on how index entries actually distribute across streams under a real
 * workload. M7 owns the rule. M4 owes it the DATA, and that obligation is why
 * this class exists — without it M7 would choose a threshold from an opinion.
 *
 * <p>⚠️ A HISTOGRAM, NOT A PER-STREAM GAUGE, and the reason is a number rather
 * than a preference. observability.md rule 1 closes the metric label allow-list
 * because a {@code stream} label costs ~2,400,000 series and ~5.7 GiB of TSDB
 * memory. This project exists to make ingestion cheap; per-stream telemetry
 * would make our own observability heavier than the traffic it describes. So
 * the shape emitted is "how many streams have roughly this many entries" —
 * bucket counts, no identities — which is exactly the distribution a threshold
 * is chosen from.
 *
 * <p>⚠️ AND THE IDENTITIES STILL GET OUT, through the periodic top-K EVENT.
 * That is observability.md rule 2: attribution goes to events, not to labels. K
 * names are bounded however many streams exist, so the cost does not scale with
 * the dimension that made a label unaffordable.
 *
 * <p>⚠️ EXPONENTIAL BUCKETS. The question is "where is the knee", and index
 * entries per stream span orders of magnitude. Linear buckets would put every
 * interesting stream in the last one, reporting that a tail exists while hiding
 * its shape — which answers M7's question with the fact that it has a question.
 */
public final class CompactionObservable {

    /**
     * ⚠️ UPPER BOUNDS, and the last bucket is open-ended. A closed final bucket
     * silently discards the streams most likely to need compaction, which are
     * the only ones M7 is asking about.
     */
    private static final long[] BUCKETS = {1, 3, 10, 30, 100, 300, 1_000, 3_000,
        10_000, 30_000, 100_000};

    private final Map<RunKey, AtomicLong> entries = new ConcurrentHashMap<>();
    private final int k;

    public CompactionObservable(int k) {
        this.k = k;
    }

    /**
     * Replaces the distribution with one a replay derived from the log.
     *
     * <p>⚠️ REPLACES RATHER THAN ADDS, and that is the point: a recovered log
     * describes what the chain holds, not what this process watched arrive.
     * Merging would make a restart double every count it had already seen.
     */
    public void seed(Map<RunKey, Long> perStream) {
        entries.clear();
        perStream.forEach((k, n) -> entries.put(k, new AtomicLong(n)));
    }

    /** One more commit-log index entry landed for {@code stream}. */
    public void recordIndexEntry(RunKey stream) {
        entries.computeIfAbsent(stream, s -> new AtomicLong()).incrementAndGet();
    }

    /**
     * Streams per bucket, keyed by the bucket's RANGE.
     *
     * <p>⚠️ THE KEY IS A RANGE AND MUST STAY ONE. A key that carried a stream
     * identity would be the forbidden label arriving through a map instead of
     * through a tag, and {@code check-metric-cardinality.sh} greps source for
     * label NAMES — it cannot see an identity that reaches a dimension at
     * runtime through a variable. The test asserts the shape of these keys for
     * exactly that reason.
     */
    public Map<String, Long> histogram() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (int i = 0; i < BUCKETS.length; i++) {
            out.put(label(i), 0L);
        }
        out.put(label(BUCKETS.length), 0L);
        for (AtomicLong n : entries.values()) {
            int b = bucketOf(n.get());
            if (b >= 0) {
                out.merge(label(b), 1L, Long::sum);
            }
        }
        return out;
    }

    private static int bucketOf(long n) {
        if (n <= 0) {
            // ⚠️ ZERO IS NOT ONE. `label(0)` reads "1-1", so without this a
            // stream holding nothing is reported as holding exactly one entry
            // -- inflating the low end of the very distribution M7 uses to
            // decide a stream is NOT worth compacting. Unreachable while the
            // only writer increments, and that is exactly why it is written
            // down now rather than after a decrement path is added.
            return -1;
        }
        for (int i = 0; i < BUCKETS.length; i++) {
            if (n <= BUCKETS[i]) {
                return i;
            }
        }
        return BUCKETS.length;
    }

    private static String label(int i) {
        if (i == BUCKETS.length) {
            return BUCKETS[BUCKETS.length - 1] + 1 + "+";
        }
        long lo = i == 0 ? 1 : BUCKETS[i - 1] + 1;
        return lo + "-" + BUCKETS[i];
    }

    /** One stream and how many index entries it holds. */
    public record StreamCount(RunKey stream, long entries) {
    }

    /**
     * The K heaviest streams, heaviest first.
     *
     * <p>⚠️ K IS A CAP, NOT A QUOTA: a log with fewer streams than K reports
     * what it has. And the list is BOUNDED however many streams exist, which is
     * the whole reason attribution is affordable here and unaffordable as a
     * label.
     */
    public List<StreamCount> topK() {
        List<StreamCount> all = new ArrayList<>();
        entries.forEach((s, n) -> all.add(new StreamCount(s, n.get())));
        all.sort(Comparator.comparingLong(StreamCount::entries).reversed()
                // ⚠️ RunKey's OWN ordering, which is (indexId, partitionId).
                // Comparing on indexId alone is not a total order over RunKey:
                // two partitions of one index with equal counts rank equal, and
                // the surviving order is then whatever the map iteration
                // produced -- so the periodic event names partition 0 on one
                // emission and partition 1 on the next, and an operator diffing
                // consecutive events sees churn that is not in the data.
                .thenComparing(StreamCount::stream));
        return List.copyOf(all.subList(0, Math.min(k, all.size())));
    }
}
