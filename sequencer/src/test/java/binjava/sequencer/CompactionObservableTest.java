// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.RunKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The compaction-trigger observable (M4.14, acceptance criterion 10).
 *
 * <p>⚠️ WHY M4 OWES A NUMBER TO M7. Q17 asks what threshold should trigger
 * compaction, and the honest answer is that nobody knows yet — it depends on how
 * commit-log index entries actually distribute across streams in a real
 * workload. So M4's job is not to compact anything; it is to emit the
 * distribution, so that M7 chooses its threshold from data rather than from an
 * opinion. This is a measurement obligation, and it is the last acceptance
 * criterion with no implementation behind it.
 *
 * <p>⚠️ NOT AS A LABEL, AND THE NUMBER IS THE REASON. observability.md rule 1
 * closes the label allow-list: a `stream` label costs ~2,400,000 series and
 * ~5.7 GiB of TSDB memory, on a system whose whole purpose is to make ingestion
 * cheap. Our own telemetry would outweigh the traffic it describes. So the
 * distribution goes out as a HISTOGRAM — bucket counts, no per-stream identity —
 * and the identities that matter go out as a periodic TOP-K EVENT, which is rule
 * 2's "attribution goes to events, not to labels".
 */
class CompactionObservableTest {

    /**
     * ⚠️ FIXED UUIDs, so the top-K ordering is a statement about ENTRY COUNTS
     * and not about whichever identifiers happened to be generated. A random
     * UUID here would make the tie-break -- and therefore the assertion -- vary
     * between runs.
     */
    private static RunKey stream(int n) {
        return new RunKey(
                java.util.UUID.fromString("00000000-0000-0000-0000-00000000000" + n), 0);
    }

    private static CompactionObservable withEntries(Map<RunKey, Integer> counts) {
        CompactionObservable o = new CompactionObservable(3);
        counts.forEach((k, n) -> {
            for (int i = 0; i < n; i++) {
                o.recordIndexEntry(k);
            }
        });
        return o;
    }

    @Test
    void theHistogramCountsSTREAMSPerBucketNotEntriesPerStream() {
        var o = withEntries(Map.of(
                stream(1), 1,
                stream(2), 3,
                stream(3), 40,
                stream(4), 900));

        Map<String, Long> h = o.histogram();

        // ⚠️ THE SHAPE IS "HOW MANY STREAMS HAVE ROUGHLY THIS MANY ENTRIES",
        // which is the distribution M7 needs to pick a threshold from. A
        // histogram keyed the other way round -- entries per named stream --
        // is the per-stream label wearing a different hat, and costs the same
        // 2.4 million series.
        assertThat(h.values().stream().mapToLong(Long::longValue).sum())
                .as("every stream lands in exactly one bucket").isEqualTo(4L);
        assertThat(h.keySet())
                .as("buckets are ranges, never stream identities -- nothing here "
                        + "may be a stream name")
                .allSatisfy(k -> assertThat(k).matches("[0-9]+(\\+|-[0-9]+)"));
    }

    @Test
    void theTopKEventNamesTheHEAVIESTStreamsAndOnlyK() {
        var o = withEntries(Map.of(
                stream(1), 5,
                stream(2), 900,
                stream(3), 40,
                stream(4), 7,
                stream(5), 300));

        List<CompactionObservable.StreamCount> top = o.topK();

        assertThat(top).as("exactly K, so the event is bounded however many streams exist")
                .hasSize(3);
        assertThat(top.stream().map(s -> s.stream().indexId()).toList())
                .as("and they are the heaviest, in order -- this is the attribution "
                        + "that the histogram deliberately throws away")
                .containsExactly(stream(2).indexId(), stream(5).indexId(), stream(3).indexId());
        assertThat(top.get(0).entries()).isEqualTo(900L);
    }

    @Test
    void aStreamWithNoEntriesIsNotReported() {
        var o = new CompactionObservable(3);
        assertThat(o.histogram().values().stream().mapToLong(Long::longValue).sum())
                .as("an empty log describes no streams").isZero();
        assertThat(o.topK()).as("and names none").isEmpty();
    }

    @Test
    void topKIsBoundedWhenFewerStreamsExistThanK() {
        var o = withEntries(Map.of(stream(1), 4));
        assertThat(o.topK())
                .as("K is a cap, not a quota -- a log with one stream reports one")
                .hasSize(1);
    }

    @Test
    void bucketsAreEXPONENTIALSoTheTailIsVisible() {
        var o = withEntries(Map.of(
                stream(1), 1,
                stream(2), 10,
                stream(3), 100,
                stream(4), 1000,
                stream(5), 10000));

        // ⚠️ EXPONENTIAL, NOT LINEAR. The question M7 asks is "where is the
        // knee", and a linear histogram over a distribution spanning four
        // orders of magnitude puts every interesting stream in the last
        // bucket -- which reports that a tail exists while hiding its shape.
        assertThat(o.histogram().entrySet().stream()
                .filter(e -> e.getValue() > 0).count())
                .as("five streams spanning 1..10,000 entries occupy five distinct buckets")
                .isEqualTo(5L);
    }

    @Test
    void everyBucketBOUNDARYIsPinned() {
        // ⚠️ SIX MUTATIONS TO THE BUCKETING SURVIVED BEFORE THIS TEST EXISTED,
        // measured one at a time: `<=` becoming `<` (every boundary value moves
        // up a bucket), the final bucket becoming CLOSED, the zero-fill loop
        // deleted, `BUCKETS[i-1] + 1` becoming `BUCKETS[i-1]` (overlapping
        // labels that no longer partition the range), and the open bucket's
        // label losing its `+ 1`. The three assertions that existed checked a
        // total, a count of non-zero buckets, and a regex -- all of which hold
        // under every one of those. A histogram whose boundaries are not pinned
        // is a distribution nobody can read a threshold off.
        record Case(int entries, String bucket) {
        }
        List<Case> cases = List.of(
                new Case(1, "1-1"),
                new Case(2, "2-3"),
                new Case(3, "2-3"),
                new Case(4, "4-10"),
                new Case(10, "4-10"),
                new Case(11, "11-30"),
                new Case(30, "11-30"),
                new Case(31, "31-100"),
                new Case(100, "31-100"),
                new Case(101, "101-300"));
        for (Case c : cases) {
            var o = new CompactionObservable(3);
            for (int i = 0; i < c.entries(); i++) {
                o.recordIndexEntry(stream(1));
            }
            assertThat(o.histogram().get(c.bucket()))
                    .as("a stream with exactly %s entries lands in bucket %s -- an "
                            + "off-by-one here moves every boundary value", c.entries(),
                            c.bucket())
                    .isEqualTo(1L);
        }
    }

    @Test
    void theFinalBucketIsOPENEndedSoTheHeaviestStreamsAreNotHidden() {
        // ⚠️ THE ONE FAILURE THE CLASS DOC SAYS MUST NOT HAPPEN. A closed final
        // bucket discards the streams most likely to need compaction, which are
        // the only ones M7 is asking about -- and it discards them by counting
        // them as something smaller, so the distribution still looks complete.
        var o = new CompactionObservable(3);
        for (int i = 0; i < 200_000; i++) {
            o.recordIndexEntry(stream(1));
        }
        assertThat(o.histogram().get("100001+"))
                .as("a stream far past the last bound is reported as past it, not folded "
                        + "back into the bucket below")
                .isEqualTo(1L);
        assertThat(o.histogram().get("30001-100000"))
                .as("and is NOT counted in the closed bucket below it").isZero();
    }

    @Test
    void theBucketKeySetIsSTABLEEvenWhenNothingHasBeenRecorded() {
        // Deleting the zero-fill made the key set depend on the data, so a
        // dashboard reading it would gain and lose series as traffic moved.
        var empty = new CompactionObservable(3).histogram();
        var busy = withEntries(Map.of(stream(1), 5)).histogram();
        assertThat(empty.keySet()).as("the buckets are a fixed axis, not a function of "
                + "the data that happened to arrive").isEqualTo(busy.keySet());
        assertThat(empty.values()).as("and an empty log reports zeros, not nothing")
                .allSatisfy(v -> assertThat(v).isZero());
    }

    @Test
    void theLABELSPartitionTheRangeWithNoOverLAPAndNoGap() {
        var keys = new java.util.ArrayList<>(new CompactionObservable(3).histogram().keySet());
        long expectedLo = 1;
        for (String k : keys) {
            if (k.endsWith("+")) {
                assertThat(Long.parseLong(k.substring(0, k.length() - 1)))
                        .as("the open bucket starts exactly where the last closed one ended")
                        .isEqualTo(expectedLo);
                continue;
            }
            String[] parts = k.split("-");
            assertThat(Long.parseLong(parts[0]))
                    .as("bucket %s starts where the previous ended -- overlapping or "
                            + "gapped labels mean a stream is counted twice or not at all", k)
                    .isEqualTo(expectedLo);
            expectedLo = Long.parseLong(parts[1]) + 1;
        }
    }

    @Test
    void aTIEIsBrokenDeterministicallyByTheStreamItself() {
        // ⚠️ RunKey IS Comparable on (indexId, partitionId). Ordering on
        // indexId alone is not a total order, so two partitions of one index
        // with equal counts rank equal and the surviving order is whatever the
        // map iteration produced -- the periodic event would name a different
        // partition on each emission with nothing in the data having changed.
        RunKey p0 = new RunKey(stream(1).indexId(), 0);
        RunKey p1 = new RunKey(stream(1).indexId(), 1);
        for (int run = 0; run < 5; run++) {
            var o = new CompactionObservable(1);
            for (int i = 0; i < 9; i++) {
                o.recordIndexEntry(p1);
                o.recordIndexEntry(p0);
            }
            assertThat(o.topK().get(0).stream())
                    .as("the same counts give the same winner, every time")
                    .isEqualTo(p0);
        }
    }

    @Test
    void theCOMMITLOGFeedsItAndRecoveryREBUILDSItFromTheLog() throws Exception {
        // ⚠️ TWO BLOCKING FINDINGS IN ONE TEST. Review measured that nothing in
        // production ever called `recordIndexEntry`: the class satisfied
        // acceptance criterion 10 on paper while returning twelve zero buckets
        // forever, so M7 would have picked its threshold from an empty
        // distribution. And counting on the COMMIT path rather than the APPLY
        // path would have made the same commit log yield a different histogram
        // depending on process uptime -- zero for every stream straight after a
        // restart, with the log unchanged.
        var backing = new binjava.binstore.backend.MemoryBinStore();
        String prefix = "bins/cluster-a";
        RunKey a = stream(1);
        RunKey b = stream(2);

        CommitLog log = new CommitLog(backing, prefix, 1L);
        log.open(0L, 0L);
        log.commit("seg/0", Map.of(a, 3, b, 1));
        log.commit("seg/1", Map.of(a, 2));
        assertThat(log.compaction().topK())
                .as("the live path feeds it at all -- this is the half that was missing")
                .isNotEmpty();
        assertThat(log.compaction().topK().get(0).stream())
                .as("and `a` appears in two index entries against `b`'s one").isEqualTo(a);
        assertThat(log.compaction().topK().get(0).entries()).isEqualTo(2L);

        // A FRESH log over the SAME store, as a restart produces.
        CommitLog recovered = new CommitLog(backing, prefix, 1L);
        recovered.recover();
        assertThat(recovered.compaction().topK())
                .as("recovery rebuilds the distribution FROM THE LOG, so it does not "
                        + "depend on how long a process has been running")
                .isEqualTo(log.compaction().topK());
    }

    @Test
    void theObservableCarriesNOStreamIdentityIntoAnyMetricDimension() {
        var o = withEntries(Map.of(stream(1), 3, stream(2), 4));

        // ⚠️ THIS IS THE ASSERTION THAT CORRESPONDS TO THE GATE.
        // `check-metric-cardinality.sh` reads SOURCE for a forbidden label
        // name; it cannot see a stream identity that reaches a dimension at
        // RUNTIME through a variable. This closes that half: whatever the
        // histogram emits, no key of it may be a stream.
        assertThat(o.histogram().keySet())
                .allSatisfy(k -> {
                    assertThat(k).as("a bucket key is a RANGE, never an identity")
                            .matches("[0-9]+(\\+|-[0-9]+)");
                    assertThat(k).doesNotContain("-0000-");
                });
    }
}
