// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.format.ChainEntry;
import java.io.IOException;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * What the dedup tests share, so the two halves cannot drift apart.
 *
 * <p>⚠️ EXTRACTED WHEN `SequencerDedupTest` CROSSED 500 LINES -- code-structure
 * rule 1, split rather than raise. The seam is real: one half is what happens
 * inside a single leader, the other is what survives a takeover, and they fail
 * for different reasons. `InvariantFixtures` was split off the same way.
 */
final class DedupFixtures {

    static final String PREFIX = "bins/cluster-a";
    static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    static final RunKey RA = new RunKey(A, 0);

    private DedupFixtures() {
    }

    static final class TestClock extends Clock {
        @Override public long millis() {
            return 1_000_000L;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "",
                Duration.ofSeconds(10), Duration.ofSeconds(3)), new TestClock());
    }

    static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(RA, n);
        return m;
    }

    /**
     * The offset {@code segmentKey}'s run received in {@code delta}.
     *
     * <p>⚠️ Absent is a FAILURE, not zero: a caller matches its own flush by
     * segment key, so a delta that does not carry it has already broken the
     * contract this suite is about.
     */
    static long firstOffsetOf(CommitDelta delta, String segmentKey) {
        for (var segment : delta.segments()) {
            if (segment.segmentKey().equals(segmentKey)) {
                for (RunCommit run : segment.runs()) {
                    if (run.key().equals(RA)) {
                        return run.firstOffset();
                    }
                }
            }
        }
        throw new AssertionError("the returned delta carries nothing for " + segmentKey
                + " -- a caller matches its own flush by segment key, so this IS the failure");
    }

    /**
     * How many chain deltas carry {@code segmentKey}.
     *
     * <p>⚠️ THE ROW'S HEADLINE CLAIM IS "APPENDS NOTHING", and only the chain
     * can say so. Asserting the returned delta's offsets does not: review
     * measured `log.commitAll(replays)` inserted before the answer -- answering
     * from the window AND appending the replay durably -- leaving all 322 tests
     * green while three records held two sets of committed offsets.
     *
     * <p>⚠️ AN OFFSETS-ONLY ASSERTION IS STRUCTURALLY BLIND to it, which is the
     * same reason stated the other way round: `ChainReplay` folds with
     * {@code merge(key, lastOffset + 1, Math::max)}, so a duplicate appended at
     * its original offsets advances nothing and every stream still looks right.
     */
    static int deltasCarrying(BinStore store, String segmentKey) throws IOException {
        int found = 0;
        for (var stat : store.list(PREFIX + "/ctl/log/", null, 1000).objects()) {
            if (!stat.key().endsWith(".delta")) {
                continue;
            }
            try (var in = store.get(stat.key())) {
                if (ChainEntry.decode(in.readAllBytes()) instanceof CommitDelta d
                        && d.segments().stream().anyMatch(s -> s.segmentKey().equals(segmentKey))) {
                    found++;
                }
            }
        }
        return found;
    }
}
