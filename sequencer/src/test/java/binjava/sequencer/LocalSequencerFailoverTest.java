// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Failover: what a takeover COSTS, what it PRESERVES, and what it hands back
 * when it fails.
 *
 * <p>⚠️ Split out of {@code LocalSequencerTest} when M4.6e pushed that file past
 * the 500-line limit. code-structure.md rule 1: split it, do not raise the
 * limit — the same call M4.3b recorded when a 509-line test forced
 * {@code StatBlindStore} into its own file.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerFailoverTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    private static final String PREFIX = "bins/cluster-a";

    private static final class TestClock extends Clock {
        private final long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId z) {
            return this;
        }
    }

    private static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW),
                new TestClock());
    }

    private static LocalSequencer start(BinStore store, String pod) throws IOException {
        return LocalSequencer.start(store, PREFIX, manager(store, pod), 8).orElseThrow();
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(A, 0), n);
        return m;
    }

    private static CommitRequest request(String pod, long flushSeq, String seg, int n) {
        return new CommitRequest(pod, flushSeq, seg, counts(n));
    }

    /** A takeover after {@code terms} prior terms of {@code entries} commits each. */
    private static StoreCounts takeoverCost(int terms, int entries) throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        for (int t = 0; t < terms; t++) {
            LocalSequencer s = start(backing, "pod" + t);
            for (int i = 0; i < entries; i++) {
                s.commit(request("pod" + t, i, "seg/" + t + "/" + i, 1));
            }
            s.close();
        }
        CountingBinStore counting = new CountingBinStore(backing);
        LocalSequencer next = LocalSequencer
                .start(counting, PREFIX, manager(counting, "last"), 8).orElseThrow();
        assertThat(next.epoch()).isEqualTo(terms + 1);
        return counting.counts();
    }

    @Test
    void aStartThatFailsAFTERAcquiringHandsTheLeaseBackRatherThanStrandingIt()
            throws Exception {
        // ⚠️ THE BLOCKING DEFECT REVIEW FOUND. `start` acquired the lease, sealed
        // the predecessor, then failed opening its own chain -- and returned
        // without releasing. A caller reads that as "not the leader" while this
        // node HOLDS the term, so the cluster has no sequencer for a full TTL:
        // the outage the redrive branch exists to prevent, arriving by the other
        // door. Measured before the fix: retry EMPTY, other pod EMPTY, chain 1
        // sealed, chain 2 absent.
        MemoryBinStore backing = new MemoryBinStore();
        LocalSequencer first = start(backing, "pod1");
        first.commit(request("pod1", 1, "seg/0", 3));
        first.close();
        String opensChain2 = new CommitLog(backing, PREFIX, 2).keyFor(0);
        BinStore refuses = new RefuseKeyStore(backing, opensChain2);

        assertThatThrownBy(() -> LocalSequencer.start(refuses, PREFIX,
                manager(refuses, "pod2"), 8))
                .as("the failure is reported as a failure, not disguised as a lost race")
                .isInstanceOf(IOException.class);

        // ⚠️ THE ASSERTION THAT MATTERS: somebody else can lead IMMEDIATELY.
        assertThat(LocalSequencer.start(backing, PREFIX, manager(backing, "pod3"), 8))
                .as("the lease was handed back, so a successor takes over in "
                        + "milliseconds rather than waiting out the TTL")
                .isPresent();
    }

    @Test
    void aTakeoverReadsEveryEntryOfEveryANCESTORAndTHATISAREGRESSION() throws Exception {
        // ⚠️ THIS ASSERTION WAS `isEqualTo(2L)` AND IS NOW LINEAR IN TWO
        // DIMENSIONS. Stating it plainly because a cost threshold moving in the
        // WEAKENING direction is normally forbidden, so the reason has to carry
        // it rather than the convenience.
        // ⚠️ M4.6c made a takeover read only the predecessor's END -- two GETs
        // whatever its length. That was only ACHIEVABLE because the successor
        // was throwing the predecessor's offsets away, which is I2 and
        // acceptance criterion 4, the contract `Sequencer.commit` states
        // verbatim. M4.6e inherits those offsets and they can only come from the
        // deltas that assigned them, so the read is back. The constant was the
        // cost of a broken system.
        // ⚠️ AND THE CROSSING IS TRANSITIVE, so it is not merely the predecessor
        // that is re-read but EVERY ancestor: per-failover cost is O(all commit
        // log entries ever written). An earlier version of this test pinned only
        // the predecessor-LENGTH dimension, holding the term count at 2, so
        // re-reading every ancestor twice passed it unchanged. Both dimensions
        // are pinned below. M4.8's checkpoints are what make this constant
        // again; M4.9 must bound the CROSSING and not merely own-chain replay,
        // and its row now says so.
        // ⚠️ LISTS ARE COUNTED TOO. Asserting only GETs let a mutation adding one
        // LIST per predecessor ENTRY survive -- a request rate scaling with
        // records, which is the one thing non-negotiable 6 forbids outright, on
        // the path this commit adds.
        // ⚠️ +1 GET EVERY CASE BELOW, ADR-0029 (M4.32). `LocalSequencer.start`
        // now probes the immediate predecessor's slot 0 to learn whether it was
        // genuinely opened before deciding what to seal -- one extra GET per
        // takeover, NOT per ancestor or per entry: this fixture burns no epochs,
        // so the probe finds a real CONTINUE immediately and the walk it guards
        // never iterates. A walk that DID skip burned epochs would cost one more
        // GET per epoch skipped, which is the failover-only, O(epochs-skipped)
        // price ADR-0029's Alternatives section costs and accepts against the
        // I2 violation it closes -- see `LocalSequencerAncestorSealTest` for
        // that shape. Stating the move plainly rather than silently, same
        // discipline as the `isEqualTo(2L)` note above this one.
        StoreCounts small = takeoverCost(1, 12);
        StoreCounts longer = takeoverCost(1, 40);
        assertThat(small.gets())
                .as("12 entries: one GET each, plus a constant for the chain end, "
                        + "the seal probe, the inheritable-ancestor probe, the "
                        + "ancestry probe and the new CONTINUE")
                .isEqualTo(18L);
        assertThat(longer.gets())
                .as("40 entries: the SAME constant, so the slope in ENTRIES is "
                        + "exactly 1 -- this is what catches a super-linear regression")
                .isEqualTo(46L);
        // ⚠️ TOTAL, not an enumerated subset. Asserting gets+lists let a per-entry
        // `stat` survive -- and `stat` is precisely the request class the
        // ancestry probe added this round, so the enumeration was stale the
        // moment it was written. Twice now a mutation has hidden in the kind
        // nobody listed; `total()` closes the CLASS instead of adding a third
        // name and waiting for `puts` to be next.
        assertThat(small.total())
                .as("12 entries: EVERY request kind counted, so a per-entry stat, "
                        + "list or head shows up here -- +2 here, not +1: the "
                        + "inheritable-ancestor probe is a stat AND a get")
                .isEqualTo(28L);
        assertThat(longer.total())
                .as("40 entries: the same constant overhead, slope still 1")
                .isEqualTo(56L);

        // ⚠️ THE SECOND DIMENSION: hold the chain length and vary the number of
        // prior TERMS. The slope here is what makes the cost unbounded over a
        // cluster's lifetime, and nothing pinned it before.
        StoreCounts oneTerm = takeoverCost(1, 4);
        StoreCounts fourTerms = takeoverCost(4, 4);
        // ⚠️ Totals here too, and exact rather than a divided slope: integer
        // division left up to two requests of drift, which is room a mutation
        // can live in.
        assertThat(oneTerm.total()).as("one ancestor").isEqualTo(20L);
        assertThat(fourTerms.total())
                .as("four ancestors: each costs its entries plus a constant ONCE -- "
                        + "re-reading an ancestor per descendant would grow this "
                        + "quadratically. The inheritable-ancestor probe is ALSO "
                        + "once per takeover (a stat plus a get), not once per prior "
                        + "term: this fixture has no burned epochs, so it only ever "
                        + "probes the immediate predecessor before finding a real "
                        + "CONTINUE there")
                .isEqualTo(47L);
    }

    @Test
    void OFFSETSSURVIVEAFailoverEndToEnd() throws Exception {
        // ⚠️ THE CONTRACT `Sequencer.commit` STATES VERBATIM -- offsets "will
        // never be reassigned (I2, and NFR-11 across a failover)" -- and
        // acceptance criterion 4. Until M4.6e a successor read only its own
        // epoch's prefix and began the stream again at 0; measured on M4.6c,
        // leader 1 acked 100 records at 0..99 and leader 2 returned firstOffset
        // 0 for the same stream.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = start(store, "pod1");
        first.commit(request("pod1", 1, "seg/a", 6));
        first.commit(request("pod1", 2, "seg/b", 4));
        first.close();

        LocalSequencer second = start(store, "pod2");
        var resumed = second.commit(request("pod2", 1, "seg/c", 3));

        assertThat(resumed.runs().getFirst().firstOffset())
                .as("the successor RESUMES the stream at 10, it does not restart it")
                .isEqualTo(10);
        assertThat(resumed.sequence())
                .as("in its OWN chain, at slot 1 behind its CONTINUE -- offsets "
                        + "cross the boundary, slot numbers do not")
                .isEqualTo(1);
    }
}
