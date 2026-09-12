// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What the forwarding path COSTS, in object-store requests (M5.33, NFR-1).
 *
 * ⚠️ PLACED HERE RATHER THAN MOVED HERE, and the distinction is not
 * pedantry: adding this test to {@code FleetSequencerTest} would have taken
 * that file past code-structure.md rule 1's 700-line limit, so it was
 * written into its own file instead. (An earlier draft said 723; the
 * merged file is longer than that now, and the number was never the
 * point.)
 * NOTHING WAS MOVED OUT and that file is untouched at 657 --
 * ⚠️ AND {@code DeadPeerTest} IS NOT THE CONTRAST AN EARLIER DRAFT HERE
 * CLAIMED. Its javadoc says it was "split out of {@code FleetSequencerTest}
 * when it crossed 700 lines", and review MEASURED `DeadPeerTest.java` in that
 * commit at 185 insertions and ZERO deletions, with `FleetSequencerTest.java`
 * absent from the commit's numstat entirely -- written new, moving nothing,
 * from a file that has been 657 lines since `11fe5d8` and never crossed 700.
 * (The COMMIT is seven files and 381 insertions; an earlier draft here quoted
 * the file's stat as the commit's.) That claim is pre-existing and
 * not this commit's to fix; repeating it as established fact was.
 * code-structure.md rule 1 is the rule either way: split rather than raise. The
 * seam is real rather than convenient: every other file here asks whether
 * forwarding is CORRECT, and this one asks what it costs. cost.md draws the
 * same line, and non-negotiable 6 is a claim about counts rather than about
 * outcomes.
 *
 * ⚠️ THE PUT IS COUNTED HERE, NOT LEFT NEXT DOOR.
 * {@code FleetSequencerTest}'s {@code aFollowerDoesNotWriteTheCHAINItself}
 * stays -- it asserts WHO writes the chain, a correctness claim that happens to
 * be counted -- but it runs at ONE stream, so it cannot see a write rate that
 * scales. This file counts the PUT at four streams as well, which is the same
 * question it asks of the reads.
 */
class FleetSequencerCostTest {

    private static final String A = "poda:9000";
    private static final String B = "podb:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport transport) throws Exception {
        // ⚠️ A FIXED CLOCK, and parking the tickers was NOT enough on its own.
        // Review MEASURED that with the renewer parked the lease (TTL 10 s) is
        // never renewed, so a 12 s stall ANYWHERE BEFORE the window lets it
        // lapse and the counted commit becomes a takeover: puts=4, gets=7,
        // lists=3, stats=6. The parking widened the fuse from 3 s to 10 s and
        // widened the exposure from the window to the whole test body. A clock
        // that does not advance removes both.
        LeaseManager manager = new LeaseManager(store, config(podId, endpoint),
                Clock.fixed(java.time.Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));
        FleetSequencer fleet = new FleetSequencer(store, config(podId, endpoint),
                transport, () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        // ⚠️ THE RENEWER IS PARKED, because this file counts
                        // requests in a window and `leases.renew()` is a
                        // `putIfMatch` the same meter sees. The shipped 3 s
                        // interval against a real clock means a stalled machine
                        // turns four exact pins into a flake: review MEASURED a
                        // 4 s stall inside the window giving puts=2.
                        LocalSequencer.sleepFor(java.time.Duration.ofDays(1)),
                        LocalSequencer.CHECKPOINT_EVERY_DELTAS,
                        // ⚠️ AND THE CHECKPOINT TICKER, for the same reason one
                        // fuse longer: it fires at 60 s and the priming commit
                        // has already set `dirty`, so a stalled machine gives
                        // puts=3. Review measured the renewer at 3 s; this is
                        // the same class, 20x wider.
                        CheckpointWriter.sleepFor(java.time.Duration.ofDays(1))));
        if (fleet.leading()) {
            transport.at(endpoint, fleet);
        }
        return fleet;
    }

    private static CommitRequest flush(String pod, long seq, String segment) {
        return new CommitRequest(pod, "i1", seq, segment, counts(2));
    }

    /**
     * A forwarded commit costs THREE reads, and the same three at any number of
     * streams (M5.33, NFR-1).
     *
     * <p>⚠️ MEASURED, AND THE SPEC'S COST TABLE SAID ZERO. As landed a
     * follower's commit makes three store requests: {@code RemoteSequencer}
     * reads the lease (one GET), and {@code FleetSequencer} attempts an
     * election (a STAT and a GET), plus the leaseholder's one delta PUT.
     *
     * <p>⚠️ THE PUT IS COUNTED HERE TOO, and an earlier version left it to
     * {@code FleetSequencerTest.aFollowerDoesNotWriteTheCHAINItself} on the
     * grounds that it was already pinned. It is -- at ONE stream, because
     * {@code DedupFixtures.counts} builds a single {@code RunKey}. Review
     * MEASURED a per-stream PUT sidecar in {@code RemoteSequencer.commitAll}
     * surviving all 497 tests, which is this task's own one-stream blindness
     * standing on the write side of the same commit.
     *
     * <p>⚠️ THE FOUR-STREAM COUNT IS WHAT CARRIES IT; the one-stream count is
     * the baseline the "same at any number" claim is read against. Review MEASURED
     * two mutations surviving the module: issuing three {@code currentLease()}
     * calls per forwarded commit (482/482 green), and scaling the lease read
     * per {@code RunKey} (587 green). A ONE-STREAM test is green under BOTH,
     * because at one {@code RunKey} "three per commit" and "three per stream"
     * are the same number. Asserting the SAME three at 1 and at 4 separates
     * them: per-stream scaling of the lease read gives 3 and 6.
     *
     * <p>⚠️ SIX, NOT TWELVE. An earlier draft reasoned "four streams times
     * three requests" instead of measuring, in a task whose whole subject is
     * that the number be measured. Only the LEASE read scales under that
     * mutation; the election's stat and get do not.
     *
     * <p>⚠️ NON-NEGOTIABLE 6 IS THE REASON, not tidiness: a read that scales
     * with streams is a read that scales with shards and partitions, which is
     * the one line cost.md calls the architecture.
     *
     * <p>⚠️ AND THE COUNT IS THE AS-LANDED ONE. **M5.6g** takes it from three
     * to one by reusing the lease the forwarder already read. When it lands,
     * the two expected {@code StoreCounts} are the ONLY
     * self-enforcing sites -- they red on their own. Everything else is prose
     * that must be swept: this method's name, the {@code as(...)} strings, the
     * GET/STAT/GET breakdown above, the SPEC's cost row and the backlog cell.
     * ⚠️ STATED AS A RULE RATHER THAN A LIST, because this hand-off was
     * enumerated three times in three rounds, three different ways, and no list
     * was complete. ADR-0039's saving is 3 -> 1, not 2 -> 0.
     */
    @Test
    void aForwardedCommitCostsTHREEReadsAndONEWriteAtANYNumberOfStreams() throws Exception {
        Cost atOne = forwardedCommitCost(1);
        Cost atFour = forwardedCommitCost(4);

        // ⚠️ THE WHOLE DELTA, so a request kind nobody has thought of is
        // counted by construction rather than by anyone remembering to add it.
        // `StoreCounts` is (puts, gets, lists, stats, deletes).
        assertThat(atOne.delta())
                .as("one forwarded commit: the lease GET, the election's STAT and GET, the "
                        + "leaseholder's one delta PUT -- and no LIST, ever, on a commit path")
                .isEqualTo(new StoreCounts(1, 2, 0, 1, 0));
        assertThat(atFour.delta())
                .as("and the SAME record at four streams -- every kind flat, not just the two "
                        + "this task happened to name first")
                .isEqualTo(new StoreCounts(1, 2, 0, 1, 0));
    }

    /**
     * Convenience over a whole {@link StoreCounts} delta.
     *
     * <p>⚠️ THE ASSERTION IS ON THE WHOLE RECORD. Round 1 of
     * this task pinned reads and left the PUT out, and review MEASURED a
     * per-stream PUT sidecar surviving. Round 2 added the PUT by name, and
     * review then MEASURED a per-stream LIST surviving the same way -- a LIST
     * per stream per flush, which cost.md R2 and R15 forbid outright. Naming
     * one verb at a time loses to whoever adds the next verb, so the test
     * compares the entire delta and these accessors exist only to make the
     * failure message readable.
     *
     * ⚠️ AND THE ACCESSORS ARE GONE. An earlier version kept
     * {@code reads()} and {@code puts()} assertions beside the record ones;
     * review MEASURED them DEAD BY CONSTRUCTION -- the record equality implies
     * both, and AssertJ stops at the first failure, so no mutation can reach
     * them. Worse, deleting only the record lines and keeping the accessors
     * left a per-stream LIST surviving 497 green: all the falsifying power was
     * in the records and none in the four lines that looked like the pin.
     */
    private record Cost(StoreCounts delta) {
    }

    /** What a follower's commit of {@code streams} runs costs. */
    private static Cost forwardedCommitCost(int streams) throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        var store = new CountingBinStore(backing);
        InProcessTransport transport = new InProcessTransport();
        try (FleetSequencer leader = pod(store, "poda", A, transport);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            // ⚠️ THE ROLES ARE FIXED AT CONSTRUCTION, not by this commit.
            // `Leadership` elects inside the constructor, so poda leads and
            // podb follows before either commits -- asserted rather than
            // assumed, because an earlier comment here said the priming commit
            // was what made podb a follower, and review MEASURED that replacing
            // it with these two assertions leaves the count at three.
            assertThat(leader.leading()).isTrue();
            assertThat(follower.leading()).isFalse();
            leader.commit(flush("poda", 0, "seg/a"));
            Map<RunKey, Integer> runs = new LinkedHashMap<>();
            for (int i = 0; i < streams; i++) {
                runs.put(new RunKey(DedupFixtures.A, i), 2);
            }

            // ⚠️ THE PREMISE, ASSERTED. Review MEASURED that degrading the
            // fixture to one `RunKey` (`new RunKey(A, 0)` for every i) plus the
            // per-stream lease read leaves 497 green -- both arms agree
            // trivially because they are compared to the same literal, and
            // `check-test-integrity` cannot see a one-character fixture change
            // that touches no assertion.
            assertThat(runs).hasSize(streams);

            // ⚠️ THE SECOND COMMIT IS MEASURED, because the FIRST is not where
            // a per-flush claim can fail. Review MEASURED
            // `if (commits++ > 0) { currentLease(); currentLease(); }` in
            // `commitAll` leaving 497 green while every follower commit after
            // the first cost 5 reads -- the SPEC says "per-flush and PINNED"
            // and the per-flush axis was the unpinned one. The steady state is
            // flat: commits 1, 2 and 3 all measure (1, 2, 0, 1, 0).
            follower.commit(new CommitRequest("podb", "i1", 0, "seg/b", runs));

            var before = store.counts();
            follower.commit(new CommitRequest("podb", "i1", 1, "seg/b2", runs));
            var after = store.counts();
            return new Cost(new StoreCounts(after.puts() - before.puts(),
                    after.gets() - before.gets(), after.lists() - before.lists(),
                    after.stats() - before.stats(), after.deletes() - before.deletes()));
        }
    }
}
