// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
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
 *
 * ⚠️ AND {@code DeadPeerTest} WAS CITED HERE AS A CONTRAST IT CANNOT CARRY. Its javadoc USED to say it was "split out of {@code
 * FleetSequencerTest} when it crossed 700 lines"; M5.54 removed that trigger,
 * so this is a quotation of retracted text. ⚠️ AND M5.54's OWN FIRST ANSWER WAS
 * WRONG TOO: it read `DeadPeerTest`'s 185 insertions with ZERO deletions, and
 * `FleetSequencerTest`'s maximum of 657, as DISPROOF. They are not. The cap
 * gate makes an over-cap committed version unrepresentable, so a file can
 * exceed it only in a working tree -- which is where a split happens and which
 * git cannot see. The claim was UNDECIDABLE, not false.
 * (The COMMIT is seven files and 381 insertions; an earlier draft here quoted
 * the file's stat as the commit's.) M5.54 removed that trigger; what was
 * wrong here was repeating it as established fact.
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
    /** A SECOND endpoint for the same leader, so a lease can move without a successor. */
    private static final String C = "poda-alt:9000";
    /** ⚠️ ONE key for the whole fleet: `leaseKey()` derives from the PREFIX alone,
     * never from a pod id or an endpoint -- the invariant this fixture rests on. */
    private static final String LEASE_KEY = config("poda", A).leaseKey();

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport transport) throws Exception {
        return pod(store, podId, endpoint, transport, transport);
    }

    /**
     * A pod that SENDS through {@code wire} but is REGISTERED on {@code registry}.
     *
     * <p>⚠️ THE TWO ARE THE SAME OBJECT FOR EVERY CALLER BUT ONE. Splitting them
     * lets a follower's sends pass through a decorator while the leader stays
     * reachable on the plain transport, which is what
     * {@link #forwardedCommitCostWhenTheLeaseMoves(int)} needs and what keeps a
     * successor -- and its seal and replay -- out of the counted window.
     */
    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport registry, SequencerTransport wire) throws Exception {
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
                wire, () -> LocalSequencer.start(store, PREFIX, manager, 8,
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
            registry.at(endpoint, fleet);
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

    @Test
    void aForwardedCommitWhoseLEASEMOVEDCostsONEMoreREADAndNothingElse() throws Exception {
        // ⚠️ THE SPEC's SECOND COST NUMBER, WHICH WAS STATED AS FACT AND PINNED
        // NOWHERE: "it is FOUR when the lease has moved, because `commitAll`'s
        // `NotTheLeaseholderException` arm re-reads it". M5.33 MEASURED two
        // extra lease GETs on that arm surviving the whole sequencer module.
        //
        // ⚠️ THE OBVIOUS FIXTURE IS THE WRONG ONE, and the row warned so.
        // `RemoteSequencerTest.aLeaseThatMovesDURINGTheCommitIsFollowedAfterOne`
        // `Refusal` builds this race by CLOSING the leader and STARTING a
        // successor inside the send, so a count around it is dominated by seal
        // and replay -- `(puts 5, gets 8, lists 3, stats 6, deletes 0)`, a LIST
        // on what is meant to be a commit-path pin.
        //
        // ⚠️ SO THE LEASE MOVES WITHOUT A SUCCESSOR, and the same leader is
        // reachable at two endpoints. `RemoteSequencer` compares EPOCH and
        // ENDPOINT, so either changing is a move to a forwarder -- and the two
        // arms below use both shapes deliberately: the FIRST move bumps the
        // epoch AND changes the endpoint A -> C, the SECOND bumps the epoch
        // with the endpoint already at C. ⚠️ THAT SECOND SHAPE CARRIES A
        // PROPERTY -- it is the only place the EPOCH half of that condition is
        // exercised alone, which is why its `as(...)` says so rather than
        // leaving the coverage to be discovered (M5.70). ⚠️ AND WHAT KEEPS THE WINDOW A COMMIT IS NOT THAT FIELD: an
        // earlier comment here credited "keeping `holderPodId`", and review
        // MEASURED that false -- rewriting the pod id, or bumping the epoch,
        // both leave the record at (1, 3, 0, 1, 0). What holds it steady is the
        // PARKED RENEWER and the FIXED CLOCK, which `pod()`'s own comment above
        // already explains. ⚠️ NO CLAIM ABOUT WHICH CALLERS REACH `isOwnTerm`
        // APPEARS HERE, deliberately: two drafts of this comment made one and
        // both were wrong, the second omitting `acquireLocked` -- the election,
        // which is the path this fixture runs inside the counted window.
        // ⚠️ AND THE REWRITE ITSELF IS DELIBERATELY UNCOUNTED: it goes through
        // the backing store, because moving a lease is the environment, not
        // what this commit path costs.
        // ⚠️ TWO STREAM COUNTS, FOR THE REASON THE SIBLING ABOVE ALREADY GIVES
        // AND THIS CASE FIRST IGNORED. At ONE RunKey "one re-read per commit"
        // and "one re-read per STREAM" are the same number, so a whole-record
        // equality cannot separate them: review MEASURED a per-stream loop on
        // the refusal arm leaving all 504 green. A flush of 200 streams would
        // then pay 200 lease GETs whenever it straddles a lease move -- a read
        // scaling with streams, which non-negotiable 6 forbids outright.
        assertThat(forwardedCommitCostWhenTheLeaseMoves(1).delta())
                .as("one forwarded commit whose lease moved: the SAME record as an "
                        + "unmoved one plus exactly ONE more GET -- the re-read on the "
                        + "refusal arm. Still no LIST, and still one PUT")
                .isEqualTo(new StoreCounts(1, 3, 0, 1, 0));
        assertThat(forwardedCommitCostWhenTheLeaseMoves(4).delta())
                .as("and the SAME record at four streams -- the re-read is per COMMIT, "
                        + "never per stream")
                .isEqualTo(new StoreCounts(1, 3, 0, 1, 0));
    }

    /** While armed, refuses one send and moves the lease as it does. */
    private static final class MovesTheLeaseWhenArmed implements SequencerTransport {
        private final InProcessTransport inner;
        private final BinStore uncounted;
        private final String to;
        private boolean armed;

        MovesTheLeaseWhenArmed(InProcessTransport inner, BinStore uncounted, String to) {
            this.inner = inner;
            this.uncounted = uncounted;
            this.to = to;
        }

        void arm() {
            this.armed = true;
        }

        @Override
        public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
            if (armed) {
                armed = false;
                io.github.huyz0.os.biningester.format.Lease held;
                try (java.io.InputStream in =
                        uncounted.get(LEASE_KEY)) {
                    held = io.github.huyz0.os.biningester.format.Lease.decode(in.readAllBytes());
                }
                // ⚠️ THE EPOCH IS BUMPED, because no in-protocol takeover keeps it:
                // `Lease.takenOverBy` is `new Lease(epoch + 1, ...)` and is the
                // only takeover constructor `LeaseManager` reaches. An earlier
                // fixture kept it, and review MEASURED what that hid -- M5.33's
                // own two-extra-GET mutation, GUARDED by `if (moved.epoch() !=
                // lease.epoch())`, survived both modules. That guard is the left
                // half of the condition three lines above it in `commitAll`, so
                // a same-epoch move pins the refusal arm for a move that never
                // happens. ⚠️ `isOwnTerm` compares epoch and podId and never the
                // endpoint, and nothing re-reads the leader's belief inside the
                // window, so the leader is not fenced either way. The rewrite
                // keeps `expiresAtMillis`, so the fixed clock still reads the
                // lease unexpired and the election's own cost does not move.
                io.github.huyz0.os.biningester.format.Lease moved = new io.github.huyz0.os.biningester.format.Lease(held.epoch() + 1,
                        held.holderPodId(), to, held.expiresAtMillis());
                byte[] bytes = moved.encode();
                uncounted.put(LEASE_KEY,
                        new io.github.huyz0.os.biningester.binstore.Body(bytes.length,
                                () -> new java.io.ByteArrayInputStream(bytes)));
                throw new SequencerTransport.NotTheLeaseholderException("the term moved");
            }
            return inner.send(endpoint, request);
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }
    }

    /** What a follower's commit of {@code streams} runs costs when the lease moves. */
    private static Cost forwardedCommitCostWhenTheLeaseMoves(int streams) throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        var store = new CountingBinStore(backing);
        InProcessTransport inner = new InProcessTransport();
        MovesTheLeaseWhenArmed moving = new MovesTheLeaseWhenArmed(inner, backing, C);
        try (FleetSequencer leader = pod(store, "poda", A, inner);
                FleetSequencer follower = pod(store, "podb", B, inner, moving)) {
            assertThat(leader.leading()).isTrue();
            assertThat(follower.leading()).isFalse();
            // ⚠️ THE SAME LEADER AT A SECOND ENDPOINT, which is what makes the
            // follow land without a successor to start.
            inner.at(C, leader);
            leader.commit(flush("poda", 0, "seg/a"));
            Map<RunKey, Integer> runs = new LinkedHashMap<>();
            for (int i = 0; i < streams; i++) {
                runs.put(new RunKey(DedupFixtures.A, i), 2);
            }
            // ⚠️ THE PREMISE, ASSERTED, for the reason the sibling records: a
            // one-character degradation to a single RunKey touches no assertion
            // and `check-test-integrity` cannot see it.
            assertThat(runs).hasSize(streams);
            follower.commit(new CommitRequest("podb", "i1", 0, "seg/b", runs));

            moving.arm();
            var before = store.counts();
            follower.commit(new CommitRequest("podb", "i1", 1, "seg/b2", runs));
            var after = store.counts();
            // ⚠️ THAT THE FOLLOW WENT TO THE MOVED ENDPOINT, not merely that it
            // was resent. Both endpoints serve the same leader object, so the
            // COUNTS cannot tell them apart -- review measured sending to
            // `lease.holderEndpoint()` instead of `moved.holderEndpoint()`
            // giving an identical record.
            assertThat(inner.sentTo()).as("the retry went to the endpoint the RE-READ "
                    + "lease named").endsWith(C);

            // ⚠️ AND THE EXTRA READ IS FOR THE STRADDLING COMMIT ALONE, not a
            // surcharge a fleet pays forever once it has seen a failover. The
            // window above ends AT the move, so review MEASURED a permanent +1
            // surviving both modules: a `sawMove` flag set on the refusal arm
            // and re-read at the top of every later `commitAll` is invisible to
            // a meter that stops at the commit which straddles. That would make
            // every follower commit cost four reads after the first failover in
            // a fleet's life, while the SPEC still prices the unmoved path at
            // three.
            // ⚠️ TWO COMMITS PAST THE MOVE, NOT ONE, AND THEN A SECOND MOVE.
            // Both reviewers measured what one of each missed: a surcharge
            // PHASE-SHIFTED by a commit (`since++ % 2 == 0`) steps over a single
            // sample, and a re-read loop bounded by the number of moves this pod
            // has FOLLOWED is indistinguishable from the one legitimate re-read
            // while the fixture only ever moves once.
            assertThat(costOf(store, follower, 2, runs))
                    .as("the commit AFTER the move costs the UNMOVED record again -- the "
                            + "re-read is per refusal, never a permanent surcharge")
                    .isEqualTo(new StoreCounts(1, 2, 0, 1, 0));
            assertThat(costOf(store, follower, 3, runs))
                    .as("and so does the one after THAT -- a surcharge that skips a commit "
                            + "steps over a single sample")
                    .isEqualTo(new StoreCounts(1, 2, 0, 1, 0));
            moving.arm();
            assertThat(costOf(store, follower, 4, runs))
                    .as("a SECOND move costs one more read and no more -- a re-read loop "
                            + "counting the moves seen would cost two. ⚠️ AND THIS ARM "
                            + "CARRIES A SECOND PROPERTY, so do not trim it for cost "
                            + "reasons: the epoch bumps with the endpoint ALREADY at C, "
                            + "so it is the only place the EPOCH half of `commitAll`'s "
                            + "\"did the lease move\" test is exercised alone. Dropping "
                            + "that half, or turning its `&&` into `||`, reds HERE and "
                            + "nowhere else (M5.70)")
                    .isEqualTo(new StoreCounts(1, 3, 0, 1, 0));
            return new Cost(new StoreCounts(after.puts() - before.puts(),
                    after.gets() - before.gets(), after.lists() - before.lists(),
                    after.stats() - before.stats(), after.deletes() - before.deletes()));
        }
    }

    /** What ONE more follower commit costs, as a whole {@link StoreCounts} delta. */
    private static StoreCounts costOf(CountingBinStore store, FleetSequencer follower,
            long flushSeq, Map<RunKey, Integer> runs) throws Exception {
        var before = store.counts();
        follower.commit(new CommitRequest("podb", "i1", flushSeq, "seg/post" + flushSeq, runs));
        var after = store.counts();
        return new StoreCounts(after.puts() - before.puts(), after.gets() - before.gets(),
                after.lists() - before.lists(), after.stats() - before.stats(),
                after.deletes() - before.deletes());
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
