// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.firstOffsetOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * A pod leads when it can and forwards when it cannot (M5.6).
 *
 * <p>⚠️ IT DOES NOT YET CLOSE THE CORRECTNESS HOLE, and an earlier draft of
 * this sentence claimed it did. {@code RemoteSequencer} made forwarding
 * possible and nothing chose to use it; this class chooses — but nothing in
 * production constructs THIS either, so a real non-leaseholder pod still cannot
 * commit. M5.6b proves the commit path across two pods; SHUTTING the hole needs
 * a production {@code SequencerTransport} (M5.6e) and a production {@code
 * main()}, which no row owns.
 */
class FleetSequencerTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";
    private static final String C = "pod-c:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static LeaseManager leases(BinStore store, String podId, String endpoint) {
        return new LeaseManager(store, config(podId, endpoint), Clock.systemUTC());
    }

    /** A pod: its own lease manager, its own election, forwarding through one transport. */
    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport transport) throws Exception {
        LeaseManager manager = leases(store, podId, endpoint);
        FleetSequencer fleet = new FleetSequencer(store, config(podId, endpoint),
                transport, () -> LocalSequencer.start(store, PREFIX, manager, 8));
        if (fleet.leading()) {
            // ⚠️ A LEADER MUST BE REACHABLE AT ITS OWN ENDPOINT, or the pods
            // forwarding to it have nowhere to send.
            transport.at(endpoint, fleet);
        }
        return fleet;
    }

    private static CommitRequest flush(String pod, long seq, String segment) {
        return new CommitRequest(pod, "i1", seq, segment, counts(2));
    }
    /** A clock the test advances, so a lease can lapse without wall-clock time. */
    private static final class MovableClock extends Clock {
        private volatile long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public java.time.Instant instant() {
            return java.time.Instant.ofEpochMilli(millis);
        }

        @Override public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    /** A sequencer that answers every commit with {@code failure}, and counts them. */
    private static final class Refusing implements Sequencer {
        private final IOException failure;
        private final IOException closeFailure;
        private int commits;
        /**
         * ⚠️ HOOKS, so the two windows the `closed` flag exists for are
         * drivable on ONE thread. Both are real interleavings -- a SIGTERM
         * lands while a flush is inside {@code commitAll}, and a commit arrives
         * while the lease is being handed back -- and a test that spawned
         * threads for them would assert a schedule rather than a rule.
         */
        private Runnable duringCommit;
        private Runnable duringClose;

        Refusing(IOException failure) {
            this(failure, null);
        }

        Refusing(IOException failure, IOException closeFailure) {
            this.failure = failure;
            this.closeFailure = closeFailure;
        }

        Refusing duringCommit(Runnable action) {
            this.duringCommit = action;
            return this;
        }

        Refusing duringClose(Runnable action) {
            this.duringClose = action;
            return this;
        }

        @Override public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            commits++;
            if (duringCommit != null) {
                duringCommit.run();
            }
            throw failure;
        }

        @Override public void close() throws IOException {
            if (duringClose != null) {
                duringClose.run();
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    /**
     * ⚠️ A CLOSED POD MUST NOT COMMIT, and until now it did -- by FORWARDING.
     * {@code Leadership.sequencer()} refuses to elect after close, so
     * {@code leadership.sequencer()} returns null and the commit fell straight
     * through to {@code remote.commitAll}, which reads the lease and sends. A
     * pod that has released its lease and told its caller it is shutting down
     * then goes on committing through a peer, for as long as anything holds a
     * reference -- an in-flight flush, a queued batch.
     */
    @Test
    void aCommitAfterCLOSEIsREFUSED_NotFORWARDED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        try {
            FleetSequencer mine = pod(store, "poda", A, transport);
            mine.close();
            // ⚠️ RE-REGISTERED AFTER THE CLOSE, and an earlier draft did not --
            // its comment claimed a reachable holder made a forwarded commit
            // SUCCEED, while `close` had already cleared the shared transport
            // through `remote.close()`. Under the deleted-check mutation the
            // commit then failed with "nowhere to send": the right verdict for
            // the wrong reason, which is the shape this fixture exists to rule
            // out.
            transport.at(C, holder);

            assertThatThrownBy(() -> mine.commit(flush("poda", 0, "seg/after-close")))
                    .as("a released pod does not commit, through its own chain or anyone's")
                    .isInstanceOf(IOException.class)
                    // ⚠️ AND NOT A FENCING. `Sequencer`'s contract defines
                    // `instanceof FencedException` as "safe to re-send
                    // ELSEWHERE", and this refusal says nothing about who holds
                    // the lease -- only that THIS pod stopped. Review MEASURED
                    // the mutation: throwing a FencedException with the same
                    // message left the whole suite green.
                    .isNotInstanceOf(FencedException.class)
                    // ⚠️ THE ENTRY GUARD'S OWN WORDS. Both refusals say
                    // "closed", so a `hasMessageContaining("closed")` is
                    // satisfied by the OTHER one -- review MEASURED that
                    // deleting this guard left the suite green, because after
                    // close the fall-through reaches the in-flight check and
                    // says "closed" too. The suite then constrained their union
                    // and neither individually.
                    .hasMessageContaining("must not commit again");

            assertThat(transport.sentTo())
                    .as("and it did not reach the transport at all")
                    .isEmpty();
        } finally {
            holder.close();
        }
    }

    /**
     * ⚠️ BOTH CLOSES RUN, AND NEITHER FAILURE IS ERASED. `close` put
     * {@code remote.close()} in a `finally`, so when the lease release threw and
     * the transport threw too, the transport's exception REPLACED the release's
     * -- and the release is the one that says the lease was not handed back,
     * which is the difference between a successor taking over in milliseconds
     * and waiting out the TTL.
     */
    /**
     * ⚠️ A CLOSE THAT FAILED IS STILL A CLOSE. A pod whose lease release threw
     * does not go on committing through a peer — the one path where `close`
     * does not return normally.
     *
     * <p>⚠️ IT PINS ONE MUTATION, NOT TWO, and an earlier version of this
     * sentence claimed both. Latching {@code closed} only after the
     * {@code throw} REDs this test; latching it after the two closes does not,
     * because by then the close has returned. That second ordering is what
     * {@code aCommitARRIVINGWhileTheLeaseIsBeingHandedBackIsREFUSED} exists for.
     * A "MEASURED" line that does not reproduce is worse than none.
     */
    @Test
    void aCloseThatFAILEDStillREFUSESLaterCommits() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        try {
            IOException releaseFailed = new IOException("injected: the lease was not handed back");
            Refusing held = new Refusing(new IOException("unused"), releaseFailed);
            FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                    transport, () -> Optional.of(held));

            assertThatThrownBy(mine::close).isSameAs(releaseFailed);
            transport.at(C, holder);

            assertThatThrownBy(() -> mine.commit(flush("poda", 0, "seg/after-failed-close")))
                    .as("the pod is closed whether or not the goodbye landed")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed");
            assertThat(transport.sentTo())
                    .as("and it forwarded nothing")
                    .isEmpty();
        } finally {
            holder.close();
        }
    }

    /**
     * ⚠️ THE IN-FLIGHT FLUSH, which is the caller the entry guard cannot help.
     * A commit enters, the local sequencer is fenced, and the pod is closed
     * before the fallback forwards -- so the entry check passed and the send is
     * still ahead. Review MEASURED the gap: deleting the pre-forward re-check
     * left the whole build green, because no test closed a pod mid-commit.
     *
     * <p>⚠️ ONE THREAD, and deliberately. The close happens inside the local
     * sequencer's own {@code commitAll}, which is a real interleaving -- a
     * SIGTERM lands while a flush is in the store -- and asserting it without
     * threads asserts the RULE rather than a schedule.
     */
    @Test
    void aPodCLOSEDWhileACommitIsINFLIGHTDoesNotForwardIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        try {
            java.util.concurrent.atomic.AtomicReference<FleetSequencer> self =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Refusing fenced = new Refusing(new FencedException("injected: this term ended"));
            FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                    transport, () -> Optional.of(fenced));
            self.set(mine);
            // ⚠️ THE CLOSE LANDS AFTER THE ENTRY CHECK AND BEFORE THE FORWARD.
            fenced.duringCommit(() -> {
                try {
                    self.get().close();
                } catch (IOException neverHappens) {
                    throw new java.io.UncheckedIOException(neverHappens);
                }
            });
            transport.at(C, holder);

            assertThatThrownBy(() -> mine.commit(flush("poda", 0, "seg/in-flight")))
                    .as("a fenced commit is normally forwarded -- but not by a pod that has "
                            + "since been closed")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("in flight");
            assertThat(transport.sentTo())
                    .as("so nothing reached a peer")
                    .isEmpty();
        } finally {
            holder.close();
        }
    }

    /**
     * ⚠️ THE FLAG IS UP BEFORE EITHER HALF RUNS, which is what its comment
     * claims and what nothing pinned: a commit arriving while the lease is
     * still being handed back must already be refused. Review MEASURED the
     * mutation -- moving {@code closed = true} below the two closes -- and it
     * survived, because every other test closes and then commits.
     */
    @Test
    void aCommitARRIVINGWhileTheLeaseIsBeingHandedBackIsREFUSED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        try {
            java.util.concurrent.atomic.AtomicReference<FleetSequencer> self =
                    new java.util.concurrent.atomic.AtomicReference<>();
            List<Throwable> refused = new java.util.ArrayList<>();
            Refusing held = new Refusing(new IOException("unused"));
            FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                    transport, () -> Optional.of(held));
            self.set(mine);
            held.duringClose(() -> {
                try {
                    self.get().commit(flush("poda", 0, "seg/mid-close"));
                    refused.add(null);
                } catch (Throwable why) {
                    refused.add(why);
                }
            });

            mine.close();

            assertThat(refused).hasSize(1);
            assertThat(refused.get(0))
                    .as("the flag is raised before the lease goes back, not after")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed");
        } finally {
            holder.close();
        }
    }

    /**
     * ⚠️ A TRANSPORT FAILURE IS NOT SWALLOWED when the release succeeded.
     * Review MEASURED the gap: a close that lost the transport's failure and
     * reported success stayed green, because the only test driving a transport
     * failure also failed the release, which is the other branch.
     */
    @Test
    void closeREPORTSATransportFailureWhenTheLeaseWentBackCleanly() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        IOException transportFailed = new IOException("injected: the transport did not close");
        SequencerTransport transport = new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
                throw new NotTheLeaseholderException("unused");
            }

            @Override
            public void close() throws IOException {
                throw transportFailed;
            }
        };
        FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                transport, () -> Optional.of(new Refusing(new IOException("unused"))));

        assertThatThrownBy(mine::close)
                .as("a clean lease release does not hide a transport that would not close")
                .isSameAs(transportFailed);
    }

    @Test
    void closeRUNSBothHalvesAndREPORTSBothFailures() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        IOException releaseFailed = new IOException("injected: the lease was not handed back");
        IOException transportFailed = new IOException("injected: the transport did not close");
        Refusing held = new Refusing(new IOException("unused"), releaseFailed);
        SequencerTransport transport = new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
                throw new NotTheLeaseholderException("unused");
            }

            @Override
            public void close() throws IOException {
                throw transportFailed;
            }
        };
        FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                transport, () -> Optional.of(held));

        assertThatThrownBy(mine::close)
                .as("the lease release is the failure a caller must see FIRST")
                .isSameAs(releaseFailed);
        assertThat(releaseFailed.getSuppressed())
                .as("and the transport's failure is attached, never substituted -- deleting "
                        + "`remote.close()` and swapping the two both fail here")
                .containsExactly(transportFailed);
    }

    @Test
    void theFIRSTPodLeadsAndTheSecondFORWARDSToIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        try (FleetSequencer first = pod(store, "poda", A, transport);
                FleetSequencer second = pod(store, "podb", B, transport)) {

            assertThat(first.leading()).as("the first pod took the term").isTrue();
            assertThat(second.leading()).as("the second did not, and that is normal")
                    .isFalse();

            assertThat(firstOffsetOf(first.commit(flush("poda", 0, "seg/a")), "seg/a"))
                    .isZero();
            assertThat(firstOffsetOf(second.commit(flush("podb", 0, "seg/b")), "seg/b"))
                    .as("the follower's records land in the SAME total order, through "
                            + "the leader -- which is what a multi-pod deployment needs "
                            + "and what M4 could not do")
                    .isEqualTo(2L);
        }
        assertThat(deltasCarrying(store, "seg/b"))
                .as("one delta, written by the leaseholder").isEqualTo(1);
    }

    @Test
    void aFOLLOWERIsPromotedWhenTheLeaderGoesAway() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        FleetSequencer first = pod(store, "poda", A, transport);
        try (FleetSequencer second = pod(store, "podb", B, transport)) {
            assertThat(second.leading()).isFalse();
            first.commit(flush("poda", 0, "seg/a"));

            // The leader goes away and releases its term.
            first.close();
            transport.gone(A);

            // ⚠️ PROMOTION HAPPENS ON A COMMIT, not on a timer. A pod that lost
            // the election at boot must be able to lead later, or a leader's
            // death is a permanent outage for every follower. Choosing once at
            // startup would make that unreachable.
            assertThat(firstOffsetOf(second.commit(flush("podb", 0, "seg/b")), "seg/b"))
                    .as("the follower took the vacant term and kept the total order")
                    .isEqualTo(2L);
            assertThat(second.leading()).as("and it is now the leader").isTrue();
        }
    }

    @Test
    void aLeaderThatIsFENCEDFallsBackToForwardingRatherThanFailing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        // ⚠️ ONE CLOCK FOR BOTH PODS, advanced by hand: a lease that never
        // lapses cannot be taken, so a fixed clock would assert nothing.
        MovableClock clock = new MovableClock();
        LeaseManager mine = new LeaseManager(store, config("poda", A), clock);
        FleetSequencer first = new FleetSequencer(store, config("poda", A),
                transport, () -> LocalSequencer.start(store, PREFIX, mine, 8));
        transport.at(A, first);
        try {
            assertThat(first.leading()).isTrue();
            first.commit(flush("poda", 0, "seg/a"));

            // The term lapses and pod-c takes it -- a partition plus a TTL, the
            // way a leader is fenced without knowing.
            clock.millis += Duration.ofSeconds(11).toMillis();
            LeaseManager challenger = new LeaseManager(store, config("podc", C), clock);
            LocalSequencer usurper = LocalSequencer.start(store, PREFIX, challenger, 8)
                    .orElseThrow();
            transport.at(C, usurper);
            try {
                // ⚠️ THE FENCED COMMIT APPENDED NOTHING, so forwarding it is not
                // a duplicate. Failing here would drop a producer's write on
                // every takeover.
                assertThat(firstOffsetOf(first.commit(flush("poda", 1, "seg/b")), "seg/b"))
                        .as("the fenced leader's commit still lands, through the new holder")
                        .isEqualTo(2L);
            } finally {
                usurper.close();
            }
            assertThat(first.leading()).as("and it knows it no longer leads").isFalse();
        } finally {
            first.close();
        }
        assertThat(deltasCarrying(store, "seg/b"))
                .as("exactly once -- the fenced attempt wrote nothing to duplicate")
                .isEqualTo(1);
    }

    @Test
    void aFollowerDoesNotWriteTheCHAINItself() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        var store = new io.github.huyz0.os.biningester.binstore.CountingBinStore(backing);
        InProcessTransport transport = new InProcessTransport();
        try (FleetSequencer first = pod(store, "poda", A, transport);
                FleetSequencer second = pod(store, "podb", B, transport)) {
            first.commit(flush("poda", 0, "seg/a"));
            long before = store.counts().puts();
            second.commit(flush("podb", 0, "seg/b"));

            // ⚠️ THE INVARIANT A LEASE EXISTS FOR: exactly one pod writes the
            // chain, so the follower's commit costs one delta PUT.
            assertThat(store.counts().puts() - before)
                    .as("one PUT, made by the leaseholder on the follower's behalf")
                    .isEqualTo(1L);
        }
    }

    @Test
    void anElectionThatNeverWinsStillForwardsForever() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        try (FleetSequencer leader = pod(store, "poda", A, transport)) {
            // ⚠️ A POD THAT CAN NEVER LEAD IS STILL A USEFUL POD. A wedged
            // election -- a permissions problem, say -- must not stop it
            // forwarding, or every append it takes fails.
            Leadership.Election never = Optional::empty;
            try (FleetSequencer stuck = new FleetSequencer(store, config("podb", B),
                    transport, never)) {
                assertThat(stuck.leading()).isFalse();
                assertThat(firstOffsetOf(stuck.commit(flush("podb", 0, "seg/b")), "seg/b"))
                        .as("it forwards, and its records land")
                        .isZero();
            }
            assertThat(leader.leading()).isTrue();
        }
    }

    @Test
    void anAMBIGUOUSLocalFailureIsPROPAGATED_NeverForwarded() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        // A real holder at C, so a pod that DID forward would find somewhere to
        // send and would succeed -- the test would then pass for the wrong
        // reason if forwarding were merely unreachable.
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        // ⚠️ NEITHER "lease" NOR "fenced" IN THE TEXT, and that is the point: a
        // 503 on a delta PUT may have LANDED and lost its reply.
        Refusing ambiguous = new Refusing(new IOException("injected: the reply was lost"));
        try (FleetSequencer mine = new FleetSequencer(store, config("poda", A),
                transport, () -> Optional.of(ambiguous))) {

            // ⚠️ THE SAFETY ARM: only a FencedException says nothing was
            // written. Forwarding this one would commit records the failed PUT
            // may already have appended.
            assertThatThrownBy(() -> mine.commit(flush("poda", 0, "seg/b")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("the reply was lost");

            // ⚠️ AND IT STILL LEADS. A pod that dropped its sequencer on a
            // transient 503 would unpublish its endpoint and re-elect on the
            // next commit -- burning an epoch and paying a seal plus a full
            // recovery for a hiccup `renewForever` refuses to fail over on.
            assertThat(mine.leading()).as("an ambiguous failure is not a fencing").isTrue();
        } finally {
            holder.close();
        }
        assertThat(transport.sentTo())
                .as("nothing was forwarded, so nothing could be committed twice")
                .isEmpty();
        assertThat(deltasCarrying(store, "seg/b")).isZero();
        assertThat(ambiguous.commits).as("and it was attempted once, not retried").isEqualTo(1);
    }




    @Test
    void TWOCallersAreInsideCommitAllAtOnce() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        CyclicBarrier bothInside = new CyclicBarrier(2);
        // ⚠️ TWO CALLERS, BECAUSE THE LEADER MAY BE A BATCHER. The cost is
        // argued on `FleetSequencer`'s javadoc; only a second thread holds it,
        // and no cost gate sees this one.
        Sequencer rendezvous = new Sequencer() {
            @Override public CommitDelta commitAll(List<CommitRequest> requests)
                    throws IOException {
                try {
                    bothInside.await(10, TimeUnit.SECONDS);
                } catch (Exception onlyOne) {
                    throw new IOException("only ONE caller was ever inside commitAll", onlyOne);
                }
                throw new IOException("both callers were inside commitAll at once");
            }

            @Override public void close() {
            }
        };
        try (FleetSequencer fleet = new FleetSequencer(store, config("poda", A),
                transport, () -> Optional.of(rendezvous))) {
            List<CompletableFuture<Void>> both = List.of(
                    commitOn(fleet, "poda", 0), commitOn(fleet, "poda", 1));
            for (CompletableFuture<Void> one : both) {
                assertThatThrownBy(() -> one.get(30, TimeUnit.SECONDS))
                        .as("neither caller waited for the other outside the store")
                        .hasRootCauseMessage("both callers were inside commitAll at once");
            }
        }
    }

    /** A commit on its own thread, so two can be in flight together. */
    private static CompletableFuture<Void> commitOn(FleetSequencer fleet, String pod, long seq) {
        return CompletableFuture.runAsync(() -> {
            try {
                fleet.commit(flush(pod, seq, "seg/" + seq));
            } catch (IOException e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }

    @Test
    void theTRANSPORTIsClosedEvenWhenGivingTheTermBackFAILS() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, leases(store, "podc", C), 8).orElseThrow();
        transport.at(C, holder);
        // ⚠️ `LocalSequencer.close` RELEASES THE LEASE and that `putIfMatch`
        // can 503, so this is the ordinary case the try/finally exists for.
        Refusing releaseFails = new Refusing(
                new FencedException("unused"), new IOException("injected: the release PUT failed"));
        FleetSequencer mine = new FleetSequencer(store, config("poda", A), transport,
                () -> Optional.of(releaseFails));

        assertThatThrownBy(mine::close)
                .as("the caller LEARNS the term was not handed back")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("the release PUT failed");

        // ⚠️ AND THE TRANSPORT WENT ANYWAY -- in production a real peer client
        // and its pool, leaked once per pod that meets a hiccup shutting down.
        assertThatThrownBy(() -> transport.send(C, flush("poda", 0, "seg/x")))
                .as("the transport was closed despite the failure above")
                .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        holder.close();
    }

    @Test
    void anINVALIDArgumentIsRefusedBEFOREATermIsTaken() {
        AtomicBoolean elected = new AtomicBoolean();

        // ⚠️ THE ORDER IS THE WHOLE POINT. Electing first and validating second
        // means a null argument leaves a lease HELD and RENEWED by a pod whose
        // only reference was just discarded by the failed constructor: nothing
        // can ever call close() to release it, so the lease names a dead pod
        // forever, no other pod's tryAcquire can win, and -- the constructor
        // having thrown -- this pod cannot forward either. One null argument
        // stops the whole fleet committing for the life of the JVM.
        assertThatThrownBy(() -> new FleetSequencer(null, config("poda", A),
                new InProcessTransport(), () -> {
                    elected.set(true);
                    return Optional.empty();
                }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("store");

        assertThat(elected.get())
                .as("no term was taken, so there is nothing left holding a lease")
                .isFalse();
    }
}
