// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static binjava.sequencer.DedupFixtures.firstOffsetOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
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

        Refusing(IOException failure) {
            this(failure, null);
        }

        Refusing(IOException failure, IOException closeFailure) {
            this.failure = failure;
            this.closeFailure = closeFailure;
        }

        @Override public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            commits++;
            throw failure;
        }

        @Override public void close() throws IOException {
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
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
        var store = new binjava.binstore.CountingBinStore(backing);
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
