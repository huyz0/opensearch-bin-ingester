// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Lease;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ AN AMBIGUOUS ACQUISITION IS THE WORSE HALF (M4.3g). M4.3d taught
 * {@code renew} and {@code release} that a conditional write which THROWS may
 * have landed; {@code tryAcquire} was left treating it as a flat failure. A
 * node whose acquiring write landed then HOLDS the lease and cannot discover
 * it: its own {@code tryAcquire} refuses because what it reads is unexpired,
 * and {@code renew} refuses because it believes it holds nothing. The cluster
 * has no sequencer for a full TTL and that node recovers only by burning an
 * epoch — where an ambiguous renew recovers on the very next call.
 *
 * <p>⚠️ The belief recorded on that path is UNVERIFIED, not held, and
 * {@code held()} must keep saying empty for it. M4.3e's property is that
 * {@code held()} never reports a term this instance does not hold, and "my
 * write may have landed" is not holding it.
 */
class LeaseManagerAcquireAmbiguityTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);

    /** ⚠️ ADVANCED, never slept on. */
    private static final class TestClock extends Clock {
        private long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        void advance(Duration d) {
            millis += d.toMillis();
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

    private static LeaseManager manager(binjava.binstore.BinStore store, String podId,
            TestClock clock) {
        return new LeaseManager(store, new LeaseConfig("bins/cluster-a", podId, "",
                TTL, RENEW), clock);
    }

    @Test
    void anAcquisitionThatLandedButLostItsResponseIsFoundOnTheNextTry() throws Exception {
        // ⚠️ Measured before the fix: the store read `epoch=1 holderPodId=podA`
        // while `held()`, `tryAcquire()` and `renew()` ALL returned empty, and
        // podA recovered only after a full TTL by taking over its own lease at
        // epoch 2.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);

        assertThatThrownBy(m::tryAcquire)
                .as("the ambiguity is reported, not swallowed")
                .isInstanceOf(IOException.class);
        assertThat(m.held())
                .as("and NOT reported as held -- the write MAY have landed, which "
                        + "is not the same as holding the term")
                .isEmpty();

        assertThat(m.tryAcquire())
                .as("asking again discovers the term it already won")
                .isPresent();
        assertThat(m.held()).isPresent();
        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()).epoch())
                .as("the SAME term -- recovery is not a takeover, and no epoch is burnt")
                .isEqualTo(1);
        assertThat(m.renew()).as("and it is a usable term, not just a believed one").isPresent();
    }

    @Test
    void anAcquisitionWhoseWriteWasLostStillAcquiresNormally() throws Exception {
        // ⚠️ The other half: nothing landed, so there is nothing to discover
        // and the retry must be an ordinary cold start rather than an attempt
        // to adopt a lease that is not there.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LOST, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);

        assertThatThrownBy(m::tryAcquire).isInstanceOf(IOException.class);
        assertThat(m.held()).isEmpty();

        assertThat(m.tryAcquire()).as("an ordinary first acquisition").isPresent();
        assertThat(m.held().orElseThrow().epoch()).isEqualTo(1);
    }

    @Test
    void anAmbiguousAcquisitionThatWasOvertakenDoesNotClaimTheSuccessorsTerm() throws Exception {
        // ⚠️ THE CONTROL. Recovery must key on this instance's own candidate
        // term, not adopt whatever it finds -- otherwise the node that failed
        // to acquire walks in on the winner.
        // ⚠️ HERE THE podId HALF IS THE LOAD-BEARING ONE, which is the reverse
        // of M4.3d and M4.3e. Those pinned the epoch half, because a takeover
        // ALWAYS advances the epoch, so a StatefulSet-restarted podA differs
        // from its predecessor only there. This path is the COLD START: two
        // different pods both mint epoch 1, so the epoch cannot tell them apart
        // and only the holder can. The conjunct that is defence-in-depth on the
        // renew path is the discriminator on this one.
        // ⚠️ What is NOT distinguishable, and cannot be without putting a
        // unique writer mark in the lease: the SAME podId cold-starting twice
        // concurrently, where our unverified candidate is byte-identical to the
        // winner's lease. That is out-of-protocol -- a StatefulSet exists to
        // make pod names unique -- and closing it would be a wire-format
        // change. Recorded in M4.3l rather than pretended away.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LOST, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager loser = manager(store, "podA", clock);
        assertThatThrownBy(loser::tryAcquire).isInstanceOf(IOException.class);

        LeaseManager winner = manager(backing, "podB", clock);
        assertThat(winner.tryAcquire()).isPresent();
        assertThat(Lease.decode(backing.get(winner.key()).readAllBytes()).epoch())
                .as("the winner cold-started too, so the epochs MATCH")
                .isEqualTo(1);

        assertThat(loser.tryAcquire())
                .as("someone else's live term is not this instance's to discover")
                .isEmpty();
        assertThat(loser.held()).isEmpty();
    }

    @Test
    void aTakeoverThatLandedButLostItsResponseIsAlsoFoundOnTheNextTry() throws Exception {
        // ⚠️ THE SITE THE INCIDENT WAS MEASURED ON. This row's own evidence is
        // a TAKEOVER -- the store reading `epoch=2 holderPodId=podA` -- and
        // epoch 2 comes from `takenOverBy`, not from a cold start. The first
        // draft covered only `putIfAbsent`: deleting the takeover site's
        // remembering left all 82 tests green, so the half of the fix the bug
        // report came from was the half that shipped unconstrained.
        MemoryBinStore backing = new MemoryBinStore();
        TestClock clock = new TestClock();
        assertThat(manager(backing, "podA", clock).tryAcquire())
                .as("someone holds epoch 1 and then goes away").isPresent();
        clock.advance(TTL);

        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_MATCH);
        LeaseManager successor = manager(store, "podB", clock);

        assertThatThrownBy(successor::tryAcquire).isInstanceOf(IOException.class);
        assertThat(successor.held())
                .as("a takeover that MAY have landed is not a term held")
                .isEmpty();

        assertThat(successor.tryAcquire())
                .as("asking again discovers the takeover it already won")
                .isPresent();
        assertThat(Lease.decode(backing.get(successor.key()).readAllBytes()).epoch())
                .as("epoch 2 -- the term it actually took, with no THIRD epoch burnt "
                        + "taking over from itself")
                .isEqualTo(2);
        assertThat(successor.renew()).as("and it is usable").isPresent();
    }

    @Test
    void renewingStraightAfterAnAmbiguousAcquisitionSettlesItRatherThanThrowing() throws Exception {
        // ⚠️ NO INTERVENING `tryAcquire`. Every other test here settles the
        // unverified belief through the acquire path first, so no test in the
        // tree ever reached `renew` with a null version -- and `renew`'s own
        // `@return`, added by this task, promises exactly this: "an unverified
        // belief reports nothing held, yet a renew can settle it and return a
        // lease". A contract sentence with no executable form.
        // ⚠️ The mutation this kills is a plausible tightening --
        // `mine.ambiguous() && mine.verified()` -- under which the refresh is
        // skipped and `putIfMatch` is handed a null version, throwing an
        // unchecked NPE out of a method documented to throw IOException.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThatThrownBy(m::tryAcquire).isInstanceOf(IOException.class);

        clock.advance(RENEW);
        assertThat(m.renew())
                .as("the renew settles the term the acquisition may have won")
                .isPresent();
        assertThat(m.held()).isPresent();
    }

    @Test
    void releasingAfterAnAmbiguousAcquisitionStillFreesTheLeaseAtOnce() throws Exception {
        // ⚠️ The same shape M4.3d built for the renew path
        // (`releasingAfterAnAmbiguousRenewStillFreesTheLeaseAtOnce`), owed to
        // the acquire path and missing from it. Under the tightening above,
        // `release` throws an unchecked NPE on the SIGTERM path: the pod exits
        // without writing the expired lease and the successor waits a full TTL
        // -- the harm release exists to prevent, on the branch that was left
        // uncovered.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThatThrownBy(m::tryAcquire).isInstanceOf(IOException.class);

        m.release();

        // ⚠️ NO clock advance: a successor that has to wait is the failure.
        assertThat(manager(backing, "podB", clock).tryAcquire())
                .as("the term it may have won was actually given up")
                .isPresent();
    }

    @Test
    void anAmbiguousTakeoverDoesNotAdoptTheLapsedTermItWasTakingOverFrom() throws Exception {
        // ⚠️ THE EPOCH CONJUNCT AT THIS SITE. The control above pins the podId
        // half, because two different pods both cold-start at epoch 1. This
        // pins the other half at the same call site, and it needs a scenario
        // where the holder MATCHES: a node whose own term lapsed while the
        // store was unreachable mints a candidate at epoch E+1 under its OWN
        // podId while the store still holds epoch E.
        // ⚠️ Under a podId-only predicate the node adopts its own LAPSED term
        // with a live version, `tryAcquire` returns an already-expired lease,
        // `held()` reports it -- violating M4.3e -- and the fencing epoch never
        // advances across the failure.
        MemoryBinStore backing = new MemoryBinStore();
        TestClock clock = new TestClock();
        assertThat(manager(backing, "podA", clock).tryAcquire()).isPresent();
        clock.advance(TTL);

        AmbiguousPutStore store = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LOST, AmbiguousPutStore.Target.PUT_IF_MATCH);
        LeaseManager restarted = manager(store, "podA", clock);
        assertThatThrownBy(restarted::tryAcquire).isInstanceOf(IOException.class);
        assertThat(Lease.decode(backing.get(restarted.key()).readAllBytes()).epoch())
                .as("the takeover was LOST, so the store still holds the lapsed term")
                .isEqualTo(1);

        assertThat(restarted.tryAcquire())
                .as("the retry is a real takeover, not an adoption of what it was "
                        + "taking over from")
                .isPresent();
        assertThat(Lease.decode(backing.get(restarted.key()).readAllBytes()).epoch())
                .as("the fencing epoch ADVANCED -- same holder, so only the epoch "
                        + "could have refused the lapsed term")
                .isEqualTo(2);
    }
}
