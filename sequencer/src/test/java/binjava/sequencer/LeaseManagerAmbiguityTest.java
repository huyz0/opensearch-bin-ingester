// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
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
 * ⚠️ AN AMBIGUOUS RENEW MUST NOT SELF-FENCE A HEALTHY LEADER (M4.3d). If the
 * conditional PUT lands but its response is lost, the holder's cached
 * {@code Version} is stale and the NEXT renew loses to the holder's OWN bytes
 * — so a node that never stopped being the leader concludes it was fenced, and
 * the cluster has no sequencer until someone takes over an unexpired lease it
 * must first wait out.
 *
 * <p>⚠️ The retry cadence is the CALLER's, not one renew interval: {@code renew}
 * lets the {@code IOException} escape, and "the store was unreachable" is
 * precisely the error a caller retries at once. So the node can conclude it is
 * fenced almost immediately while the landed lease runs to the full TTL.
 *
 * <p>⚠️ Safety is never at stake — ADR-0002 makes the lease liveness, and the
 * gap sits inside ADR-0007's accepted failover. This is about not spending a
 * failover the cluster did not need.
 */
class LeaseManagerAmbiguityTest {

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
    void anAmbiguousRenewWhoseWriteLandedDoesNotFenceItsOwnHolder() throws Exception {
        // ⚠️ THE DANGEROUS HALF. The write took effect, so the version moved,
        // and the holder does not know. Under the defect the next renew's
        // conditional write loses to the bytes this very instance wrote.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew)
                .as("the ambiguity is reported, not swallowed -- the caller must "
                        + "not read a lost response as a successful renew")
                .isInstanceOf(IOException.class);

        clock.advance(RENEW);
        assertThat(m.renew())
                .as("the holder recovers its own term instead of fencing itself")
                .isPresent();
        assertThat(m.held()).isPresent();
        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()).epoch())
                .as("still the SAME term -- recovery is not a takeover")
                .isEqualTo(1);
    }

    @Test
    void aCompoundFailureWhereTheREREADAlsoFailsStillKeepsTheAmbiguousTermRatherThanFencing()
            throws Exception {
        // ⚠️ THE COMPOUND FAILURE M4.3d's round-1 test review flagged as
        // defended by nobody's intent (M4.3i): the object store is STILL
        // unreachable when the next renew's `refreshed()` tries to RE-READ,
        // after an earlier write's response was already lost. The shipped
        // code is correct here by CONSTRUCTION -- `refreshed()`'s `store.stat`
        // throws before `belief` is ever reassigned, so the ambiguous flag
        // untouched by that call survives as whatever it already was -- but
        // nothing proved it. A well-meaning try/catch around a caller's own
        // renew loop that read ANY IOException as "not renewed" and self-fenced
        // would reintroduce M4.3d's exact harm through this exact path, with
        // every OTHER test in this file still green, because none of them
        // makes the store fail on the read half.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_MATCH, true);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew)
                .as("the ambiguous write's response is lost")
                .isInstanceOf(IOException.class);

        clock.advance(RENEW);
        assertThatThrownBy(m::renew)
                .as("the re-read that would resolve the ambiguity ALSO fails -- "
                        + "a compound failure, not the same one reported twice")
                .isInstanceOf(IOException.class);
        // ⚠️ THE ASSERTION THAT MATTERS. Two failures in a row are exactly the
        // shape that tempts a caller into giving up -- and `held()` staying
        // present here is what proves NOTHING in this instance did.
        assertThat(m.held())
                .as("the term is not given up over a transient read failure "
                        + "stacked on a transient write failure")
                .isPresent();

        clock.advance(RENEW);
        assertThat(m.renew())
                .as("once the store is reachable again the deferred refresh "
                        + "completes and the SAME term renews -- the ambiguous "
                        + "flag survived BOTH failures rather than being cleared "
                        + "by either")
                .isPresent();
        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()).epoch())
                .as("still the SAME term -- recovery is not a takeover")
                .isEqualTo(1);
    }

    @Test
    void anAmbiguousRenewWhoseWriteWasLostAlsoKeepsTheTerm() throws Exception {
        // ⚠️ The version did NOT move here, so a correct recovery must find
        // the term intact and keep it. A refresh that cleared the belief
        // whenever it ran would fence the holder on this path instead.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LOST);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew).isInstanceOf(IOException.class);

        clock.advance(RENEW);
        assertThat(m.renew()).as("nothing was lost, so nothing is given up").isPresent();
        assertThat(m.held()).isPresent();
    }

    @Test
    void anAmbiguousRenewFollowedByARealTakeoverStillReportsFenced() throws Exception {
        // ⚠️ THE CONTROL, and the successor REUSES podA's name on purpose. A
        // StatefulSet gives stable pod names, so the realistic successor to a
        // dead podA is a restarted podA -- and with a podB successor this test
        // passes even if the epoch half of the predicate is deleted, because
        // the podId half alone still refuses. It would then pass while a
        // fenced holder could match on podId, adopt the SUCCESSOR'S live
        // version, and win its next conditional write: two nodes sequencing at
        // once, the one thing this predicate exists to prevent.
        // ⚠️ Said plainly rather than implied: this pins the EPOCH half. The
        // podId half is defence-in-depth and is NOT pinned, because no path in
        // the protocol mints a same-epoch lease under a different holder --
        // `takenOverBy` advances the epoch and restamps the holder together,
        // and `renewedUntil` changes neither.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED);
        TestClock clock = new TestClock();
        LeaseManager a = manager(store, "podA", clock);
        assertThat(a.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(a::renew).isInstanceOf(IOException.class);

        // podA is genuinely gone long enough for podB to take over.
        clock.advance(TTL);
        LeaseManager b = manager(backing, "podA", clock);
        assertThat(b.tryAcquire()).isPresent();
        assertThat(b.held().orElseThrow().epoch())
                .as("a NEW term under the SAME name -- only the epoch tells them apart")
                .isEqualTo(2);

        assertThat(a.renew())
                .as("the old process really was superseded -- recovery must not undo that")
                .isEmpty();
        assertThat(a.held()).as("and it stops believing it holds the term").isEmpty();
        assertThat(Lease.decode(backing.get(a.key()).readAllBytes()).epoch())
                .as("the successor's term stands, unmolested by the fenced holder")
                .isEqualTo(2);
    }

    @Test
    void releasingAfterAnAmbiguousRenewStillFreesTheLeaseAtOnce() throws Exception {
        // ⚠️ M4.3d widened scope to `release` on the argument that releasing
        // with a stale version writes NOTHING, so the successor waits out the
        // very TTL release exists to spare it. That argument was prose with no
        // executable form -- the whole refresh could be deleted from `release`
        // with every other test green. This is the argument as a test.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED);
        TestClock clock = new TestClock();
        LeaseManager a = manager(store, "podA", clock);
        assertThat(a.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(a::renew).isInstanceOf(IOException.class);
        a.release();

        // ⚠️ NO clock advance: a successor that has to wait is the failure.
        LeaseManager b = manager(backing, "podB", clock);
        assertThat(b.tryAcquire())
                .as("the lease was actually given up, not silently kept for a TTL")
                .isPresent();
    }

    @Test
    void aReleaseWhoseSuccessfulRefreshIsFollowedByAFailedWriteKeepsTheRefreshedBelief()
            throws Exception {
        // ⚠️ M4.3k, folding in an M4.3f round-2 minor. `refreshed()`'s
        // `belief = fresh` field write is unconstrained by every OTHER test
        // in this file: a caller only ever needs the RETURN value for its own
        // local logic, so deleting the field write leaves the whole suite
        // green -- the return value carries the caller, but nothing checks
        // the FIELD afterward. It is not dead: a `release` whose post-refresh
        // conditional write ALSO fails leaves the STALE belief in the field
        // instead of the FRESH one, if that field write is missing.
        //
        // ⚠️ TWO INDEPENDENT AMBIGUOUS EVENTS, not one replayed: the cold-start
        // acquisition (`putIfAbsent`) is made ambiguous first, landing for
        // real but losing its response -- `writeOrRemember` remembers an
        // UNVERIFIED candidate (version == null, M4.3g). `release`'s OWN
        // conditional write (`putIfMatch`) is independently made ambiguous
        // second. `Target.PUT_IF_ABSENT` and `Target.PUT_IF_MATCH` never
        // intercept each other's calls, so stacking the two fakes here does
        // not need either to know about the other -- unlike stacking two on
        // the SAME target, where `Mode.LANDED`'s own delegate call would be
        // caught by the inner fake before the outer's caller ever sees it.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore acquireLands = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        AmbiguousPutStore store = new AmbiguousPutStore(acquireLands,
                AmbiguousPutStore.Mode.LOST, AmbiguousPutStore.Target.PUT_IF_MATCH);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);

        assertThatThrownBy(m::tryAcquire)
                .as("the cold-start write landed but its response was lost")
                .isInstanceOf(IOException.class);

        assertThatThrownBy(m::release)
                .as("release's own refresh succeeds -- clearing the ambiguity "
                        + "and establishing a REAL version where there was none "
                        + "-- but release's OWN conditional write then fails too")
                .isInstanceOf(IOException.class);

        // ⚠️ THE ASSERTION THAT MATTERS. Under the fix, `belief` is the
        // REFRESHED belief -- version real, ambiguous cleared -- so `held()`
        // reports present. Under a mutant that deletes `refreshed()`'s
        // `belief = fresh` field write, `belief` is still the ORIGINAL
        // `unverified()` candidate from the failed acquire: version null,
        // `verified()` false, so `held()` would report EMPTY instead, even
        // though a successful refresh happened moments before.
        assertThat(m.held())
                .as("the refreshed belief survives release's own failed write")
                .isPresent();
    }

    @Test
    void aRecoveryThatFindsNoLeaseAtAllStandsDownInsteadOfLooping() throws Exception {
        // ⚠️ Nothing legitimately deletes the object -- `release` writes an
        // expired lease precisely so the epoch counter survives -- so absence
        // means something outside this protocol happened. Standing down is the
        // safe direction, and the unsafe one is not merely unsafe but STUCK:
        // `putIfMatch` on an absent key throws by SPI contract, which re-arms
        // the ambiguity flag, so a recovery that returned "still mine" would
        // loop on IOException instead of giving up.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew).isInstanceOf(IOException.class);
        backing.delete(java.util.List.of(m.key()));

        assertThat(m.renew()).as("gives up rather than throwing").isEmpty();
        assertThat(m.held()).isEmpty();
    }

    @Test
    void anUnambiguousRenewCostsExactlyOneConditionalWriteAndNothingElse() throws Exception {
        // ⚠️ COST, not correctness: recovery re-reads, and a re-read on every
        // renew would DOUBLE the lease's request rate for a case that almost
        // never happens. The refresh is owed only after an ambiguous write.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        long statsBefore = store.counts().stats();
        long getsBefore = store.counts().gets();
        long putsBefore = store.counts().puts();
        long totalBefore = store.counts().total();

        clock.advance(RENEW);
        assertThat(m.renew()).isPresent();
        clock.advance(RENEW);
        assertThat(m.renew()).isPresent();

        assertThat(store.counts().puts() - putsBefore)
                .as("one conditional write per renew").isEqualTo(2);
        assertThat(store.counts().stats() - statsBefore)
                .as("and no re-read on the healthy path").isZero();
        assertThat(store.counts().gets() - getsBefore).isZero();
        assertThat(store.counts().total() - totalBefore)
                .as("and NOTHING else either -- a LIST added inside renew would "
                        + "pass every per-category assertion above")
                .isEqualTo(2);
    }

    @Test
    void aWriteThatLandsAfterTheRefreshReadStillSelfFencesOnce() throws Exception {
        // ⚠️ THIS PINS AN ACCEPTED COST, NOT A DESIRED BEHAVIOUR (ADR-0027).
        // One refresh attempt narrows M4.3d's self-fence; it does not close it.
        // If the client's timeout fires while the write is still queued inside
        // the object store, the refresh's read sees the version unmoved, adopts
        // it, clears the ambiguity flag -- and the conditional write that
        // follows loses to this instance's OWN earlier bytes, with nothing left
        // to say it might have been its own.
        // ⚠️ The node then stands down for the full TTL of a lease it never
        // stopped holding. Safety is untouched: a lost CAS is always the safe
        // direction, and ADR-0002 makes the lease liveness rather than safety,
        // so the cost is one failover inside ADR-0007's accepted budget.
        // ⚠️ If someone later implements the alternative ADR-0027 rejected,
        // THIS TEST SHOULD FAIL and be deleted with the ADR superseded. It
        // exists so the accepted cost is visible and measured rather than
        // described.
        MemoryBinStore backing = new MemoryBinStore();
        DelayedLandingStore store = new DelayedLandingStore(backing);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        long lateRenewExpiry = clock.millis() + TTL.toMillis();
        assertThatThrownBy(m::renew)
                .as("the write is queued, not lost -- but nothing can say which")
                .isInstanceOf(IOException.class);

        clock.advance(RENEW);
        assertThat(m.renew())
                .as("the refresh read the old version, then the late write landed "
                        + "underneath it -- the CAS loses to this node's own bytes")
                .isEmpty();
        assertThat(m.held())
                .as("so a healthy holder concludes it was fenced: the accepted cost")
                .isEmpty();
        // ⚠️ The EXACT expiry, not merely "unexpired". Asserting the latter
        // would be satisfied by the ORIGINAL acquisition's lease too, so it
        // could not tell the late-landing interleaving from a plain lost CAS --
        // and that interleaving is the whole of what this test claims to show.
        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()).expiresAtMillis())
                .as("what the store holds is the LATE RENEW, not the acquisition")
                .isEqualTo(lateRenewExpiry);

        // ⚠️ UNSHORTENABLE, which is the quantity ADR-0027 turns on and the
        // difference between "it might come back" and "it cannot before
        // T+TTL". The fencing branch destroyed the version, so `release` now
        // returns at its `belief == null` guard and cannot hand back a lease
        // this node still holds on the store. Retaining any version here --
        // even an unverified one, which keeps `held()` empty and so preserves
        // M4.3e -- would let `release` refresh onto the late renew and free the
        // lease at once, making the ADR false with the suite green.
        m.release();
        assertThat(manager(backing, "podB", clock).tryAcquire())
                .as("no successor can take it early: the stall is the full remainder")
                .isEmpty();
    }

    @Test
    void aLateWriteAlsoDefeatsReleaseAndNothingObservesTheLoss() throws Exception {
        // ⚠️ THE SAME ACCEPTED COST, ONE METHOD OVER (ADR-0027), and worse in
        // one respect: `release` DISCARDS its conditional write's result, so
        // where `renew` at least learns it lost, here nothing observes the loss
        // at all. The pod exits believing it handed the lease back, having
        // written nothing, and the successor waits out the very TTL release
        // exists to spare it -- the harm M4.3d widened its own scope to close,
        // reached through the residual ADR-0027 accepts.
        // ⚠️ Pinned because the ADR's scope claim should be executable rather
        // than a sentence: an earlier draft of it described only the renew
        // path, and a reader asking "does this cover release?" found no answer
        // in the artifact whose job is to be the answer.
        MemoryBinStore backing = new MemoryBinStore();
        DelayedLandingStore store = new DelayedLandingStore(backing);
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        long lateRenewExpiry = clock.millis() + TTL.toMillis();
        assertThatThrownBy(m::renew).isInstanceOf(IOException.class);

        // ⚠️ SIGTERM arrives while the renew is still queued in the store.
        m.release();
        assertThat(m.held()).as("the pod believes it gave the lease back").isEmpty();

        // ⚠️ NO clock advance, so this asks whether the lease was ACTUALLY
        // freed. It was not: the release's CAS lost to the late renew.
        assertThat(manager(backing, "podB", clock).tryAcquire())
                .as("the successor cannot take it -- it must wait out the TTL")
                .isEmpty();
        // ⚠️ The EXACT expiry again: "still unexpired" alone is also true of
        // the original acquisition's lease, so it could not distinguish the
        // late renew landing from the release simply never happening.
        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()).expiresAtMillis())
                .as("because what the store holds is the LATE RENEW, still live")
                .isEqualTo(lateRenewExpiry);
    }
}
