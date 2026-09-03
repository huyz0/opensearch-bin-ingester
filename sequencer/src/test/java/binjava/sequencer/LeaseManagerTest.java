// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Lease;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ THE LEASE IS LIVENESS, NOT SAFETY (ADR-0002). Losing it must never lose a
 * write: safety comes from the epoch in the object path plus the write-once
 * chain, so every test here is about who may sequence and for how long, never
 * about whether a committed record survives.
 */
class LeaseManagerTest {

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
        return new LeaseManager(store, "bins/cluster-a", podId, "", TTL, RENEW, clock);
    }

    @Test
    void theKeyCarriesTheSlotSoRaisingSIsConfigNotAFormatChange() {
        // ⚠️ ADR-0007 fixes S = 1, but the slot dimension stays present in
        // every path and every key so raising it later is configuration rather
        // than a key-grammar change -- the same reason CommitLog's chain path
        // has carried slot 0 since M1.
        LeaseManager m = manager(new MemoryBinStore(), "pod1", new TestClock());
        assertThat(m.key()).isEqualTo("bins/cluster-a/ctl/lease/0.json");
    }

    @Test
    void theFirstAcquisitionStartsAtEpochOneBecauseZeroMeansNoLease() throws Exception {
        // ⚠️ ONE, not zero, and this is M4.4b: epoch 0 is RESERVED for "no
        // lease". `CommitLog`'s 2-arg constructor writes epoch 0 for every
        // caller that has no lease yet -- `DefaultIngest` among them -- so if
        // the first leadership term were also 0, wiring the lease into the
        // commit path would put a first leader's chain byte-identical to what
        // those callers already write. `putIfAbsent` would still buy I1, but
        // I3's "readers of the new epoch never look there" would be VOID,
        // because it would not be a different epoch. Reserving 0 makes every
        // leased chain provably disjoint from the unleased one.
        TestClock clock = new TestClock();
        LeaseManager m = manager(new MemoryBinStore(), "pod1", clock);
        Lease got = m.tryAcquire().orElseThrow();
        assertThat(got.epoch()).isEqualTo(1);
        assertThat(got.holderPodId()).isEqualTo("pod1");
        assertThat(got.expiresAtMillis()).isEqualTo(clock.millis() + TTL.toMillis());
    }

    @Test
    void theFirstAcquisitionUsesPutIfAbsentBecausePutIfMatchThrowsOnAnAbsentKey()
            throws Exception {
        // ⚠️ The SPI contract is explicit: `putIfMatch` on an ABSENT key
        // THROWS -- "there is no version to have moved from" -- and that is
        // deliberately NOT folded into the lost-race empty Optional. So the
        // first acquisition is a different call from every later one.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        manager(store, "pod1", new TestClock()).tryAcquire().orElseThrow();
        assertThat(store.counts().puts()).as("exactly one write, and it succeeded").isEqualTo(1);
    }

    @Test
    void exactlyOneOfTwoContendersAcquiresAnUnheldLease() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        Optional<Lease> a = manager(shared, "podA", clock).tryAcquire();
        Optional<Lease> b = manager(shared, "podB", clock).tryAcquire();
        assertThat(a).isPresent();
        assertThat(b).as("the lease is held and unexpired, so B must not get it").isEmpty();
    }

    @Test
    void aHeldUnexpiredLeaseIsNotStolen() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        manager(shared, "podA", clock).tryAcquire().orElseThrow();
        clock.advance(TTL.minusMillis(1));
        assertThat(manager(shared, "podB", clock).tryAcquire())
                .as("one millisecond before expiry it is still A's").isEmpty();
    }

    @Test
    void anExpiredLeaseIsTakenOverAndTheEpochAdvancesByExactlyOne() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        Lease first = manager(shared, "podA", clock).tryAcquire().orElseThrow();
        clock.advance(TTL);
        Lease second = manager(shared, "podB", clock).tryAcquire().orElseThrow();
        assertThat(first.epoch()).as("the first term is 1; 0 is reserved for no-lease").isEqualTo(1);
        // ⚠️ EXACTLY one. The epoch is in the object path, so a skipped epoch
        // leaves a chain a reader cannot distinguish from one it failed to
        // read, and a reused epoch lets a fenced writer's objects pass for the
        // new term's.
        assertThat(second.epoch()).isEqualTo(2);
        assertThat(second.holderPodId()).isEqualTo("podB");
    }

    @Test
    void theHolderRenewsWithoutChangingTheEpoch() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager m = manager(shared, "podA", clock);
        m.tryAcquire().orElseThrow();
        clock.advance(RENEW);
        Lease renewed = m.renew().orElseThrow();
        // ⚠️ Renewal must NOT bump the epoch: it identifies a term, and a new
        // term every few seconds would start a new chain and orphan the last.
        assertThat(renewed.epoch()).isEqualTo(1);
        assertThat(renewed.expiresAtMillis()).isEqualTo(clock.millis() + TTL.toMillis());
    }

    @Test
    void aFencedHolderCannotRenewAndLearnsItFromTheEmptyResult() throws Exception {
        // ⚠️ THE GRAY-FAILURE PATH. A holder paused long enough to lose its
        // lease must discover that on its next renew and stop sequencing --
        // "renewal failure ⇒ immediately stop sequencing". It learns by the
        // renew returning empty, never by consulting a clock.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(shared, "podA", clock);
        a.tryAcquire().orElseThrow();
        clock.advance(TTL);
        manager(shared, "podB", clock).tryAcquire().orElseThrow();
        assertThat(a.renew()).as("A's version moved under it -- it is fenced").isEmpty();
    }

    @Test
    void renewingWithoutEverHavingAcquiredIsRefused() throws Exception {
        LeaseManager m = manager(new MemoryBinStore(), "pod1", new TestClock());
        assertThat(m.renew()).isEmpty();
    }

    @Test
    void releaseKeepsTheEpochSoTheNextHolderStillAdvancesIt() throws Exception {
        // ⚠️ RELEASE MUST NOT DELETE THE OBJECT. Deleting it would lose the
        // epoch counter, so the next acquirer would start again at the first
        // term (1, since 0 is reserved -- M4.4b) and REUSE
        // an epoch a previous term already wrote objects under -- which is
        // precisely what fencing exists to prevent. Release instead writes an
        // already-expired lease, keeping the counter.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(shared, "podA", clock);
        a.tryAcquire().orElseThrow();
        a.release();
        Lease next = manager(shared, "podB", clock).tryAcquire().orElseThrow();
        assertThat(next.epoch()).as("the counter survived the release").isEqualTo(2);
    }

    @Test
    void releaseMakesTheLeaseImmediatelyTakeableWithoutWaitingOutTheTtl() throws Exception {
        // ⚠️ The whole point of a voluntary release on SIGTERM: a failover in
        // milliseconds instead of one bounded by the TTL (ADR-0007 puts that
        // at ~10 s worst case).
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(shared, "podA", clock);
        a.tryAcquire().orElseThrow();
        assertThat(manager(shared, "podB", clock).tryAcquire())
                .as("before release B cannot have it").isEmpty();
        a.release();
        assertThat(manager(shared, "podB", clock).tryAcquire())
                .as("after release B takes it with no clock advance at all").isPresent();
    }

    @Test
    void releasingWhenNotHeldIsAHarmlessNoOpRatherThanAnError() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LeaseManager m = manager(store, "pod1", new TestClock());
        m.release();
        assertThat(store.counts().total())
                .as("nothing held, so nothing written -- SIGTERM on a non-holder is normal")
                .isZero();
    }

    @Test
    void aFencedHolderCannotReleaseSomeoneElsesLease() throws Exception {
        // ⚠️ Otherwise a paused holder waking after failover would hand B's
        // live term to whoever asked next.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(shared, "podA", clock);
        a.tryAcquire().orElseThrow();
        clock.advance(TTL);
        Lease bs = manager(shared, "podB", clock).tryAcquire().orElseThrow();
        a.release();
        Lease onDisk = Lease.decode(shared.get(a.key()).readAllBytes());
        assertThat(onDisk).as("B's term is untouched").isEqualTo(bs);
    }

    @Test
    void aCorruptLeaseObjectFailsLoudlyRatherThanBeingTakenOver() throws Exception {
        // ⚠️ Leadership must not be decided from bytes nobody can parse. The
        // tempting alternative -- treat unreadable as unheld and take it -- is
        // how two nodes end up sequencing at once.
        MemoryBinStore shared = new MemoryBinStore();
        LeaseManager m = manager(shared, "pod1", new TestClock());
        shared.put(m.key(), Body.ofBytes("not a lease".getBytes(
                java.nio.charset.StandardCharsets.UTF_8)));
        assertThatThrownBy(m::tryAcquire).isInstanceOf(IOException.class);
    }

    @Test
    void theRenewIntervalIsConfigurationNotAConstant() {
        // ⚠️ Measurement M1 settles TTL and the renew interval at M8, from
        // real GC-pause data. This milestone must not hardcode what M8 will
        // measure, so both are constructor parameters and the accessor exists
        // for the loop that will use it.
        LeaseManager m = new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(30), Duration.ofSeconds(7), new TestClock());
        assertThat(m.renewInterval()).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    void aRenewIntervalNotShorterThanTheTtlIsRefused() {
        // ⚠️ Renewing no more often than the lease lives guarantees losing it:
        // the corpus renews at TTL/3 precisely so a missed renew is survivable.
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(10), Duration.ofSeconds(10), new TestClock()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("renewInterval");
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(10), Duration.ofSeconds(11), new TestClock()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNonPositiveTtlIsRefused() {
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ZERO, Duration.ofSeconds(1), new TestClock()))
                .isInstanceOf(IllegalArgumentException.class)
                // ⚠️ The FULL message. "ttl" alone is also contained in the
                // renewInterval-vs-ttl guard's own message, so the whole ttl
                // positivity guard was deletable with the suite green --
                // verbatim the equivalent-mutant class M4.3 round 2 closed.
                .hasMessageContaining("ttl must be positive");
    }

    @Test
    void acquiringCostsOneReadAndOneWriteAndRenewingCostsOneWrite() throws Exception {
        // ⚠️ The lease is ~0.33 PUT/s at TTL/3, which the cost model budgets.
        // A read-per-renew would double it for nothing: the holder already
        // knows its own version.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "pod1", clock);
        m.tryAcquire().orElseThrow();
        long afterAcquire = store.counts().total();
        clock.advance(RENEW);
        m.renew().orElseThrow();
        assertThat(store.counts().total() - afterAcquire)
                .as("renewal is ONE conditional write, with no re-read").isEqualTo(1);
    }

    @Test
    void whenTwoNodesBothSeeAnAbsentLeaseExactlyOneWinsTheConditionalWrite() throws Exception {
        // ⚠️ THE ACQUISITION CAS ITSELF, which nothing else here pins. Round-1
        // test review measured that `putIfAbsent` could be replaced by an
        // unconditional `put` with all 18 tests green, because the only
        // multi-contender test refuses its second contender at the EXPIRY
        // check and never reaches a conditional write at all. The production
        // consequence is two ingesters on a fresh prefix both holding the
        // SAME first term (1 since M4.4b reserved epoch 0).
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(new StatBlindStore(shared), "podA", clock);
        LeaseManager b = manager(new StatBlindStore(shared), "podB", clock);
        Optional<Lease> first = a.tryAcquire();
        Optional<Lease> second = b.tryAcquire();
        assertThat(first).isPresent();
        assertThat(second).as("the store arbitrates: the loser gets empty, not a second first-term lease")
                .isEmpty();
    }

    @Test
    void aHolderCanRenewRepeatedlyBecauseItRecordsEachNewVersion() throws Exception {
        // ⚠️ Round-1 test review measured that deleting `heldVersion = won.get()`
        // survived every test, because both renew tests renewed ONCE. Under
        // that bug the SECOND renew CASes against a superseded version, loses,
        // and the holder reads its own write as proof it is fenced -- every
        // renew interval, forever, and indistinguishable at the call site from
        // real fencing.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager m = manager(shared, "podA", clock);
        m.tryAcquire().orElseThrow();
        for (int i = 0; i < 5; i++) {
            clock.advance(RENEW);
            Lease renewed = m.renew().orElseThrow(
                    () -> new AssertionError("a healthy holder must not fence itself"));
            assertThat(renewed.epoch()).isEqualTo(1);
        }
    }

    @Test
    void aNodeThatTookOverCanThenRenew() throws Exception {
        // ⚠️ The takeover path records its own version, which nothing else
        // pinned: round-1 review measured that adopting the version it CASed
        // FROM left all 18 green. Failover would appear to succeed and then the
        // new holder's first renew would lose and it would stop sequencing.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        manager(shared, "podA", clock).tryAcquire().orElseThrow();
        clock.advance(TTL);
        LeaseManager b = manager(shared, "podB", clock);
        b.tryAcquire().orElseThrow();
        clock.advance(RENEW);
        assertThat(b.renew()).as("the successor must be able to hold its own term").isPresent();
    }

    @Test
    void heldReportsWhatThisInstanceActuallyHolds() throws Exception {
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager m = manager(shared, "podA", clock);
        assertThat(m.held()).isEmpty();
        Lease got = m.tryAcquire().orElseThrow();
        assertThat(m.held()).contains(got);
        m.release();
        assertThat(m.held()).as("a released lease is no longer held").isEmpty();
    }

    @Test
    void aFencedHolderStopsBelievingItHoldsTheLease() throws Exception {
        // ⚠️ Not cosmetic: a caller that ignored the empty renew result must
        // not be able to keep CASing against a stale version.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(shared, "podA", clock);
        a.tryAcquire().orElseThrow();
        clock.advance(TTL);
        manager(shared, "podB", clock).tryAcquire().orElseThrow();
        assertThat(a.renew()).isEmpty();
        assertThat(a.held()).as("it dropped the belief along with the lease").isEmpty();
    }

    @Test
    void aNegativeTtlOrRenewIntervalIsRefused() {
        // ⚠️ The ZERO case alone left `ttl.isNegative()` deletable, and the
        // whole renewInterval positivity guard with it.
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(-1), Duration.ofSeconds(1), new TestClock()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttl must be positive");
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(10), Duration.ZERO, new TestClock()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("renewInterval");
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "pod1", "",
                Duration.ofSeconds(10), Duration.ofSeconds(-1), new TestClock()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("renewInterval");
    }

    @Test
    void aBlankPodIdIsRefusedAtConstructionRatherThanAtTheFirstAcquire() {
        // ⚠️ Fail EARLY. An unset POD_NAME otherwise constructs fine and fails
        // later out of `tryAcquire`, which is declared `throws IOException`, as
        // an unchecked IllegalArgumentException from Lease's constructor.
        assertThatThrownBy(() -> new LeaseManager(new MemoryBinStore(), "p", "  ", "",
                TTL, RENEW, new TestClock()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("podId");
    }

    @Test
    void theEndpointIsWrittenIntoTheLeaseRatherThanDiscarded() throws Exception {
        // ⚠️ Every other test passes "", so `endpoint` could be replaced by a
        // "" literal with the suite green -- and M5's commit forwarding is the
        // thing that will need to READ it.
        MemoryBinStore shared = new MemoryBinStore();
        LeaseManager m = new LeaseManager(shared, "bins/cluster-a", "pod1", "10.0.0.4:8080",
                TTL, RENEW, new TestClock());
        assertThat(m.tryAcquire().orElseThrow().holderEndpoint()).isEqualTo("10.0.0.4:8080");
    }

    @Test
    void theEndpointIsAlsoWrittenOnTheTAKEOVERPathNotJustTheColdStart() throws Exception {
        // ⚠️ Round-2 review measured that substituting "" at the TAKEOVER call
        // site survived, while the cold-start one died -- and takeover is the
        // FAILOVER path, which is exactly the half M5's commit forwarding needs
        // in order to reach the new holder.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        new LeaseManager(shared, "bins/cluster-a", "podA", "10.0.0.1:1", TTL, RENEW, clock)
                .tryAcquire().orElseThrow();
        clock.advance(TTL);
        Lease taken = new LeaseManager(shared, "bins/cluster-a", "podB", "10.0.0.2:2",
                TTL, RENEW, clock).tryAcquire().orElseThrow();
        assertThat(taken.holderEndpoint()).isEqualTo("10.0.0.2:2");
    }

    @Test
    void aLostAcquisitionDropsAnyBeliefFromAnEarlierTerm() throws Exception {
        // ⚠️ `adopt`'s own belief-clearing, which round-2 review measured as
        // unconstrained: the two fenced-holder tests both reach the clearing in
        // `renew`, never in `adopt`. StatBlind makes the losing acquisition
        // reachable while `held` is already non-null.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(new StatBlindStore(shared), "podA", clock);
        a.tryAcquire().orElseThrow();
        assertThat(a.held()).isPresent();
        // A second acquisition attempt now LOSES -- the object exists and the
        // CAS is putIfAbsent -- and must leave no stale belief behind.
        assertThat(a.tryAcquire()).isEmpty();
        assertThat(a.held()).as("a lost acquisition clears the belief, as renew does").isEmpty();
    }

    @Test
    void aNonHolderRenewIssuesNoRequestAtAll() throws Exception {
        // ⚠️ Stronger than "returns empty". Round-2 review measured that adding
        // an unconditional `put` of an expired epoch-0 lease to that branch
        // survived -- a NON-HOLDER destroying the fencing counter.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LeaseManager m = manager(store, "pod1", new TestClock());
        assertThat(m.renew()).isEmpty();
        assertThat(store.counts().total())
                .as("a non-holder must not touch the lease object").isZero();
    }

    @Test
    void noAcquisitionEverWritesEpochZeroBecauseItIsReservedForTheUnleasedChain() throws Exception {
        // ⚠️ M4.4b's whole point, asserted across BOTH acquisition paths --
        // cold start and takeover -- because a lease at epoch 0 would collide
        // with `CommitLog`'s no-lease default and void I3 for the first term.
        MemoryBinStore shared = new MemoryBinStore();
        TestClock clock = new TestClock();
        Lease cold = manager(shared, "podA", clock).tryAcquire().orElseThrow();
        assertThat(cold.epoch()).isNotZero();
        clock.advance(TTL);
        Lease taken = manager(shared, "podB", clock).tryAcquire().orElseThrow();
        assertThat(taken.epoch()).isNotZero();
        assertThat(taken.epoch()).isGreaterThan(cold.epoch());

        // ⚠️ THE INPUT THAT MAKES THIS TEST'S NAME TRUE, and round-1 test
        // review found it missing: nothing in the tree ever handed
        // `takenOverBy` a lease already AT epoch 0, so
        // `epoch == 0 ? 0 : epoch + 1` -- which mints a lease at the reserved
        // epoch, the exact state this task exists to make impossible --
        // survived all 491 tests. The input is not hypothetical: `Lease`
        // deliberately keeps 0 PARSEABLE so a legacy or hand-edited object
        // does not become a liveness stall, and the pre-M4.4b build minted
        // exactly such an object on every cold start.
        MemoryBinStore legacy = new MemoryBinStore();
        TestClock t2 = new TestClock();
        LeaseManager m = manager(legacy, "podC", t2);
        legacy.put(m.key(), Body.ofBytes(
                new Lease(0, "old", "", t2.millis()).encode()));
        assertThat(m.tryAcquire().orElseThrow().epoch())
                .as("taking over a lease already at the reserved epoch still lands above it")
                .isEqualTo(1);
    }
}
