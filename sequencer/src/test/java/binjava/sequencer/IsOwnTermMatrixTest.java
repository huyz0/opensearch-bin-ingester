// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Lease;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * M4.3n. The NEGATIVE-CONTROL MATRIX for {@code isOwnTerm}, because the same
 * defect — a predicate confined to ONE of its two conjuncts, so the other
 * alone still discriminates and the gap reads as coverage — has now been
 * caught by review three times in one milestone (M4.3d, M4.3e, M4.3g) and the
 * per-commit shape of it is invisible.
 *
 * <p>⚠️ THE ENUMERATION MATTERS MORE THAN THE MECHANISM. {@code isOwnTerm} has
 * TWO lexical call sites — {@code tryAcquire}'s unexpired branch, and inside
 * {@code refreshed} — reached by FOUR paths, because {@code refreshed} is
 * itself called from acquire-recovery, from {@code renewLocked} AND from
 * {@code releaseLocked}. {@code release} is precisely where this milestone has
 * been bitten twice: M4.3d widened scope to give it the refresh and review
 * found that widening had zero coverage; M4.3g's review found {@code release}
 * with an unverified belief unreached again. A cell that is not in the spec
 * cannot show up as unfilled, which is why all four paths are enumerated here
 * even though three of them share one method body.
 *
 * <p>⚠️ EVERY SITE NEEDS BOTH CELLS, one conjunct varied at a time with the
 * OTHER HELD MATCHED. A restarted pod at epoch E+1 is refused by the epoch
 * alone; an out-of-protocol same-epoch lease under another holder is refused
 * by the podId alone; varying both at once would let a mutant that checks only
 * one conjunct still pass, because the other alone already discriminates —
 * the exact shape that stayed invisible for three tasks.
 *
 * <p>⚠️ THE EPOCH CELLS ARE IN-PROTOCOL (a genuine same-`podId` restart
 * takeover after expiry — a StatefulSet gives stable names, so this is the
 * realistic case). THE POD-ID CELLS ARE DELIBERATELY OUT-OF-PROTOCOL, named as
 * such rather than left to look accidental: {@code isOwnTerm}'s own reason for
 * checking podId at all is that "no in-protocol path mints a same-epoch lease
 * under a different holder" ({@code LeaseManagerBeliefTest}), so those cells
 * are built with a direct store write standing in for tampering or a bug
 * elsewhere, matching the precedent in {@code LeaseManagerTest}'s legacy-lease
 * fixtures.
 *
 * <p>Proposed by M4.3g's round-1 test review, which measured that a
 * podId-only predicate confined to the recovery block left all 83 sequencer
 * tests green.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class IsOwnTermMatrixTest {

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

    private static LeaseManager manager(BinStore store, String podId, TestClock clock) {
        return new LeaseManager(store, new LeaseConfig("bins/cluster-a", podId, "",
                TTL, RENEW), clock);
    }

    /** Which of {@code isOwnTerm}'s two conjuncts this case mismatches, alone. */
    private enum MismatchedConjunct {
        EPOCH, POD_ID
    }

    /**
     * Overwrites the stored lease so it mismatches {@code reference} in
     * exactly ONE conjunct, holding the other matched.
     *
     * @param reference the lease this instance currently believes is its own
     */
    private static void mismatchOneConjunct(BinStore store, String key, Lease reference,
            MismatchedConjunct which, TestClock clock) throws IOException {
        long expiry = clock.millis() + TTL.toMillis();
        Lease mismatched = which == MismatchedConjunct.EPOCH
                ? new Lease(reference.epoch() + 1, reference.holderPodId(), "", expiry)
                : new Lease(reference.epoch(), "a-different-pod", "", expiry);
        store.put(key, Body.ofBytes(mismatched.encode()));
    }

    @ParameterizedTest
    @EnumSource(MismatchedConjunct.class)
    void theUnexpiredBranchRefusesASingleMismatchedConjunct(MismatchedConjunct which)
            throws Exception {
        // ⚠️ SITE 1: tryAcquire's own unexpired-lease branch.
        MemoryBinStore store = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager m = manager(store, "podA", clock);
        Lease mine = m.tryAcquire().orElseThrow();

        if (which == MismatchedConjunct.EPOCH) {
            // IN-PROTOCOL: a genuine same-pod restart takeover after expiry.
            clock.advance(TTL);
            assertThat(manager(store, "podA", clock).tryAcquire()).isPresent();
        } else {
            // OUT-OF-PROTOCOL, named as such -- see the class javadoc.
            mismatchOneConjunct(store, m.key(), mine, which, clock);
        }

        assertThat(m.tryAcquire())
                .as("something valid and unexpired is there, so nothing to acquire")
                .isEmpty();
        assertThat(m.held())
                .as(which + " alone must discriminate -- the stored term is not m's")
                .isEmpty();
    }

    @ParameterizedTest
    @EnumSource(MismatchedConjunct.class)
    void acquireRecoveryRefusesASingleMismatchedConjunct(MismatchedConjunct which)
            throws Exception {
        // ⚠️ SITE 2: acquireLocked's recovery of an unverified candidate,
        // reached via refreshed(). The ambiguous write LANDS for real so a
        // reference lease genuinely exists to mismatch against.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore ambiguous = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        TestClock clock = new TestClock();
        LeaseManager m = manager(ambiguous, "podA", clock);
        assertThatThrownBy(m::tryAcquire)
                .as("the cold-start write landed but its response was lost")
                .isInstanceOf(IOException.class);
        Lease mine = new Lease(1, "podA", "", clock.millis() + TTL.toMillis());

        if (which == MismatchedConjunct.EPOCH) {
            clock.advance(TTL);
            assertThat(manager(backing, "podA", clock).tryAcquire()).isPresent();
        } else {
            mismatchOneConjunct(backing, m.key(), mine, which, clock);
        }

        assertThat(m.tryAcquire())
                .as("recovery must not adopt a candidate that is not there")
                .isEmpty();
        assertThat(m.held())
                .as(which + " alone must discriminate -- refreshed() must refuse to recover")
                .isEmpty();
    }

    @ParameterizedTest
    @EnumSource(MismatchedConjunct.class)
    void renewRecoveryRefusesASingleMismatchedConjunct(MismatchedConjunct which) throws Exception {
        // ⚠️ SITE 3: renewLocked's refreshed() call, reached when the belief
        // is ambiguous from a landed-but-lost-response renew.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore ambiguous = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_MATCH);
        TestClock clock = new TestClock();
        LeaseManager m = manager(ambiguous, "podA", clock);
        Lease mine = m.tryAcquire().orElseThrow();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew)
                .as("the renew's response is lost, though its write landed")
                .isInstanceOf(IOException.class);
        // ⚠️ m's belief is now ambiguous, at the RENEWED expiry the landed
        // write actually wrote -- not `mine`'s original one.
        Lease renewed = mine.renewedUntil(clock.millis() + TTL.toMillis());

        clock.advance(TTL);
        if (which == MismatchedConjunct.EPOCH) {
            assertThat(manager(backing, "podA", clock).tryAcquire()).isPresent();
        } else {
            mismatchOneConjunct(backing, m.key(), renewed, which, clock);
        }

        assertThat(m.renew())
                .as(which + " alone must discriminate -- the stale belief must be fenced")
                .isEmpty();
        assertThat(m.held()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(MismatchedConjunct.class)
    void releaseRecoveryRefusesASingleMismatchedConjunct(MismatchedConjunct which)
            throws Exception {
        // ⚠️ SITE 4: releaseLocked's refreshed() call -- the site M4.3d
        // widened scope to reach and M4.3g's review found unreached again.
        // ⚠️ THE ASSERTION THAT MATTERS is not merely "release does not
        // throw": a broken isOwnTerm here would let release ADOPT the OTHER
        // holder's live lease as its own and then EXPIRE it -- so the
        // discriminator is that the store's lease is left untouched.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore ambiguous = new AmbiguousPutStore(backing,
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_MATCH);
        TestClock clock = new TestClock();
        LeaseManager m = manager(ambiguous, "podA", clock);
        Lease mine = m.tryAcquire().orElseThrow();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew).isInstanceOf(IOException.class);
        Lease renewed = mine.renewedUntil(clock.millis() + TTL.toMillis());

        clock.advance(TTL);
        Lease liveHolder;
        if (which == MismatchedConjunct.EPOCH) {
            liveHolder = manager(backing, "podA", clock).tryAcquire().orElseThrow();
        } else {
            mismatchOneConjunct(backing, m.key(), renewed, which, clock);
            liveHolder = Lease.decode(backing.get(m.key()).readAllBytes());
        }

        m.release();

        assertThat(Lease.decode(backing.get(m.key()).readAllBytes()))
                .as(which + " alone must discriminate -- release must not touch a "
                        + "term that was never m's to give up")
                .isEqualTo(liveHolder);
    }
}
