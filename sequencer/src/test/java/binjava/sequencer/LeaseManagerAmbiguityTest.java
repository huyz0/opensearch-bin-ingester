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
        return new LeaseManager(store, "bins/cluster-a", podId, "", TTL, RENEW, clock);
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
}
