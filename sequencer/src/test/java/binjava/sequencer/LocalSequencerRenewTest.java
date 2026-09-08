// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Lease;
import binjava.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A healthy leader keeps its term (M4.17).
 *
 * <p>⚠️ NOTHING RENEWED THE LEASE. {@code LocalSequencer.start} acquired it and
 * never called {@link LeaseManager#renew()} again, while {@code DefaultIngest}
 * holds the sequencer for its whole lifetime — so every pod lost its lease at
 * each TTL and the cluster failed over for no reason at all. {@code renew} and
 * its tests have existed since M4.3b; what was missing is the TICK that calls
 * it, which no task owned until this one.
 *
 * <p>⚠️ LIVENESS, NOT SAFETY, and the distinction is why this was survivable:
 * a leader that silently lapses is exactly the fenced leader the seal protocol
 * already handles. The cost is an unnecessary failover every TTL per pod, and a
 * BURNED EPOCH each time — and burned epochs are what M4.16 shows turning into
 * an I2 violation, so this row and that one compound.
 *
 * <p>⚠️ THE TICK IS A SEAM for the reason {@code BatchingSequencer}'s window is:
 * a test that slept would be asserting "renewed at roughly the right time", and
 * testing.md forbids the sleep that would buy it. Production sleeps the renew
 * interval; a test releases exactly the ticks it means to observe.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerRenewTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    private static final String PREFIX = "bins/cluster-a";

    /** A clock the test advances, so a TTL can expire without wall-clock time. */
    private static final class TestClock extends Clock {
        private volatile long millis = 1_000_000L;

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

    /**
     * A tick the test releases by hand.
     *
     * <p>⚠️ THE TEST WAITS ON THE OBSERVABLE, not on this. Releasing a permit
     * says only that the renewer MAY proceed; whether it renewed is read back
     * from the lease in the store, under a deadline, so a tick that silently
     * did nothing fails loudly rather than passing.
     */
    private static final class ManualTicker implements LocalSequencer.RenewTicker {
        private final Semaphore due = new Semaphore(0);
        private final Semaphore parked = new Semaphore(0);

        @Override public void awaitNextRenew() throws InterruptedException {
            // ⚠️ RELEASED BEFORE THE PARK, so a permit here means the renewer
            // has finished whatever the PREVIOUS tick asked of it and is back
            // waiting. That is the only edge a test can wait on without
            // reaching into production state.
            parked.release();
            due.acquire();
        }

        void tick() {
            due.release();
        }

        /** Blocks until the renewer is parked waiting for a tick. */
        void awaitParked() throws InterruptedException {
            assertThat(parked.tryAcquire(20, TimeUnit.SECONDS))
                    .as("the renewer reached its tick").isTrue();
            parked.release();
        }

        /** Releases one tick and returns once the renewer has processed it. */
        void tickAndAwaitProcessed() throws InterruptedException {
            assertThat(parked.tryAcquire(5, TimeUnit.SECONDS))
                    .as("the renewer is waiting for a tick").isTrue();
            // ⚠️ STALE PERMITS GO FIRST. `parked` accumulates one per completed
            // loop, so after any earlier `tick()` the second acquire below would
            // take a leftover permit and return BEFORE the renewer had processed
            // this tick -- degrading this method into the `Thread.sleep(50)` it
            // replaced, silently. ⚠️ WHAT THAT COSTS, measured, is not that the
            // fence-on-IOException mutation survives: it still dies, but at
            // `awaitExpiryPast`'s twenty-second deadline instead of at the
            // `commit` two lines below, so the assertion that names the defect
            // stops being the one that catches it. An earlier draft of this
            // comment claimed survival, which the tree does not do.
            parked.drainPermits();
            due.release();
            assertThat(parked.tryAcquire(5, TimeUnit.SECONDS))
                    .as("the renewer processed the tick and came back round").isTrue();
            parked.release();
        }
    }

    private static LeaseManager manager(BinStore store, String podId, Clock clock) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW), clock);
    }

    /** ⚠️ A renew interval short enough for the REAL ticker to fire inside a test. */
    private static LeaseManager briskManager(BinStore store, String podId, Clock clock) {
        return new LeaseManager(store,
                new LeaseConfig(PREFIX, podId, "", TTL, Duration.ofMillis(150)), clock);
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(A, 0), n);
        return m;
    }

    private static CommitRequest request(String pod, long flushSeq, String seg, int n) {
        return new CommitRequest(pod, "i1", flushSeq, seg, counts(n));
    }

    private static long leaseExpiry(BinStore store) throws IOException {
        String key = new LeaseConfig(PREFIX, "pod1", "", TTL, RENEW).leaseKey();
        try (InputStream in = store.get(key)) {
            return Lease.decode(in.readAllBytes()).expiresAtMillis();
        }
    }

    /** Polls until the stored lease's expiry moves past {@code was}, or fails. */
    private static long awaitExpiryPast(BinStore store, long was, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        long now = leaseExpiry(store);
        while (now <= was && System.nanoTime() < deadline) {
            Thread.sleep(2);
            now = leaseExpiry(store);
        }
        assertThat(now).as(what).isGreaterThan(was);
        return now;
    }

    @Test
    void aHealthyLeaderRENEWSSoItsTermDoesNotLapse() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        TestClock clock = new TestClock();
        ManualTicker ticker = new ManualTicker();
        LocalSequencer seq = LocalSequencer.start(
                store, PREFIX, manager(store, "pod1", clock), 8, ticker).orElseThrow();

        long acquired = leaseExpiry(store);
        long afterStart = store.counts().puts();
        // ⚠️ THE CLOCK MOVES, because a renew that wrote the SAME expiry would
        // be indistinguishable from one that never happened. The lease is
        // stamped `now + ttl`, so the assertion is only meaningful once `now`
        // has advanced.
        clock.millis += RENEW.toMillis();
        ticker.tick();

        long afterFirst = awaitExpiryPast(store, acquired, "the first tick extends the term");
        clock.millis += RENEW.toMillis();
        ticker.tick();

        // ⚠️ A SECOND TICK, because a renewer that renews once and exits leaves
        // the pod losing its lease at the next TTL — the very defect this task
        // exists to close, one tick further out.
        awaitExpiryPast(store, afterFirst, "and it keeps renewing, tick after tick");

        // ⚠️ WHAT A TICK COSTS, not merely that it happened. Two extra
        // `leases.renew()` calls per tick -- three lease PUTs where one is owed
        // -- survived every other assertion here, and a renew is a conditional
        // write whose rate ADR-0008 budgets at ~0.33/s for the whole cluster.
        assertThat(store.counts().puts() - afterStart)
                .as("two ticks, two lease writes -- a tick is ONE renew")
                .isEqualTo(2L);
        seq.close();
    }

    @Test
    void aLeaderThatLEARNSItIsFencedSTOPSSequencingWITHOUTTouchingTheStore() throws Exception {
        // ⚠️ THE ASSERTION IS ZERO REQUESTS, and a first draft of this test
        // asserted only that the commit threw "fenced" -- which PASSED against
        // a tree with no renewer at all. `CommitLog` already refuses to write
        // into a sealed chain with exactly that word, so the test measured the
        // SEAL protocol and said nothing about this task. The difference that
        // matters is WHERE the refusal comes from: a leader that has learned it
        // is fenced must stop on its own, before the store, rather than
        // discovering it one round trip later.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        TestClock clock = new TestClock();
        ManualTicker ticker = new ManualTicker();
        LocalSequencer seq = LocalSequencer.start(
                store, PREFIX, manager(store, "pod1", clock), 8, ticker).orElseThrow();
        seq.commit(request("pod1", 1, "seg/0", 3));

        // ⚠️ A REAL TAKEOVER, not a stubbed empty: the lease lapses on the
        // shared clock and a second pod acquires it, which is how a fenced
        // leader comes about in production.
        clock.millis += TTL.toMillis() + 1;
        LocalSequencer successor = LocalSequencer.start(
                store, PREFIX, manager(store, "pod2", clock), 8, new ManualTicker()).orElseThrow();
        assertThat(successor.epoch()).as("the successor took the term").isEqualTo(2);

        ticker.tick();

        // ⚠️ `renew` RETURNING EMPTY IS HOW A HOLDER LEARNS IT IS FENCED -- its
        // own javadoc says so, and says the holder must then stop sequencing
        // immediately. A tick that renewed and IGNORED the answer would be
        // worse than no tick: the old leader keeps committing into a chain a
        // successor has already sealed, reassigning offsets it has issued. I2.
        long giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Throwable refusal = null;
        long spent = -1;
        while (System.nanoTime() < giveUpAt) {
            long before = store.counts().total();
            try {
                seq.commit(request("pod1", 2, "seg/1", 3));
            } catch (Throwable failed) {
                spent = store.counts().total() - before;
                if (spent == 0) {
                    refusal = failed;
                    break;
                }
            }
            Thread.sleep(2);
        }

        assertThat(refusal)
                .as("a fenced leader refuses locally, spending no request; last attempt cost "
                        + spent)
                .isNotNull();
        assertThat(refusal)
                .as("and it says the LEASE went, not merely that the chain was sealed")
                .hasMessageContaining("lease");
        successor.close();
    }

    @Test
    void aStoreFAILUREDuringRenewDoesNotFenceAHealthyLeader() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        String leaseKey = new LeaseConfig(PREFIX, "pod1", "", TTL, RENEW).leaseKey();
        FailPutIfMatchStore store = new FailPutIfMatchStore(backing, leaseKey);
        TestClock clock = new TestClock();
        ManualTicker ticker = new ManualTicker();
        LocalSequencer seq = LocalSequencer.start(
                store, PREFIX, manager(store, "pod1", clock), 8, ticker).orElseThrow();

        // ⚠️ AN IOException IS NOT AN EMPTY RESULT, and `renew`'s contract is
        // explicit that the two differ: empty means FENCED, an IOException
        // means the store was unreachable and the term is untouched. A renewer
        // that fenced on either would hand the cluster a failover every time
        // the store hiccuped -- turning a transient blip into the very outage
        // this task removes.
        // ⚠️ A RENDEZVOUS, NOT A SLEEP. An earlier draft used
        // `Thread.sleep(50)` here and testing.md rule 15 forbids exactly that:
        // measured, deleting it still killed the fence-on-IOException mutant,
        // but only via the 20-second deadline further down -- so the commit
        // below was asserting nothing whenever the 50ms window was lost, and a
        // transient flag cleared on the next successful renew would survive.
        // The ticker now reports when the renewer has come back round, so the
        // failed renew is known to have been processed before the commit runs.
        store.startFailing();
        clock.millis += RENEW.toMillis();
        ticker.tickAndAwaitProcessed();

        seq.commit(request("pod1", 1, "seg/0", 3));

        // ⚠️ AND IT RENEWS AGAIN once the store recovers, which is the half a
        // permanently-failing store could not show: the renewer must still be
        // alive, not merely have declined to fence.
        long before = leaseExpiry(backing);
        store.stopFailing();
        clock.millis += RENEW.toMillis();
        ticker.tick();

        awaitExpiryPast(backing, before, "the renewer survived the failure and renewed after it");
        seq.close();
    }

    @Test
    void theSHIPPINGTickerWaitsTheCONFIGUREDRenewIntervalNotSomeOtherOne() throws Exception {
        // ⚠️ THE SHIPPING TICKER RAN IN NO TEST, which is verbatim the defect
        // the sibling seam already had and fixed. Every test here injects its
        // own ticker through the 5-arg `start`; production takes the 4-arg one.
        // Measured surviving mutation: `sleepFor(Duration.ofDays(1))` left the
        // WHOLE BUILD green -- a leader renewing once a day against a
        // ten-second TTL, i.e. exactly the defect M4.17 exists to remove,
        // reinstated with the fix in place.
        // ⚠️ BOUNDED ON BOTH SIDES, because a floor alone is met by any timer
        // that waits LONGER than asked -- including the once-a-day one -- and a
        // ceiling alone by one that never waits. A sleep may overrun, never
        // undershoot, so the floor cannot flake; the ceiling has ~30x slack.
        long before = System.nanoTime();
        LocalSequencer.sleepFor(Duration.ofMillis(120)).awaitNextRenew();
        long elapsed = System.nanoTime() - before;

        assertThat(elapsed)
                .as("it waits at least the interval it was given")
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(120));
        assertThat(elapsed)
                .as("and THAT interval, not a multiple of it or a constant")
                .isLessThan(TimeUnit.SECONDS.toNanos(4));
    }

    @Test
    void thePUBLICStartWIRESTheConfiguredIntervalIntoTheTickerItInstalls() throws Exception {
        // ⚠️ THE SEAM AND THE WIRING ARE DIFFERENT CLAIMS, and pinning the first
        // is what the test above does. Making `sleepFor` package-private let a
        // test call it directly -- and a direct call never builds a
        // `LeaseConfig`, so `sleepFor(leases.renewInterval())` could become
        // `sleepFor(Duration.ofDays(1))` with the WHOLE BUILD green. Measured by
        // review: 2m02s, failures=0 in every result XML, and the seam test
        // itself ran and passed under the mutant. That is verbatim the
        // overstatement `baselines/review.txt` already records against
        // `BatchingSequencer` -- "both its bounds bypass the constructor" --
        // reached again one class over.
        // ⚠️ SO THIS GOES THROUGH THE PUBLIC 4-ARG `start`, the one production
        // uses, with a renew interval short enough for the real ticker to fire.
        // `LeaseConfig` accepts 150ms: only zero, negative and >= ttl are
        // refused. A once-a-day ticker, or one scaled off the configured value,
        // simply never renews inside the deadline.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        TestClock clock = new TestClock();
        LocalSequencer seq = LocalSequencer.start(
                store, PREFIX, briskManager(store, "pod1", clock), 8).orElseThrow();
        try {
            long acquired = leaseExpiry(store);
            // ⚠️ The clock must move or a renew writes the same expiry it read.
            clock.millis += 500;

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            long now = leaseExpiry(store);
            while (now <= acquired && System.nanoTime() < deadline) {
                Thread.sleep(5);
                now = leaseExpiry(store);
            }
            assertThat(now)
                    .as("the ticker production installs renews on the CONFIGURED interval")
                    .isGreaterThan(acquired);

            // ⚠️ AND AT THE CONFIGURED RATE, not merely at some rate. A ticker
            // scaled DOWN passes everything above -- the lease still gets
            // renewed, just far too often. Measured surviving mutation:
            // `sleepFor(leases.renewInterval().dividedBy(100))` is a 100x lease
            // PUT rate against ADR-0008's budgeted ~0.33/s for the whole
            // cluster, which is a cost defect wearing a liveness fix's clothes.
            // ⚠️ THE SLEEP HERE IS THE MEASUREMENT WINDOW, not synchronisation:
            // a rate needs an interval to be a rate at all, and nothing is
            // being waited FOR.
            // ⚠️ THE MARGINS ARE NOT SYMMETRIC, and an earlier draft of this
            // comment said they were. Honest is ~7/s against a floor of 3 and a
            // ceiling of 30: the `dividedBy(100)` mutant at ~583 dies by two
            // orders of magnitude, but `multipliedBy(4)` lands at 2 and dies by
            // a factor of ~2.3. The FLOOR is the tight side, and the second
            // sequencer below is what makes that case decisive rather than
            // marginal.
            // ⚠️ THE FLOOR IS 4, NOT 3, and the extra unit is load-bearing:
            // `multipliedBy(2)` measures EXACTLY 3 and so survived an inclusive
            // floor of 3, leaving a whole surviving band of roughly 0.56x to
            // 2.22x. That is not harmless -- `LeaseConfig` refuses only
            // `renewInterval >= ttl`, so a 9s/10s configuration doubled to 18s
            // is M4.17's own defect reinstated. Honest is 6-7 even at load
            // average 124, so a floor of 4 keeps ~1.75x slack.
            // ⚠️ A FLOOR AS WELL AS A CEILING, and a SECOND sequencer at a
            // different interval, because one-sided bounds let two mutations
            // through -- both measured. `interval.multipliedBy(4)` renews at
            // 600ms, which satisfies a 20s liveness deadline and a 30/s
            // ceiling; at this class's 3s/10s configuration that same 4x puts
            // the renew at 12s against a 10s TTL, i.e. M4.17's own defect
            // reinstated with the fix in place. And a constant
            // `Thread.sleep(200)` ignores the argument entirely, at 5 lease
            // PUTs/s against ADR-0008's ~0.33/s budget for the whole cluster.
            // ⚠️ THE SECOND SEQUENCER IS WHAT SEPARATES THEM: it runs a 600ms
            // interval in the SAME window on its OWN store, so a correct ticker
            // shows ~7/s here and ~1/s there, while a 4x mutant collapses this
            // one to ~1/s and a constant lifts that one to ~5/s. Neither can
            // satisfy both bounds at once.
            CountingBinStore slowStore = new CountingBinStore(new MemoryBinStore());
            LeaseManager slowLeases = new LeaseManager(slowStore,
                    new LeaseConfig(PREFIX, "pod9", "", TTL, Duration.ofMillis(600)), clock);
            try (LocalSequencer slow =
                    LocalSequencer.start(slowStore, PREFIX, slowLeases, 8).orElseThrow()) {
                long fastBefore = store.counts().puts();
                long slowBefore = slowStore.counts().puts();
                // ⚠️ THE SLEEP IS THE MEASUREMENT WINDOW, not synchronisation:
                // a rate needs an interval to be a rate, and nothing is waited
                // FOR. Both sequencers are measured across the same one.
                Thread.sleep(1000);
                long fastRate = store.counts().puts() - fastBefore;
                long slowRate = slowStore.counts().puts() - slowBefore;

                assertThat(fastRate)
                        .as("a 150ms interval renews ~7 times a second -- not hundreds, "
                                + "and not once")
                        .isBetween(4L, 30L);
                assertThat(slowRate)
                        .as("and a 600ms interval renews ~1 -- so the interval is READ, "
                                + "not a constant")
                        .isLessThan(3L);
            }
        } finally {
            seq.close();
        }
    }


    @Test
    void aCallerAfterCLOSEIsToldCLOSEDRatherThanFENCED() throws Exception {
        // ⚠️ close() AND THE POST-TICK CHECK MASK EACH OTHER, so only removing
        // BOTH reaches any distinguishable state -- the interrupt covers a
        // renewer parked on the tick, the check covers one already past it.
        // Under that double mutation the renewer wakes after `release()`, gets
        // an empty renew, and sets `fenced` -- so a DELIBERATE shutdown is
        // reported to every later caller as a lost lease, which is exactly the
        // confusion the fenced/closed split exists to prevent and sends an
        // operator hunting for a takeover that never happened.
        // ⚠️ THIS IS THE FOURTH OBSERVABLE TRIED AND THE FIRST DETERMINISTIC
        // ONE. A resurrected lease never happens (`release()` nulls the belief,
        // so a late renew returns empty rather than writing). A fenced message
        // on the FIRST refusal is a race, because `closed` is set synchronously
        // and `fenced` arrives on the renewer's thread. A fenced message on
        // every refusal across a three-second poll could not be made to fail on
        // demand. What makes this one exact is that `fenced` is written BEFORE
        // the thread terminates, so joining it is a happens-before edge.
        // ⚠️ THE FIRST WAIT IS LOAD-BEARING: without it the loop header's own
        // `!closed` check retires the renewer before it ever reaches the tick,
        // and the test passes vacuously against the mutation.
        MemoryBinStore store = new MemoryBinStore();
        TestClock clock = new TestClock();
        ManualTicker ticker = new ManualTicker();
        LocalSequencer seq = LocalSequencer.start(
                store, PREFIX, manager(store, "pod1", clock), 8, ticker).orElseThrow();

        ticker.awaitParked();
        seq.close();
        ticker.tick();
        assertThat(seq.awaitRenewerStopped(20_000))
                .as("the renewer finished whatever close left it to do").isTrue();

        assertThatThrownBy(() -> seq.commit(request("pod1", 1, "seg/0", 3)))
                .isInstanceOf(IOException.class)
                .as("a deliberate shutdown is reported as a close")
                .hasMessageContaining("released its lease")
                .as("never as a fence, which would name a takeover that did not happen")
                .hasMessageNotContaining("fenced");
    }
}
