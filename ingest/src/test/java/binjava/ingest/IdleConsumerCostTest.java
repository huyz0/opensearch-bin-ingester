// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.OpType;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Zero object-store requests attributable to idle consumers, at fan-out
 * (M5.19, NFR-2, M5 SPEC criterion 8).
 *
 * <p>⚠️ IT IS PROVEN HERE FOR THE FIRST TIME, NOT RE-PROVEN. M5's SPEC records
 * that the M1 baseline was never established: the property held BY
 * CONSTRUCTION, because no class on the consumer path held a {@code BinStore}
 * at all, so no consumer test could fail. A green run that repeats that shape
 * is a non-proof, which is why every case here names the mutation it dies
 * under and why {@code SegmentPublisher} is driven on the SAME loop shape the
 * production flush loop uses.
 *
 * <p>⚠️ TWO NUMBERS, NOT ONE. The store's own meter cannot see a grant: a
 * presigned URL is a signature rather than a request, and the GET the consumer
 * then makes happens in another process entirely. So criterion 8 asserts the
 * ingester's {@code StoreCounts} AND {@link GrantIssuer#grantsIssued()}, and
 * neither alone is the property. ⚠️ Giving the consumer a {@code BinStore} to
 * count instead is the regression ADR-0023 exists to prevent.
 *
 * <p>⚠️ THE IDLE CASE READS THE CLOCK ZERO TIMES, AND THAT IS ASSERTED RATHER
 * THAN GLOSSED. Criterion 8 requires the clock to be READ or the case is the
 * tautology M1.16b was withdrawn for, and the honest answer is that an idle pod
 * does not even look at the time: {@code Accumulator.isFlushDue} returns at its
 * empty guard BEFORE {@code clock.millis()}. So the zero is asserted as a count
 * of PRODUCTION reads through a counting clock -- a polling design reads once
 * per interval per pod and fails it -- while criterion 8's "the clock is
 * exercised" half is discharged by the fan-out case below, where production
 * reads it on every round and that count is asserted too.
 *
 * <p>⚠️ THE RED CRITERION 8 DEMANDS TAKES TWO SITES TO REACH THE REQUEST
 * COUNT, and one to be caught at all. Deleting {@code Accumulator.isFlushDue}'s
 * empty guard alone is enough to red this file -- on the CLOCK READS, which go
 * from 0 to 3,000, the signature of a design that polls. It is NOT enough to
 * move the request total, because {@code drain()} has a guard of its own and
 * returns nothing to PUT; both guards go for that, and the loop then PUTs once
 * per interval -- 3,000 requests for an idle cluster. An earlier draft of this
 * paragraph said one site left the suite green, which stopped being true when
 * the clock assertion landed in the same commit.
 */
class IdleConsumerCostTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    /** Criterion 8's floor is 1,000 consumers; ⚠️ over 1,000 DISTINCT streams. */
    private static final int CONSUMERS = 1_000;

    /** Criterion 8's floor is 3,000 intervals. */
    private static final int INTERVALS = 3_000;

    /** ⚠️ Enough intervals for the active stream to flush several times. */
    private static final int ROUNDS = 100;

    /** ⚠️ ADVANCED, never slept on: 3,000 real intervals cannot fit L0's budget. */
    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;
        private long reads;

        @Override
        public long millis() {
            reads++;
            return millis;
        }

        /**
         * ⚠️ HOW MANY TIMES PRODUCTION ASKED THE TIME, which is the difference
         * between exercising the clock and merely holding one. A poll design
         * reads it once per interval per pod; the idle case below reads it
         * ZERO times, and the traffic case thousands.
         */
        long reads() {
            return reads;
        }

        void advance(Duration d) {
            millis += d.toMillis();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    private static List<AutoCloseable> subscribeAll(SubscriptionHub hub, int consumers,
            AtomicLong delivered) {
        return subscribeAll(hub, consumers, delivered, false);
    }

    /**
     * @param onOneKey true subscribes every consumer to the ACTIVE stream
     *     instead of one idle stream each. ⚠️ Which of the two a case wants is
     *     not a detail: with a consumer per idle stream the serving path sees
     *     ONE target however many are registered, so anything counted per
     *     TARGET is flat whatever the implementation does -- round 2 measured
     *     a per-target grant mint surviving all four cases that way.
     */
    private static List<AutoCloseable> subscribeAll(SubscriptionHub hub, int consumers,
            AtomicLong delivered, boolean onOneKey) {
        List<AutoCloseable> handles = new ArrayList<>(consumers);
        for (int i = 0; i < consumers; i++) {
            handles.add(hub.subscribe(new RunKey(INDEX, onOneKey ? 0 : i),
                    SubscriptionHub.assembling(push -> delivered.incrementAndGet())));
        }
        return handles;
    }

    private static void closeAll(List<AutoCloseable> handles) throws Exception {
        for (AutoCloseable handle : handles) {
            handle.close();
        }
    }

    /**
     * ⚠️ THE PRODUCTION FLUSH LOOP'S SHAPE, not a hand-fed flush.
     * {@code DefaultIngest.flushLoop} asks {@code accumulator.isFlushDue()} on
     * every wake-up and publishes only when it says yes; this drives the same
     * two calls over simulated time. A loop that published unconditionally
     * would measure the test rather than the trigger.
     */
    private static void tick(Accumulator accumulator, SegmentPublisher publisher,
            TestClock clock, Duration interval) throws Exception {
        clock.advance(interval);
        if (accumulator.isFlushDue()) {
            publisher.publish(accumulator);
        }
    }

    @Test
    @Timeout(60)
    void aTHOUSANDIdleConsumersOverTHREETHOUSANDIntervalsCostZEROOfBothNumbers()
            throws Exception {
        TestClock clock = new TestClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");
        Accumulator accumulator = new Accumulator(config, clock);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        // ⚠️ A PRESIGN-CAPABLE STORE, because `MemoryBinStore` refuses at
        // construction (ADR-0041) -- the grant counter cannot be asserted at
        // zero by a deployment that could not have issued one anyway.
        GrantIssuer grants = new GrantIssuer(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        AtomicLong delivered = new AtomicLong();
        List<AutoCloseable> handles = subscribeAll(hub, CONSUMERS, delivered);
        long before = store.counts().total();

        for (int interval = 0; interval < INTERVALS; interval++) {
            tick(accumulator, publisher, clock, config.intervalFloor());
        }

        assertThat(store.counts().total() - before)
                .as("%d consumers idle across %d intervals -- a design that polled, or a "
                        + "timer that fired on an empty stream, costs one request per "
                        + "interval per pod forever, and NFR-2 is the whole reason this "
                        + "service is not Kafka", CONSUMERS, INTERVALS)
                .isZero();
        assertThat(grants.grantsIssued())
                .as("the only request an idle consumer could cause is the GET a grant "
                        + "authorises, and no meter in this process can see that one")
                .isZero();
        // ⚠️ THIS ZERO IS BY CONSTRUCTION AND IS NOT THE PROPERTY ON ITS OWN:
        // with no traffic nothing is published, so no serving path runs and an
        // issuer that minted per idle consumer would still read zero here.
        // `theGRANTCountIsFLATInTheCONSUMERCount` is what holds that, by
        // running real traffic at both fan-outs. Said plainly rather than left
        // for a reader to discover, because M1's withdrawn proof had exactly
        // this shape.
        assertThat(delivered.get())
                .as("nothing was published, so nothing was delivered -- stated so that a "
                        + "future reader does not read the zero above as a hub that stopped "
                        + "working")
                .isZero();
        assertThat(clock.reads())
                .as("an idle pod does not even ask the time: %d intervals and production "
                        + "never reached a clock read, because the empty guard is BEFORE it. "
                        + "A polling design reads once per interval and lands at %d",
                        INTERVALS, INTERVALS)
                .isZero();
    }

    /**
     * The grant count is FLAT in the idle consumer count (M5.19, round 1's
     * major).
     *
     * <p>⚠️ THE ZERO IN THE IDLE CASE IS BY CONSTRUCTION, and review named it:
     * nothing publishes there, so an issuer minting per idle consumer would
     * still read zero. This runs REAL traffic under {@code direct} -- the only
     * mode that mints -- at one subscriber and at 1,000.
     *
     * <p>⚠️ ALL 1,000 SUBSCRIBE TO THE COMMITTED STREAM, and round 2 measured
     * why that is not a detail. With a consumer per IDLE stream the serving
     * path sees one target whatever is registered, so moving the mint inside
     * the per-target loop left all four cases here green and only
     * {@code DirectServingTest} caught it -- the vacuity of round 1 moved
     * rather than went away. One grant per SEGMENT is the property
     * {@code GrantIssuer} states; per consumer at fan-out is one signature per
     * consumer for one object, invisible to every meter in this process
     * because signing issues no request.
     */
    @Test
    @Timeout(60)
    void theGRANTCountIsFLATInTheCONSUMERCount() throws Exception {
        long alone = grantsForOneActiveStream(1);
        long atFanOut = grantsForOneActiveStream(CONSUMERS);

        assertThat(atFanOut)
                .as("the same traffic at 1 and at %d subscribers OF THE SAME STREAM -- a "
                        + "grant count that moved with the subscriber count is one "
                        + "consumer-side GET per consumer for one object, off-meter and "
                        + "unbounded by anything cost.md counts", CONSUMERS)
                .isEqualTo(alone);
        assertThat(alone)
                .as("PREMISE: grants really were minted, or both sides are zero and the "
                        + "equality means nothing")
                .isPositive();
    }

    private static long grantsForOneActiveStream(int consumers) throws Exception {
        TestClock clock = new TestClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");
        Accumulator accumulator = new Accumulator(config, clock);
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        SubscriptionHub hub = new SubscriptionHub();
        GrantIssuer grants = new GrantIssuer(store);
        AtomicLong delivered = new AtomicLong();
        List<AutoCloseable> handles = subscribeAll(hub, consumers, delivered, true);
        // ⚠️ DIRECT AT BOTH FAN-OUTS, which takes a threshold of CONSUMERS
        // rather than of 1. `FetchPolicy` answers DIRECT only while the
        // segment fan-out is AT OR BELOW the threshold -- above it, PROXY is
        // the cheaper answer and the policy is right to give it -- so a
        // threshold of 1 made the 1,000-subscriber arm mint nothing and the
        // comparison read 100 against 0. The threshold is config, and pinning
        // it here is what keeps the MODE fixed so the GRANT COUNT is what
        // varies.
        SegmentServing serving = new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L, 1L, CONSUMERS, true)),
                store.capabilities(), new SegmentProxy(store), grants);

        driveOneActiveStream(accumulator, publisher, hub, clock, config, serving, true);

        closeAll(handles);
        return grants.grantsIssued();
    }

    /**
     * The grant counter counts what it mints (M5.19).
     *
     * <p>⚠️ THE ZERO ABOVE DOES NOT PIN IT. A counter that never increments
     * reads zero in exactly the case criterion 8 asserts, so the idle case
     * alone is green against a {@code grantsIssued()} hard-coded to return 0 --
     * which is the second number of the criterion made decorative.
     */
    @Test
    void theGRANTCounterCountsWHATItMints() throws Exception {
        GrantIssuer issuer = new GrantIssuer(new StoreFakes.CanPresign());

        issuer.grantFor("bins/cluster-a/seg-1");
        issuer.grantFor("bins/cluster-a/seg-2");

        assertThat(issuer.grantsIssued())
                .as("two grants, two consumer-side GETs authorised -- and no meter in this "
                        + "process can see either, which is why this number exists")
                .isEqualTo(2);
    }

    /**
     * An idle fan-out adds NOTHING to an active stream's cost (M5.19).
     *
     * <p>⚠️ THE ABSOLUTE ZERO ABOVE IS BLIND TO A PER-SUBSCRIBER COST, because
     * with no traffic nothing is published and no serving path runs at all.
     * This one runs the same traffic twice -- once with ONE subscriber, once
     * with 1,000 of which 999 are idle -- and requires the request total AND
     * the delivery count to be equal.
     *
     * <p>⚠️ THE DELIVERY COUNT IS THE HALF THAT BITES. Under {@code inline}
     * the bytes are in this pod's hand, so even a serving path that fanned out
     * to every registered subscriber would buy no request and the totals alone
     * would stay equal -- MEASURED: with {@code subscribersFor} returning every
     * subscriber whatever key it asks about, the request totals are still
     * identical. What changes is that 1,000 consumers are handed a window
     * belonging to a stream they never subscribed to, which under {@code
     * proxy} or {@code direct} is 999 reads or 999 grants. So the case asserts
     * the deliveries too, and that is what the mutation dies under.
     */
    @Test
    @Timeout(60)
    void anIDLEFanOutAddsNOTHINGToAnACTIVEStreamsRequestCountOrItsDeliveries()
            throws Exception {
        Run alone = runOneActiveStream(1);
        Run atFanOut = runOneActiveStream(CONSUMERS);

        assertThat(atFanOut.requests())
                .as("the same traffic, once with ONE subscriber and once with %d -- a request "
                        + "count that moved with the subscriber count is the fetch-per-"
                        + "consumer design ADR-0004 rejected at $3,732 a month against $25",
                        CONSUMERS)
                .isEqualTo(alone.requests());
        assertThat(atFanOut.delivered())
                .as("only the ONE subscriber of the committed run is delivered to; the other "
                        + "%d asked for streams that did not move. Waking them costs nothing "
                        + "under inline and 999 reads under proxy", CONSUMERS - 1)
                .isEqualTo(alone.delivered());
        assertThat(alone.requests())
                .as("PREMISE: the active stream really did cost requests, or both sides are "
                        + "zero and the equality means nothing")
                .isPositive();
    }

    /** What one traffic run cost, in the two numbers criterion 8 is about. */
    private record Run(long requests, long delivered) {
    }

    /**
     * Runs the SAME traffic with {@code consumers} subscribers.
     *
     * <p>⚠️ IT PUBLISHES THROUGH THE HUB, not merely through
     * {@link SegmentPublisher}. An earlier draft stopped at the PUT, so the
     * serving path never ran and the comparison could not have seen a
     * per-subscriber cost at all -- the defect it exists to catch.
     */
    private static Run runOneActiveStream(int consumers) throws Exception {
        TestClock clock = new TestClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");
        Accumulator accumulator = new Accumulator(config, clock);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        SubscriptionHub hub = new SubscriptionHub();
        AtomicLong delivered = new AtomicLong();
        List<AutoCloseable> handles = subscribeAll(hub, consumers, delivered);
        // ⚠️ INLINE FOREVER, by CONFIG rather than by size: the bytes this pod
        // is holding are what the subscriber is given, which is the whole of
        // ADR-0004's zero. A threshold here would make the case depend on a
        // record length nobody chose deliberately.
        SegmentServing serving = new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(
                        Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                store.capabilities(), new SegmentProxy(store));
        long before = store.counts().total();

        driveOneActiveStream(accumulator, publisher, hub, clock, config, serving, false);

        assertThat(delivered.get())
                .as("PREMISE: the one subscribed stream really was delivered to, or a count "
                        + "that did not move proves nothing")
                .isPositive();
        closeAll(handles);
        return new Run(store.counts().total() - before, delivered.get());
    }

    /**
     * ⚠️ ONE ACTIVE STREAM AND THE REST IDLE, and the active one IS subscribed
     * -- partition 0 is the first key handed to {@code subscribe}. An active
     * stream nobody subscribes to would make every comparison here blind,
     * because nothing would be served at all.
     *
     * <p>⚠️ IT IS THE PRODUCTION FLUSH LOOP'S SHAPE: ask
     * {@code accumulator.isFlushDue()} on every wake-up and publish only when
     * it says yes. A loop that published unconditionally would measure the
     * test rather than the trigger -- and the clock is READ on every round,
     * through {@code add} and through the trigger, which is criterion 8's
     * "exercised" half.
     */
    private static void driveOneActiveStream(Accumulator accumulator,
            SegmentPublisher publisher, SubscriptionHub hub, TestClock clock,
            IngestConfig config, SegmentServing serving, boolean cold) throws Exception {
        RunKey active = new RunKey(INDEX, 0);
        long readsBefore = clock.reads();
        long sequence = 1;
        for (int round = 0; round < ROUNDS; round++) {
            accumulator.add(active, new SegmentRecord("doc-" + round, OpType.INDEX,
                    OptionalLong.of(1), new byte[64]));
            clock.advance(config.intervalFloor());
            if (accumulator.isFlushDue()) {
                var published = publisher.publish(accumulator).orElseThrow();
                List<RunCommit> runs = new ArrayList<>();
                published.recordCounts().forEach(
                        (key, count) -> runs.add(new RunCommit(key, count, 0L)));
                // ⚠️ `cold` MEANS ANOTHER POD WROTE IT: the held key then does
                // not match the committed one, which is the only state in which
                // the policy answers anything but INLINE -- this pod holding
                // the bytes is never a reason to buy a request for them.
                hub.publish(new CommitDelta(sequence++, published.key(), runs),
                        cold ? "seg-written-by-another-pod" : published.key(),
                        published.segment(), serving);
            }
        }
        assertThat(clock.reads() - readsBefore)
                .as("PREMISE: production really read the injected clock on this path -- "
                        + "criterion 8 calls a case that does not the tautology M1.16b was "
                        + "withdrawn for")
                .isGreaterThanOrEqualTo(ROUNDS);
    }
}
