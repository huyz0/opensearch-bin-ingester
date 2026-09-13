// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.sequencer.CommitLog;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** ⚠️ Push, not poll: an idle consumer must issue ZERO store requests (NFR-2). */
class SubscriptionHubTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static CommitDelta delta(long seq, RunKey key, int count, long first) {
        return new CommitDelta(seq, "seg-" + seq, List.of(new RunCommit(key, count, first)));
    }

    /**
     * The bytes every routing case publishes.
     *
     * <p>⚠️ THE SIZE DECIDES NOTHING: the policy below is inline-forever, so
     * these three bytes go {@code INLINE} because of the CONFIG and not because
     * of their length. An earlier draft of this comment said "small enough
     * that `FetchPolicy` answers INLINE", which would send a reader looking for
     * a threshold that is not consulted.
     */
    private static final byte[] HELD = {1, 2, 3};

    /**
     * Publishes on the PRODUCTION path with the segment in hand.
     *
     * <p>⚠️ THESE CASES USED THE ONE-ARGUMENT {@code publish}, which M5.45a
     * left without a production caller and which delivered every run with
     * {@code new byte[0]} -- a real subscriber loses the whole window, because
     * {@code ConsumerClient.decodeInto} opens {@code delivery.segment()} with
     * no fallback. The routing assertions cost nothing by moving: the held key
     * is the delta's only segment key at every call site, so {@code
     * publishSegment} takes the INLINE arm and never touches {@code
     * serving.proxy()}.
     *
     * <p>⚠️ THAT ZERO IS NOT ASSERTED HERE, and saying so is the honest
     * scope: the store below exists to supply {@code capabilities()}, and
     * nothing in this file counts its reads. {@code
     * AssembledServingPathTest.aSegmentUNDERTheInlineCapIsServedINLINEAndReadZEROTimes}
     * is what holds that property.
     */
    private static void publishHeld(SubscriptionHub hub, CommitDelta delta) {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        hub.publish(delta, delta.segmentKey(), HELD,
                new SegmentServing(
                        new FetchPolicy(new FetchPolicyConfig(
                                Long.MAX_VALUE, Long.MAX_VALUE, 1)),
                        store.capabilities(), new SegmentProxy(store)));
    }



    @Test
    void aSubscriberIsPushedItsOwnStreamsCommits() {
        SubscriptionHub hub = new SubscriptionHub();
        var received = new CopyOnWriteArrayList<SubscriptionHub.Push>();
        try (var ignored = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(received::add))) {
            publishHeld(hub, delta(0, new RunKey(A, 0), 3, 10));
            assertThat(received).singleElement().satisfies(p -> {
                assertThat(p.segmentKey()).isEqualTo("seg-0");
                assertThat(p.firstOffset()).isEqualTo(10);
                assertThat(p.recordCount()).isEqualTo(3);
                assertThat(p.lastOffset()).as("inclusive").isEqualTo(12);
            });
        }
    }

    @Test
    void aSubscriberIsNotWokenByOtherStreams() {
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger woken = new AtomicInteger();
        try (var ignored = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> woken.incrementAndGet()))) {
            publishHeld(hub, delta(0, new RunKey(B, 0), 1, 0));
            publishHeld(hub, delta(1, new RunKey(A, 7), 1, 0));
            // ⚠️ Fanning every delta to every subscriber makes wakeups scale
            // with TOTAL cluster traffic rather than the subscriber's own -- the
            // same defect shape as a request that scales with indices.
            assertThat(woken.get()).isZero();
            publishHeld(hub, delta(2, new RunKey(A, 0), 1, 0));
            assertThat(woken.get()).isEqualTo(1);
        }
    }

    @Test
    void everySubscriberOfAStreamGetsTheCommit() {
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger one = new AtomicInteger();
        AtomicInteger two = new AtomicInteger();
        try (var ignoredA = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> one.incrementAndGet()));
                var ignoredB = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> two.incrementAndGet()))) {
            publishHeld(hub, delta(0, new RunKey(A, 0), 1, 0));
            assertThat(one.get()).isEqualTo(1);
            assertThat(two.get()).isEqualTo(1);
        }
    }

    @Test
    void closingASubscriptionStopsDeliveryAndReleasesTheStream() {
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger woken = new AtomicInteger();
        var sub = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> woken.incrementAndGet()));
        publishHeld(hub, delta(0, new RunKey(A, 0), 1, 0));
        sub.close();
        publishHeld(hub, delta(1, new RunKey(A, 0), 1, 1));

        assertThat(woken.get()).as("only the first").isEqualTo(1);
        assertThat(hub.subscriberCount(new RunKey(A, 0))).isZero();
        // ⚠️ The empty list is dropped too. A cluster that churns through
        // short-lived consumers would otherwise keep one entry per stream ever
        // seen, forever.
        assertThat(hub.subscribedStreams()).isEmpty();
    }

    @Test
    void aThrowingSubscriberDoesNotStopTheOthersOrTheCommit() {
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger healthy = new AtomicInteger();
        try (var ignoredBad = hub.subscribe(new RunKey(A, 0), p -> {
            throw new IllegalStateException("consumer is wedged");
        }); var ignoredGood = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> healthy.incrementAndGet()))) {
            // ⚠️ The commit is ALREADY DURABLE. A consumer that throws must not
            // roll back or stall a write that succeeded; it falls behind and
            // recovers from the log, which is what the log is for.
            publishHeld(hub, delta(0, new RunKey(A, 0), 1, 0));
            assertThat(healthy.get()).isEqualTo(1);
        }
    }

    @Test
    void aCommitTouchingManyStreamsWakesEachOfThemOnce() {
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        try (var ignored1 = hub.subscribe(new RunKey(A, 0), SubscriptionHub.assembling(p -> a.incrementAndGet()));
                var ignored2 = hub.subscribe(new RunKey(B, 0), SubscriptionHub.assembling(p -> b.incrementAndGet()))) {
            publishHeld(hub, new CommitDelta(0, "seg", List.of(
                    new RunCommit(new RunKey(A, 0), 2, 0),
                    new RunCommit(new RunKey(B, 0), 5, 0))));
            assertThat(a.get()).isEqualTo(1);
            assertThat(b.get()).isEqualTo(1);
        }
    }

    @Test
    void manyIdleSubscribersIssueNoStoreRequestsAtAll() throws Exception {
        // ⚠️ NOT criterion 3, and an earlier comment here said it was. This loop
        // calls only hub.subscriberCount() -- a map lookup -- between the two
        // counter reads below; no ConsumerClient exists in this test and
        // SubscriptionHub holds no BinStore at all. The .isZero() a few lines
        // down is therefore held by CONSTRUCTION, the same reason row T8 is
        // struck in the M1 SPEC: nothing here can reach a store, so the
        // assertion cannot fail. What IS real, and load-bearing: the LIVENESS
        // half. 1,600 registered consumers, a commit log written and read, and
        // subscriberCount() staying 1 for every one of 3,000 iterations proves
        // they are still subscribed -- "all 1,600 were alive, not merely
        // silent" -- which a dead or never-started consumer would not show.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p", 0);
        SubscriptionHub hub = new SubscriptionHub();

        var subs = new java.util.ArrayList<SubscriptionHub.Subscription>();
        var wokenPerStream = new java.util.ArrayList<AtomicInteger>();
        for (int i = 0; i < 1600; i++) {
            AtomicInteger counter = new AtomicInteger();
            wokenPerStream.add(counter);
            subs.add(hub.subscribe(new RunKey(A, i), SubscriptionHub.assembling(p -> counter.incrementAndGet())));
        }
        // ⚠️ A POSITIVE liveness signal: all 1,600 are registered. "Nothing was
        // delivered" is also what 1,600 consumers that never started produce, so
        // on its own a zero proves the opposite of what it claims.
        assertThat(hub.subscribedStreams()).hasSize(1600);

        long requestsBefore = store.counts().total();
        for (int i = 0; i < 3000; i++) {
            // nothing happens: no commit, no poll, no timer
            assertThat(hub.subscriberCount(new RunKey(A, i % 1600))).isEqualTo(1);
        }
        // ⚠️ Kept, and still true, but NOT EVIDENCE: nothing above could have
        // made this nonzero. See the comment on this method.
        assertThat(store.counts().total() - requestsBefore)
                .as("held by construction, not asserted -- see the method comment").isZero();

        // and a control push at the end must reach every one of them
        for (int i = 0; i < 1600; i++) {
            publishHeld(hub, delta(i, new RunKey(A, i), 1, 0));
        }
        assertThat(wokenPerStream.stream().filter(c -> c.get() == 1).count())
                .as("all 1,600 were alive, not merely silent").isEqualTo(1600);
        subs.forEach(SubscriptionHub.Subscription::close);
        assertThat(log.nextSequence()).isZero();
    }
}
