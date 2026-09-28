// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Pushed bytes scale with CONSUMERS per segment, never with RUNS (M5.40a).
 *
 * <p>⚠️ THE NUMBER THIS IS ABOUT. {@code publishSegment} built one target per
 * (subscription, run), so a node holding K runs of a segment was opened K times
 * and handed the same bytes K times. For an 8 MiB segment of ~1,600 runs a node
 * holding ~178 of them took ~1.4 GiB to receive 8 MiB, and across the whole
 * segment that is 1,600 x 8 MiB = 12,800 MiB = 12.5 GiB of pushes for 8 MiB of
 * ingest -- against NFR-5's cross-AZ budget of 0.1% of ingested bytes, six
 * orders of magnitude over.
 *
 * <p>⚠️ **IT IS NFR-5, NOT NFR-4, AND THE GET COUNT WAS NEVER THE PROBLEM.**
 * {@code aSegmentOfMANYRunsIsReadONCENotOncePerRun} already passes: the store
 * is read once however many runs subscribe. What multiplied was the bytes
 * leaving the ingester, which is a different budget.
 *
 * <p>⚠️ AND THIS DOES NOT REACH 0.1%, WHICH IS THE POINT OF ASSERTING
 * PROPORTIONALITY INSTEAD. One stream per consumer still sends the segment to
 * each of ~9 nodes, ~6x ingested bytes cross-AZ, three orders over. Reaching
 * the budget needs the per-AZ prefetch M5.16 owns. What this row can be held to
 * is the SHAPE: K runs on one consumer cost one segment, not K.
 */
class OneStreamPerConsumerTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final byte[] HELD =
            "the segment bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    @Test
    void aConsumerHoldingMANYRunsOfOneSegmentIsHandedTheBytesONCE() throws Exception {
        int k = 64;
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger opens = new AtomicInteger();
        AtomicLong bytes = new AtomicLong();
        List<SubscriptionHub.Push> seen = new ArrayList<>();

        // ⚠️ ONE Subscriber INSTANCE FOR ALL K RUNS, which is how a consumer
        // says "these are all mine". The hub groups by that identity; two
        // different instances are two consumers even on the same node.
        SubscriptionHub.Subscriber oneNode = pushes -> {
            opens.incrementAndGet();
            seen.addAll(pushes);
            return (buffer, offset, length) -> bytes.addAndGet(length);
        };

        List<RunCommit> runs = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        for (int partition = 0; partition < k; partition++) {
            RunKey key = new RunKey(INDEX, partition);
            runs.add(new RunCommit(key, 2, 100L * partition));
            handles.add(hub.subscribe(key, oneNode));
        }

        publishHeld(hub, new CommitDelta(0, "seg-0", runs));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(bytes.get())
                .as("the segment is handed over ONCE however many runs of it this consumer holds")
                .isEqualTo(HELD.length);
        assertThat(opens.get())
                .as("one stream, so one open -- K opens is K streams by another name")
                .isEqualTo(1);
        assertThat(seen)
                .as("and still K pushes, because each run has its own key, count and offset")
                .hasSize(k);
        assertThat(seen.stream().map(SubscriptionHub.Push::key).distinct().count())
                .as("one push per run, not K copies of one run")
                .isEqualTo(k);
    }

    /**
     * A broken stream completes NONE of that consumer's runs, and only its own.
     *
     * <p>⚠️ THE MERGE FORCES THIS, it is not a preference. One sink is one byte
     * stream, and a stream that threw part way through has delivered a PREFIX --
     * no run of it is whole, so completing any of them would hand a consumer a
     * fragment labelled as a segment.
     *
     * <p>⚠️ THE FAKE FAILS EXACTLY ONCE, AND THAT IS WHAT MAKES THE CASE
     * FALSIFIABLE. Under the old contract this consumer's four runs had four
     * sinks SHARING THIS COUNTER, so one throw killed one of them and the other
     * three completed -- three runs delivered from a stream that had already
     * failed. One sink cannot do that. Review verified the other half by
     * mutation: a fake throwing on EVERY write leaves this case PASSING even
     * with the grouping removed, so it would pin nothing.
     *
     * <p>⚠️ AND "PART WAY THROUGH" IS NOT WHAT THIS CASE EXERCISES, which an
     * earlier draft of this javadoc claimed. The policy here answers
     * {@code INLINE} and {@code writeHeldBytes} issues exactly ONE
     * {@code write} of the whole 17 bytes, so the fake throws on its only write
     * with nothing delivered. What it discriminates is the NUMBER OF SINKS the
     * old contract would have opened, not a partial stream. A genuine prefix --
     * chunked, failing between chunks -- is covered in
     * {@code AssembledServingPathFailureTest}, and never yet with K &gt; 1.
     */
    @Test
    void aSTREAMThatBROKEPartWayCompletesNONEOfThatConsumersRuns() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> brokenCompleted = new ArrayList<>();
        List<SubscriptionHub.Push> healthyCompleted = new ArrayList<>();
        AtomicInteger writes = new AtomicInteger();

        SubscriptionHub.Subscriber breaks = new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(List<SubscriptionHub.Push> pushes) {
                return (buffer, offset, length) -> {
                    if (writes.getAndIncrement() == 0) {
                        throw new java.io.IOException("this consumer went away mid-stream");
                    }
                };
            }

            @Override
            public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
                brokenCompleted.addAll(pushes);
            }
        };
        SubscriptionHub.Subscriber healthy = new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(List<SubscriptionHub.Push> pushes) {
                return (buffer, offset, length) -> { };
            }

            @Override
            public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
                healthyCompleted.addAll(pushes);
            }
        };

        List<RunCommit> runs = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        for (int partition = 0; partition < 4; partition++) {
            RunKey key = new RunKey(INDEX, partition);
            runs.add(new RunCommit(key, 2, 100L * partition));
            handles.add(hub.subscribe(key, breaks));
            handles.add(hub.subscribe(key, healthy));
        }

        publishHeld(hub, new CommitDelta(0, "seg-0", runs));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(brokenCompleted)
                .as("ALL FOUR or none, and the stream broke -- so none")
                .isEmpty();
        assertThat(healthyCompleted)
                .as("one consumer's broken stream is its own; the other still gets every run")
                .hasSize(4);
    }

    /**
     * Two subscribers that COMPARE equal are still two consumers.
     *
     * <p>⚠️ THE TWO HALVES OF {@code ByIdentity} ARE REDUNDANT DEFENCES, and
     * this case pins the PAIR rather than either one. Measured, all three ways:
     * relaxing {@code equals} to {@code .equals} alone SURVIVES, because
     * {@code hashCode} is still {@code System.identityHashCode} and puts the
     * two subscribers in different buckets where they are never compared;
     * relaxing {@code hashCode} alone SURVIVES, because {@code ==} then refuses
     * the merge inside the bucket; relaxing BOTH is caught here.
     *
     * <p>⚠️ SO NEITHER SINGLE MUTATION IS OBSERVABLE THROUGH MERGING, WHICH IS
     * WHAT WAS MEASURED -- not that either is equivalent outright. Relaxing
     * {@code hashCode} to {@code Objects.hashCode} calls caller-supplied code
     * inside {@code publishSegment}'s grouping loop, which nothing wraps: a
     * subscriber whose {@code hashCode} throws would escape publishing
     * altogether and {@code DefaultIngest.pushLoop} would drop the whole
     * delta's delivery. {@code System.identityHashCode} cannot do that.
     *
     * <p>⚠️ AND REVIEW'S FAILURE SCENARIO FOR THE OTHER HALF DOES NOT REACH,
     * which it retracted after measuring: a
     * {@code record NodeSubscriber(...)} with a generated {@code equals} would
     * NOT collapse under {@code .equals} alone. What the pair buys, and what
     * this case protects, is that two live subscriptions never become one
     * target -- one sink never opened, never written, never completed, its runs
     * lost while the commit is durable.
     */
    @Test
    void TWOSubscribersThatCompareEQUALAreStillTWOConsumers() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        AlwaysEqual first = new AlwaysEqual(new ArrayList<>());
        AlwaysEqual second = new AlwaysEqual(new ArrayList<>());
        assertThat(first)
                .as("the fixture only means anything if these really do compare equal")
                .isEqualTo(second)
                .isNotSameAs(second);

        try (var ignoredA = hub.subscribe(new RunKey(INDEX, 0), first);
                var ignoredB = hub.subscribe(new RunKey(INDEX, 1), second)) {
            publishHeld(hub, new CommitDelta(0, "seg-0", List.of(
                    new RunCommit(new RunKey(INDEX, 0), 2, 0),
                    new RunCommit(new RunKey(INDEX, 1), 2, 100))));
        }

        assertThat(first.seen()).as("the first consumer is served").hasSize(1);
        assertThat(second.seen())
                .as("and so is the second -- merging them silently drops one consumer's runs")
                .hasSize(1);
    }

    /**
     * Each consumer is COMPLETED with its own runs, not with another's.
     *
     * <p>⚠️ REVIEW MEASURED {@code complete(pushes.get(i))} REDUCED TO
     * {@code pushes.get(0)} SURVIVING: the {@code open} half of "these K pushes
     * are yours" was pinned and the {@code complete} half was not pinned at
     * all. It survived because the two consumers in the case above hold the
     * SAME four runs, so their lists are equal-valued, and every other
     * {@code complete} in the suite discards its argument. Disjoint run sets
     * are what make the two distinguishable.
     */
    @Test
    void eachConsumerIsCOMPLETEDWithITSOwnRunsAndNotAnothers() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> firstCompleted = new ArrayList<>();
        List<SubscriptionHub.Push> secondCompleted = new ArrayList<>();

        SubscriptionHub.Subscriber holdsLowPartitions = completing(firstCompleted);
        SubscriptionHub.Subscriber holdsHighPartitions = completing(secondCompleted);

        try (var a0 = hub.subscribe(new RunKey(INDEX, 0), holdsLowPartitions);
                var a1 = hub.subscribe(new RunKey(INDEX, 1), holdsLowPartitions);
                var b2 = hub.subscribe(new RunKey(INDEX, 2), holdsHighPartitions);
                var b3 = hub.subscribe(new RunKey(INDEX, 3), holdsHighPartitions)) {
            publishHeld(hub, new CommitDelta(0, "seg-0", List.of(
                    new RunCommit(new RunKey(INDEX, 0), 2, 0),
                    new RunCommit(new RunKey(INDEX, 1), 2, 100),
                    new RunCommit(new RunKey(INDEX, 2), 2, 200),
                    new RunCommit(new RunKey(INDEX, 3), 2, 300))));
        }

        assertThat(firstCompleted.stream().map(SubscriptionHub.Push::key).toList())
                .as("completed with the runs this consumer actually holds")
                .containsExactly(new RunKey(INDEX, 0), new RunKey(INDEX, 1));
        assertThat(secondCompleted.stream().map(SubscriptionHub.Push::key).toList())
                .as("and the other consumer with ITS own, never the first one's")
                .containsExactly(new RunKey(INDEX, 2), new RunKey(INDEX, 3));
    }

    /**
     * {@code assembling} notifies its consumer once per run, not once per segment.
     *
     * <p>⚠️ REVIEW MEASURED THE LOOP THIS COMMIT ADDED TO {@code assembling}
     * REDUCED TO ITS FIRST PUSH AND SURVIVING, because every caller in the tree
     * subscribes one {@code RunKey} per {@code Subscriber}, so K is always 1.
     * Under that mutation a node holding K runs is told about run 0 and the
     * other K-1 vanish with no error anywhere.
     */
    @Test
    void theAssemblingAdapterNotifiesOncePerRUNAndSharesONECopyOfTheBytes() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new ArrayList<>();
        SubscriptionHub.Subscriber node = SubscriptionHub.assembling(got::add);

        try (var a = hub.subscribe(new RunKey(INDEX, 0), node);
                var b = hub.subscribe(new RunKey(INDEX, 1), node);
                var c = hub.subscribe(new RunKey(INDEX, 2), node)) {
            publishHeld(hub, new CommitDelta(0, "seg-0", List.of(
                    new RunCommit(new RunKey(INDEX, 0), 2, 0),
                    new RunCommit(new RunKey(INDEX, 1), 2, 100),
                    new RunCommit(new RunKey(INDEX, 2), 2, 200))));
        }

        assertThat(got.stream().map(SubscriptionHub.Push::key).toList())
                .as("one notification per run this consumer holds")
                .containsExactly(new RunKey(INDEX, 0), new RunKey(INDEX, 1), new RunKey(INDEX, 2));
        assertThat(got).allSatisfy(push ->
                assertThat(push.segment()).isEqualTo(HELD));
        assertThat(got.get(0).segment())
                .as("ONE array shared by reference, which is what makes the adapter's cost "
                        + "O(SEGMENT) PER SUBSCRIBER rather than per subscription")
                .isSameAs(got.get(2).segment());
    }

    private static SubscriptionHub.Subscriber completing(List<SubscriptionHub.Push> into) {
        return new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(List<SubscriptionHub.Push> pushes) {
                return (buffer, offset, length) -> { };
            }

            @Override
            public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
                into.addAll(pushes);
            }
        };
    }

    /** A subscriber whose {@code equals} says yes to anything of its own kind. */
    private record AlwaysEqual(List<SubscriptionHub.Push> seen)
            implements SubscriptionHub.Subscriber {

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            seen.addAll(pushes);
            return (buffer, offset, length) -> { };
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AlwaysEqual;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    private static void publishHeld(SubscriptionHub hub, CommitDelta delta) {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        hub.publish(delta, delta.segmentKey(), HELD,
                new SegmentServing(
                        new FetchPolicy(new FetchPolicyConfig(
                                Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                        store.capabilities(), new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger())));
    }
}
