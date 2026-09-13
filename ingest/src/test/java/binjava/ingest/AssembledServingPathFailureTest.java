// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.FetchMode;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What the assembled serving path does when a sink or the store FAILS.
 *
 * <p>⚠️ SPLIT FROM {@link AssembledServingPathTest} for the reason
 * {@code SegmentProxyFailureTest} was split from {@code SegmentProxyTest}: one
 * file asks what the path costs when everything works, the other what it does
 * when something does not. The seam is real rather than convenient -- the two
 * share only their scaffolding, and the happy-path file is already over 550
 * lines.
 *
 * <p>⚠️ EVERY TEST HERE COVERS A BRANCH NOTHING ELSE REACHES. Review measured
 * {@code Tracking.failed} deletable, and {@code if (sink.failed)} neuterable,
 * with all three suites green.
 */
class AssembledServingPathFailureTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");

    private static byte[] segmentOf(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    /** A sink that takes {@code failAfter} hand-offs and then throws. */
    private static final class FailingSink implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final int failAfter;
        private int handOffs;

        FailingSink(int failAfter) {
            this.failAfter = failAfter;
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            if (++handOffs > failAfter) {
                throw new IOException("this consumer's socket went away mid-segment");
            }
            out.write(buffer, offset, length);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    /** A sink that keeps everything it is given. */
    private static final class Collecting implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            out.write(buffer, offset, length);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    /**
     * A sink that throws mid-segment is NOT completed, and the others still are.
     *
     * <p>⚠️ COMPLETING A TRUNCATED SINK IS THE WORST OUTCOME AVAILABLE, which
     * is why this is the failure test that had to exist first. A subscriber
     * handed a prefix cannot tell it from a whole segment: it decodes fewer
     * records than the commit log says are there, under offsets the log says
     * exist, and nothing throws anywhere. {@code SegmentProxy.streamTo} returns
     * only a COUNT of the sinks that survived, so the hub has to wrap each one
     * to learn WHICH -- and review measured both halves of that wrapper
     * ({@code Tracking.failed}, and the {@code if (sink.failed)} guard)
     * deletable with every other test in the tree green.
     */
    @Test
    void aSinkThatTHROWSMidSegmentIsNOTCompletedAndTheOthersStillAre() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(700_000);
        store.put("seg-chunked", Body.ofBytes(segment));

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting healthyOne = new Collecting();
        Collecting healthyTwo = new Collecting();
        // ⚠️ ONE hand-off, then throw: far enough in to hold a prefix, far
        // short of the ~100 chunks a 700,000-byte segment takes at 7,000.
        FailingSink doomed = new FailingSink(1);
        List<SegmentSink> completed = new ArrayList<>();

        List<AutoCloseable> handles = new ArrayList<>();
        for (SegmentSink sink : List.of(healthyOne, doomed, healthyTwo)) {
            handles.add(hub.subscribe(key, new SubscriptionHub.Subscriber() {
                @Override
                public SegmentSink open(SubscriptionHub.Push push) {
                    return sink;
                }

                @Override
                public void complete(SubscriptionHub.Push push, SegmentSink sink2) {
                    completed.add(sink2);
                }
            }));
        }

        hub.publish(oneRun("seg-chunked", key, 3, 10), null, null,
                new SegmentServing(new FetchPolicy(new FetchPolicyConfig(1, 0, 1)),
                        store.capabilities(), new SegmentProxy(store, 7000)));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(completed)
                .as("the two that took every byte are completed; the one holding a PREFIX is not")
                .containsExactlyInAnyOrder(healthyOne, healthyTwo);
        assertThat(healthyOne.bytes()).as("a dead consumer does not truncate a live one")
                .isEqualTo(segment);
        assertThat(healthyTwo.bytes()).isEqualTo(segment);
        assertThat(doomed.bytes())
                .as("and the prefix it does hold is a prefix, which is why completing it would lie")
                .hasSizeLessThan(segment.length);
    }

    /**
     * A store failure on ONE segment does not deny the segment this pod HOLDS.
     *
     * <p>⚠️ A BATCHED DELTA NAMES MANY PODS' SEGMENTS AND THIS POD HOLDS ONE.
     * Reading another pod's segment is the only way to serve it, so that read
     * can fail for reasons that have nothing to do with the bytes in hand --
     * and if the failure escapes the loop over {@code delta.segments()}, every
     * segment AFTER the failing one is never published at all. The commit is
     * already durable by then, so the subscriber is not told about records that
     * exist, and {@code DefaultIngest.pushLoop} catches the throw as a slow
     * subscriber: silent, for the whole window.
     *
     * <p>⚠️ THE FAILING SEGMENT IS FIRST ON PURPOSE. With it second, a loop
     * that abandons the rest on failure passes anyway.
     */
    @Test
    void aSTOREFailureOnOneSegmentDoesNotDenyTheSegmentThisPodHOLDS() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] mine = segmentOf(4096);
        store.put("seg-mine", Body.ofBytes(mine));
        // ⚠️ `seg-theirs` is deliberately NOT put: a key the store does not
        // have is exactly the IOException a transient GET failure produces,
        // without a stub that has to imitate one.

        SubscriptionHub hub = new SubscriptionHub();
        RunKey theirsKey = new RunKey(INDEX, 1);
        RunKey minesKey = new RunKey(INDEX, 0);
        Collecting fromMine = new Collecting();
        Collecting fromTheirs = new Collecting();

        CommitDelta batched = new CommitDelta(7, List.of(
                new SegmentCommit("seg-theirs", List.of(new RunCommit(theirsKey, 5, 20))),
                new SegmentCommit("seg-mine", List.of(new RunCommit(minesKey, 3, 10)))));

        try (var ignoredA = hub.subscribe(theirsKey, push -> fromTheirs);
                var ignoredB = hub.subscribe(minesKey, push -> fromMine)) {
            assertThatThrownBy(() -> hub.publish(batched, "seg-mine", mine, serving(store)))
                    .as("the failure is still REPORTED -- swallowing it would make a store "
                            + "outage indistinguishable from a quiet window")
                    .isInstanceOf(UncheckedIOException.class);
        }

        assertThat(fromMine.bytes())
                .as("the segment we HOLD is published even though another pod's read failed")
                .isEqualTo(mine);
        assertThat(fromTheirs.bytes())
                .as("and the one that could not be read delivers nothing, rather than a prefix")
                .isEmpty();
    }

    /**
     * On the INLINE branch too, a sink that throws costs only itself.
     *
     * <p>⚠️ THIS IS THE BRANCH {@code DefaultIngest} ACTUALLY TAKES for every
     * segment under the 256 KiB cap, and until this test it had no failure
     * coverage at all -- every failing-sink test in this file drove the STORE
     * loop. Review measured two mutations surviving the whole tree because of
     * it: {@code continue} to {@code break} in {@code writeHeldBytes}, and
     * deleting that method's try/catch outright.
     *
     * <p>⚠️ {@code break} IS THE WORSE OF THE TWO AND IT IS SILENT. A sink
     * after the failing one is never written to, so nothing sets its
     * {@code Tracking.failed}, so {@link SubscriptionHub#deliver} COMPLETES it
     * -- holding zero bytes. Through {@link SubscriptionHub#assembling} that is
     * {@code new byte[0]}, and {@code ConsumerClient.decodeInto} throws inside
     * the sink, where the hub swallows it as a dead subscriber. The whole
     * window is lost for every subscriber registered after the unlucky one.
     */
    @Test
    void anINLINESinkThatTHROWSDoesNotDenyTheSubscribersAFTERIt() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        // ⚠️ Small enough that the DEFAULT policy answers `inline`, which is
        // what makes this the production branch rather than a contrived one.
        byte[] segment = segmentOf(4096);
        store.put("seg-inline", Body.ofBytes(segment));

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting first = new Collecting();
        FailingSink doomed = new FailingSink(0);
        Collecting last = new Collecting();
        List<SegmentSink> completed = new ArrayList<>();
        List<FetchMode> modes = new ArrayList<>();

        List<AutoCloseable> handles = new ArrayList<>();
        for (SegmentSink sink : List.of(first, doomed, last)) {
            handles.add(hub.subscribe(key, new SubscriptionHub.Subscriber() {
                @Override
                public SegmentSink open(SubscriptionHub.Push push) {
                    modes.add(push.via());
                    return sink;
                }

                @Override
                public void complete(SubscriptionHub.Push push, SegmentSink completedSink) {
                    completed.add(completedSink);
                }
            }));
        }

        hub.publish(oneRun("seg-inline", key, 3, 10), "seg-inline", segment, serving(store));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(modes).as("this test is about the INLINE branch, so it must BE the inline "
                + "branch -- a segment over the cap would prove the store loop again")
                .containsOnly(FetchMode.INLINE);
        assertThat(last.bytes())
                .as("the subscriber AFTER the failing one is served in full; `break` gives it "
                        + "nothing and completes it anyway")
                .isEqualTo(segment);
        assertThat(first.bytes()).isEqualTo(segment);
        assertThat(completed).containsExactlyInAnyOrder(first, last);
    }

    /**
     * A subscriber whose {@code open} throws costs only itself.
     *
     * <p>⚠️ IT IS NO LONGER THE ONLY TEST OF THIS, and M5.47 is what
     * changed that: {@code SubscriptionHubTest.aThrowingSubscriberDoesNot
     * StopTheOthersOrTheCommit} used to run the pre-M5.45a {@code publishRun}, which M5.47 REMOVED,
     * and now publishes through the four-argument {@code publish}, so removing
     * {@link SubscriptionHub#deliver}'s catch around {@code open} reds
     * {@code :ingest:test} there too. This case remains the one that pins it
     * on a MULTI-subscriber fan-out, where what the catch protects is the
     * OTHER consumers. Without it the throw leaves {@code publish},
     * reaches {@code DefaultIngest.pushLoop}, and is counted as a slow
     * subscriber: one consumer that cannot allocate a sink silently drops the
     * commit for every other consumer of the segment.
     */
    @Test
    void aSubscriberWhoseOPENThrowsDoesNotDenyTheOthers() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(4096);
        store.put("seg-inline", Body.ofBytes(segment));

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting first = new Collecting();
        Collecting last = new Collecting();

        try (var ignoredA = hub.subscribe(key, push -> first);
                var ignoredRefusesChecked = hub.subscribe(key, push -> {
                    throw new IOException("this consumer cannot take a segment right now");
                });
                // ⚠️ AN UNCHECKED THROW TOO, because the catch names
                // `IOException | RuntimeException` and only the checked half was
                // exercised: review measured the catch narrowed to `IOException`
                // surviving all three suites. An unchecked throw out of `open` is
                // the likelier production trigger -- an allocation failure, a
                // closed transport, a bug in the consumer -- and it has exactly
                // the consequence this test's javadoc describes.
                var ignoredRefusesUnchecked = hub.subscribe(key, push -> {
                    throw new IllegalStateException("this consumer is wedged");
                });
                var ignoredB = hub.subscribe(key, push -> last)) {
            hub.publish(oneRun("seg-inline", key, 3, 10), "seg-inline", segment, serving(store));
        }

        assertThat(first.bytes()).isEqualTo(segment);
        assertThat(last.bytes())
                .as("a subscriber that could not open a sink does not take the commit with it")
                .isEqualTo(segment);
    }

    /**
     * Every sink sees chunk N before any sees chunk N+1, on the HELD path.
     *
     * <p>⚠️ THE LOOP ORDER IS OBSERVABLE AT THIS TIER, which an earlier
     * draft of {@code writeHeldBytesChunked}'s javadoc denied while review had
     * consumer-outer surviving all 198 tests. It is observable because the
     * sinks can share ONE ordered log: chunk-outer writes a different sink's
     * identity in each of the first three entries, consumer-outer writes the
     * same one three times over.
     *
     * <p>⚠️ WHAT IT BUYS IS INTERLEAVING, NOT MEMORY, and the javadoc says
     * so: these bytes are already in this pod's hand. Consumer-outer would
     * hand the first consumer the WHOLE segment before the second saw a byte,
     * which for a sink that is a socket is a head-of-line block the length of
     * a segment.
     */
    @Test
    void everySinkGetsTheFIRSTChunkBeforeAnyGetsTheSECOND() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(700_000);
        store.put("seg-order", Body.ofBytes(segment));

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        List<Integer> handOffOrder = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int id = i;
            handles.add(hub.subscribe(key, push -> (buffer, offset, length) -> {
                handOffOrder.add(id);
            }));
        }

        hub.publish(oneRun("seg-order", key, 3, 10), "seg-order", segment,
                new SegmentServing(new FetchPolicy(new FetchPolicyConfig(1, 0, 1)),
                        store.capabilities(), new SegmentProxy(store, 7000)));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(handOffOrder).hasSizeGreaterThan(3);
        assertThat(Set.copyOf(handOffOrder.subList(0, 3)))
                .as("the first three hand-offs go to three DIFFERENT sinks; consumer-outer "
                        + "sends all three to the same one")
                .hasSize(3);
    }

    /**
     * A segment NOBODY subscribes to costs zero store reads.
     *
     * <p>⚠️ BOTH EARLY RETURNS COULD BE DELETED WITH THE TREE GREEN, because
     * each masks the other and nothing asserted the zero.
     * {@code SegmentProxy.streamTo}'s own javadoc says AN EMPTY LIST STILL
     * READS, so without the guards a batched delta naming segments this pod's
     * subscribers do not want buys one GET each -- a request rate that scales
     * with other pods' flushes, which is the shape criterion 8's zero exists
     * to forbid under load rather than merely at rest.
     */
    @Test
    void aSegmentNOBODYSubscribesToCostsZEROReads() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(512 * 1024);
        store.put("seg-unwanted", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        Collecting elsewhere = new Collecting();
        // ⚠️ A LIVE SUBSCRIBER ON ANOTHER STREAM, so the zero below is "nobody
        // wanted THIS segment" rather than "this hub has no subscribers".
        try (var ignored = hub.subscribe(new RunKey(INDEX, 99), push -> elsewhere)) {
            hub.publish(oneRun("seg-unwanted", new RunKey(INDEX, 0), 3, 10), null, null,
                    serving(store));
        }

        assertThat(store.counts().gets() - getsBefore)
                .as("nobody wanted it, so nothing was read for them")
                .isZero();
        assertThat(elsewhere.bytes()).as("and the uninterested subscriber heard nothing").isEmpty();
    }

    /**
     * {@link SegmentServing} refuses what its javadoc says it refuses.
     *
     * <p>⚠️ THE RUNG-1 ARGUMENT RESTED ON AN UNRUN LINE. `proxy` was made
     * required so that "this deployment cannot serve proxy" is unrepresentable
     * rather than a branch; review then measured the {@code requireNonNull}
     * deletable with {@code :ingest:test} green, which leaves the argument
     * resting on a line nothing executes. Same shape as
     * {@code FetchPolicyTest.theConstructorsREFUSEWhatTheirMessagesSayTheyRefuse}.
     */
    @Test
    void theConstructorREFUSESWhatItsJavadocSaysItRefuses() {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        FetchPolicy policy = new FetchPolicy(
                FetchPolicyConfig.defaultsFor(store.capabilities().costs()));
        SegmentProxy proxy = new SegmentProxy(store);

        assertThatThrownBy(() -> new SegmentServing(policy, store.capabilities(), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("proxy");
        assertThatThrownBy(() -> new SegmentServing(null, store.capabilities(), proxy))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("policy");
        assertThatThrownBy(() -> new SegmentServing(policy, null, proxy))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("capabilities");
    }

    /**
     * On the HELD `proxy` branch too, a sink that throws costs only itself.
     *
     * <p>⚠️ THIS IS THE BRANCH {@code DefaultIngest} TAKES FOR A LARGE
     * SEGMENT IT JUST WROTE -- bytes in hand, past the inline cap, so
     * {@code writeHeldBytesChunked} rather than {@code SegmentProxy.streamTo}.
     * Round 2 found the inline branch untested and it was closed on
     * {@code writeHeldBytes}; round 3 measured the SAME two mutations still
     * green on this sibling, because every other failing-sink test in this file
     * publishes {@code heldBytes == null} and never enters the method.
     *
     * <p>⚠️ THE SECOND MUTATION IS THE SILENT ONE. Replacing
     * {@code live.remove(i)} with {@code return} abandons the segment for
     * EVERYONE at the first throwing sink -- and the healthy sinks, whose
     * {@code Tracking.failed} is false because their own writes never threw,
     * are then COMPLETED HOLDING A PREFIX. That is the outcome
     * {@link SubscriptionHub#deliver}'s javadoc calls the worst available, and
     * the one the first test in this file exists to prevent on the other path.
     */
    @Test
    void aHELDProxySinkThatTHROWSDoesNotDenyTheOthers() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(700_000);
        store.put("seg-held", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting first = new Collecting();
        FailingSink doomed = new FailingSink(1);
        Collecting last = new Collecting();
        List<SegmentSink> completed = new ArrayList<>();
        List<FetchMode> modes = new ArrayList<>();

        List<AutoCloseable> handles = new ArrayList<>();
        for (SegmentSink sink : List.of(first, doomed, last)) {
            handles.add(hub.subscribe(key, new SubscriptionHub.Subscriber() {
                @Override
                public SegmentSink open(SubscriptionHub.Push push) {
                    modes.add(push.via());
                    return sink;
                }

                @Override
                public void complete(SubscriptionHub.Push push, SegmentSink completedSink) {
                    completed.add(completedSink);
                }
            }));
        }

        hub.publish(oneRun("seg-held", key, 3, 10), "seg-held", segment,
                new SegmentServing(new FetchPolicy(new FetchPolicyConfig(1, 0, 1)),
                        store.capabilities(), new SegmentProxy(store, 7000)));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(modes).as("held bytes past the cap travel `proxy`, chunked from memory")
                .containsOnly(FetchMode.PROXY);
        assertThat(store.counts().gets() - getsBefore)
                .as("and still from memory -- this branch buys no request")
                .isZero();
        assertThat(first.bytes())
                .as("a sink that dies mid-segment does not truncate the sink BEFORE it")
                .isEqualTo(segment);
        assertThat(last.bytes())
                .as("nor the one AFTER it")
                .isEqualTo(segment);
        assertThat(completed).containsExactlyInAnyOrder(first, last);
        assertThat(doomed.bytes()).hasSizeLessThan(segment.length);
    }

    /**
     * When EVERY subscriber fails to open, nothing is read for any of them.
     *
     * <p>⚠️ TWO EARLY RETURNS, AND EACH MASKS THE OTHER. The sibling test
     * {@link #aSegmentNOBODYSubscribesToCostsZEROReads} has no subscriber at
     * all, so it stops at the {@code targets.isEmpty()} guard and the second
     * guard is never reached -- review measured
     * {@code opened.isEmpty() && false} surviving all three suites because of
     * it. Here the subscribers EXIST and every {@code open} throws, which is
     * the only state that reaches the second guard: without it
     * {@code SegmentProxy.streamTo} is called with an empty list, and its own
     * javadoc says AN EMPTY LIST STILL READS -- one GET bought for zero
     * consumers.
     */
    @Test
    void aSegmentWHOSEEverySubscriberFailsToOpenCostsZEROReads() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(512 * 1024);
        store.put("seg-nobody-opens", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        List<AutoCloseable> handles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            handles.add(hub.subscribe(key, push -> {
                throw new IOException("no sink available");
            }));
        }

        hub.publish(oneRun("seg-nobody-opens", key, 3, 10), null, null, serving(store));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(store.counts().gets() - getsBefore)
                .as("three subscribers, none of whom could take it, is still nobody")
                .isZero();
    }

    private static CommitDelta oneRun(String segmentKey, RunKey key, int count, long first) {
        return new CommitDelta(1, segmentKey, List.of(new RunCommit(key, count, first)));
    }

    private static SegmentServing serving(CountingBinStore store) {
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store));
    }
}
