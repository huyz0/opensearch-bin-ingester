// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.FetchMode;
import binjava.format.RunCommit;
import binjava.format.SegmentCommit;
import binjava.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The serving path with `inline` and `proxy` ASSEMBLED (M5.45a).
 *
 * <p>⚠️ WHAT THIS FILE EXISTS TO CATCH, and what no test in the tree caught
 * before it: {@link FetchPolicy}, {@link SegmentProxy}, {@link GrantIssuer} and
 * {@code SubscriptionEvent} were each tested in isolation and constructed
 * NOWHERE outside a test. Every property they assert was therefore a property
 * of a class, not of a serving path. M5's criteria 5 and 6 describe the path.
 *
 * <p>⚠️ THE DECISION IS PER SEGMENT, NOT PER RUN, and that is the assertion
 * {@link #aSegmentOfMANYRunsIsReadONCENotOncePerRun} exists for. A hub looping
 * its per-{@code RunKey} subscriber map would call {@code streamTo} once per
 * run -- ~1,600 GETs for one 8 MiB segment -- which is the shard scaling NFR-4
 * rules out and non-negotiable 6 forbids. M5.40 records that gap; this closes
 * its caller-granularity half.
 */
class AssembledServingPathTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");

    /** A sink that keeps what it was given, so bytes can be compared across modes. */
    private static final class Collecting implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final AtomicLong largestHandOff = new AtomicLong();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            // ⚠️ COPIES, because SegmentSink's contract says the bytes are only
            // valid for the duration of the call.
            out.write(buffer, offset, length);
            largestHandOff.accumulateAndGet(length, Math::max);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    private static byte[] segmentOf(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    private static CommitDelta oneRun(String segmentKey, RunKey key, int count, long first) {
        return new CommitDelta(1, segmentKey, List.of(new RunCommit(key, count, first)));
    }

    /**
     * A segment small enough to inline is delivered with ZERO store reads.
     *
     * <p>⚠️ THE ZERO IS THE POINT. `inline` exists so that neither the consumer
     * NOR the ingester issues a request for bytes already in hand; a serving
     * path that re-read what it had just written would pay a GET per flush.
     */
    @Test
    void aSegmentUNDERTheInlineCapIsServedINLINEAndReadZEROTimes() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(1024);
        store.put("seg-small", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting sink = new Collecting();
        List<FetchMode> modes = new ArrayList<>();

        try (var ignored = hub.subscribe(key, new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(java.util.List<SubscriptionHub.Push> pushes) {
                pushes.forEach(push -> modes.add(push.via()));
                return sink;
            }
        })) {
            hub.publish(oneRun("seg-small", key, 3, 10), "seg-small", segment, serving(store));
        }

        assertThat(modes).as("small, in hand, in the serving AZ").containsExactly(FetchMode.INLINE);
        assertThat(sink.bytes()).isEqualTo(segment);
        assertThat(store.counts().gets() - getsBefore)
                .as("bytes already in hand are never re-read")
                .isZero();
    }

    /**
     * A segment this pod does NOT hold is served `proxy`, from ONE store read
     * no matter how many subscribers take it.
     *
     * <p>⚠️ THE CAP IS NOT WHAT DECIDES HERE, and the first version of this
     * test was named as though it were. With {@code heldBytes == null},
     * {@code publishSegment} answers {@code PROXY} from the ternary WITHOUT
     * consulting {@link FetchPolicy} at all -- so a policy hard-wired to return
     * {@code INLINE} passed the test its old name promised would catch it.
     * The claim "the cap decides" is carried by
     * {@link #theREALIngesterChoosesTheModeFromTheSegmentSIZE}, which holds the
     * bytes and straddles the cap in both directions.
     *
     * <p>⚠️ WHAT THIS TEST DOES CARRY is the count: ONE read for 64
     * subscribers. K reads is the cost `proxy` exists to avoid.
     */
    @Test
    void aSegmentThisPodDoesNOTHoldIsServedPROXYFromONEReadForEverySubscriber() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(512 * 1024);
        store.put("seg-big", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        List<Collecting> sinks = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        List<FetchMode> modes = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            Collecting sink = new Collecting();
            sinks.add(sink);
            handles.add(hub.subscribe(key, pushes -> {
                pushes.forEach(push -> modes.add(push.via()));
                return sink;
            }));
        }

        hub.publish(oneRun("seg-big", key, 3, 10), null, null, serving(store));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(modes).as("bytes we do not hold cannot be inlined; all 64 are told so")
                .hasSize(64).containsOnly(FetchMode.PROXY);
        assertThat(store.counts().gets() - getsBefore)
                .as("ONE read serves all 64; K reads is the cost proxy exists to avoid")
                .isEqualTo(1);
        for (Collecting sink : sinks) {
            assertThat(sink.bytes()).isEqualTo(segment);
        }
    }

    /**
     * `inline` and `proxy` deliver BYTE-IDENTICAL segments -- SPEC criterion 5
     * for two of the three modes.
     */
    @Test
    void inlineAndProxyDeliverBYTEIDENTICALBytes() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(200_000);
        store.put("seg", Body.ofBytes(segment));
        RunKey key = new RunKey(INDEX, 0);

        Collecting viaInline = new Collecting();
        SubscriptionHub inlineHub = new SubscriptionHub();
        try (var ignored = inlineHub.subscribe(key, push -> viaInline)) {
            inlineHub.publish(oneRun("seg", key, 3, 10), "seg", segment,
                    servingWith(store, new FetchPolicy(
                            new FetchPolicyConfig(Long.MAX_VALUE, Long.MAX_VALUE, 1))));
        }

        Collecting viaProxy = new Collecting();
        SubscriptionHub proxyHub = new SubscriptionHub();
        try (var ignored = proxyHub.subscribe(key, push -> viaProxy)) {
            proxyHub.publish(oneRun("seg", key, 3, 10), null, null,
                    servingWith(store, proxyAlways()));
        }

        assertThat(viaInline.bytes()).isEqualTo(segment);
        assertThat(viaProxy.bytes())
                .as("the mode changes who reads the bytes, never which bytes arrive")
                .isEqualTo(viaInline.bytes());
    }

    /**
     * A segment carrying MANY runs is read ONCE, not once per run.
     *
     * <p>⚠️ THIS IS M5.40'S CALLER-GRANULARITY HALF, and it is the assertion
     * that separates a correct hub from one that loops its per-{@code RunKey}
     * map: 8 runs, 8 subscribers, ONE segment, ONE read. At production scale
     * the same defect is ~1,600 GETs for one 8 MiB segment.
     */
    @Test
    void aSegmentOfMANYRunsIsReadONCENotOncePerRun() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(512 * 1024);
        store.put("seg-many", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        List<RunCommit> runs = new ArrayList<>();
        List<Collecting> sinks = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        // ⚠️ ONE PUSH LIST PER PARTITION, because the bytes are the SAME for
        // all 8 and only the push tells them apart. A hub that built every
        // push from `targets.get(0).run()` -- one segment, one run, why not --
        // delivers partition 0's key and offset to all 8 subscribers, every
        // byte assertion below still passes, and `ConsumerClient` decodes the
        // same records eight times under eight wrong stream identities.
        // Review MEASURED that mutation surviving the whole suite.
        List<List<SubscriptionHub.Push>> pushes = new ArrayList<>();
        for (int partition = 0; partition < 8; partition++) {
            RunKey key = new RunKey(INDEX, partition);
            runs.add(new RunCommit(key, 2 + partition, 100L * partition));
            Collecting sink = new Collecting();
            sinks.add(sink);
            List<SubscriptionHub.Push> mine = new ArrayList<>();
            pushes.add(mine);
            handles.add(hub.subscribe(key, forThisSubscriber -> {
                mine.addAll(forThisSubscriber);
                return sink;
            }));
        }

        hub.publish(new CommitDelta(1, "seg-many", runs), null, null, serving(store));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(store.counts().gets() - getsBefore)
                .as("8 runs in ONE segment is ONE read, not 8")
                .isEqualTo(1);
        for (Collecting sink : sinks) {
            assertThat(sink.bytes()).isEqualTo(segment);
        }
        for (int partition = 0; partition < 8; partition++) {
            int p = partition;
            assertThat(pushes.get(partition)).as("partition %d woken exactly once", p)
                    .singleElement()
                    .satisfies(push -> {
                        assertThat(push.key())
                                .as("each subscriber is told about ITS OWN stream")
                                .isEqualTo(new RunKey(INDEX, p));
                        assertThat(push.recordCount())
                                .as("and its own record count, not the first run's")
                                .isEqualTo(2 + p);
                        assertThat(push.firstOffset())
                                .as("and its own offset -- the bytes are identical for all 8, "
                                        + "so this is the only thing that separates them")
                                .isEqualTo(100L * p);
                        assertThat(push.segmentKey()).isEqualTo("seg-many");
                    });
        }
    }

    /**
     * A BATCHED delta delivers every segment, each from where its bytes are.
     *
     * <p>⚠️ THIS IS A RECORD-LOSS FIX, NOT A NEW FEATURE. The predecessor of
     * this publish took a bare {@code byte[]} and threw
     * {@code IllegalStateException} on any delta naming more than one segment --
     * correct as far as it went, since one array is not the payload of many
     * segments. But {@code DefaultIngest.pushLoop} catches
     * {@code RuntimeException} and treats it as a slow subscriber, so every
     * batched commit was dropped for EVERY subscriber, silently and with the
     * commit already durable.
     *
     * <p>⚠️ AND IT IS WHERE {@link SegmentProxy} EARNS ITS PLACE ON THE PUSH
     * PATH: this pod holds its own segment and none of the others, so the
     * others are the bytes there is no way to serve except by reading them.
     */
    @Test
    void aBATCHEDDeltaDeliversEverySegmentFromWhereItsBytesARE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] mine = segmentOf(4096);
        byte[] theirs = segmentOf(8192);
        store.put("seg-mine", Body.ofBytes(mine));
        store.put("seg-theirs", Body.ofBytes(theirs));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey minesKey = new RunKey(INDEX, 0);
        RunKey theirsKey = new RunKey(INDEX, 1);
        Collecting fromMine = new Collecting();
        Collecting fromTheirs = new Collecting();
        List<FetchMode> minesModes = new ArrayList<>();
        List<FetchMode> theirsModes = new ArrayList<>();
        List<String> minesLabels = new ArrayList<>();
        List<String> theirsLabels = new ArrayList<>();

        CommitDelta batched = new CommitDelta(7, List.of(
                new SegmentCommit("seg-mine", List.of(new RunCommit(minesKey, 3, 10))),
                new SegmentCommit("seg-theirs", List.of(new RunCommit(theirsKey, 5, 20)))));

        try (var ignoredA = hub.subscribe(minesKey, pushes -> {
                    pushes.forEach(push -> {
                        minesModes.add(push.via());
                        minesLabels.add(push.segmentKey());
                    });
                    return fromMine;
                });
                var ignoredB = hub.subscribe(theirsKey, pushes -> {
                    pushes.forEach(push -> {
                        theirsModes.add(push.via());
                        theirsLabels.add(push.segmentKey());
                    });
                    return fromTheirs;
                })) {
            hub.publish(batched, "seg-mine", mine, serving(store));
        }

        assertThat(fromMine.bytes()).as("this pod's own segment, from memory").isEqualTo(mine);
        assertThat(fromTheirs.bytes())
                .as("another pod's segment in the same delta, which the predecessor DROPPED")
                .isEqualTo(theirs);
        assertThat(minesModes).containsExactly(FetchMode.INLINE);
        assertThat(theirsModes)
                .as("bytes we do not hold cannot be inlined without buffering them first")
                .containsExactly(FetchMode.PROXY);
        assertThat(store.counts().gets() - getsBefore)
                .as("ONE read: the segment we did not have, and only that one")
                .isEqualTo(1);

        // ⚠️ AND THE LABEL, WHICH THE BYTES DO NOT COVER. Review MEASURED
        // it: passing `segments().get(0).segmentKey()` as the push label while
        // leaving the byte source alone is green across the whole module,
        // because the held-or-read decision happens one frame up in `publish`
        // and the two are independently mutable. This is ADR-0032's silent
        // data error -- the push SUCCEEDS, the offsets look right, and the
        // consumer is handed another pod's object name. It is the field
        // M5.45b's `direct` grant and M5.16's late subscriber fetch BY.
        assertThat(minesLabels)
                .as("each run is labelled with ITS OWN segment, not the first in the batch")
                .containsExactly("seg-mine");
        assertThat(theirsLabels)
                .as("and the other pod's run carries the other pod's segment key")
                .containsExactly("seg-theirs");
    }

    /**
     * No hand-off to any consumer exceeds the proxy's chunk, so the largest
     * array the serving path materialises is independent of the segment size
     * AND of K -- SPEC criterion 6 on the ASSEMBLED path.
     *
     * <p>⚠️ BOTH BYTE SOURCES, AT BOTH VALUES OF K. `proxy` reaches the
     * sinks by two different loops depending on whether this pod holds the
     * segment -- {@code SegmentProxy.streamTo} when it does not,
     * {@code writeHeldBytesChunked} when it does -- and the HELD one is the
     * path {@code DefaultIngest} actually takes, because an ingester always
     * holds the segment it just wrote. A first version of this test drove only
     * the store path, which left the production path's chunking asserted
     * nowhere above K=1.
     *
     * <p>⚠️ THIS BOUNDS THE HAND-OFF, NOT THE RESIDENCY, and on the held
     * path those are different claims: the array is already in memory, so
     * nothing here says the serving path holds less than a segment. What it
     * says is that no CONSUMER is handed more than a chunk, which is the
     * property that stays flat as K grows. The residency claim belongs to the
     * store path and is asserted by
     * {@link #theFirstHandOffReachesAConsumerBeforeTheSegmentIsDRAINED}.
     */
    @Test
    void theServingPathHandsOffNoMoreThanACHUNKAtAnyK() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(700_000);
        store.put("seg-chunked", Body.ofBytes(segment));

        for (boolean podHoldsIt : new boolean[] {false, true}) {
            for (int k : new int[] {1, 64}) {
                SubscriptionHub hub = new SubscriptionHub();
                RunKey key = new RunKey(INDEX, 0);
                List<Collecting> sinks = new ArrayList<>();
                List<AutoCloseable> handles = new ArrayList<>();
                for (int i = 0; i < k; i++) {
                    Collecting sink = new Collecting();
                    sinks.add(sink);
                    handles.add(hub.subscribe(key, push -> sink));
                }

                hub.publish(oneRun("seg-chunked", key, 3, 10),
                        podHoldsIt ? "seg-chunked" : null,
                        podHoldsIt ? segment : null,
                        servingWith(store, proxyAlways(), 7000));
                for (AutoCloseable h : handles) {
                    h.close();
                }

                for (Collecting sink : sinks) {
                    assertThat(sink.largestHandOff.get())
                            .as("at K=%d, bytes %s, no hand-off exceeds the chunk",
                                    k, podHoldsIt ? "in hand" : "from the store")
                            .isLessThanOrEqualTo(7000);
                    assertThat(sink.bytes()).as("and the whole segment still arrives")
                            .isEqualTo(segment);
                }
            }
        }
    }

    /**
     * The first consumer is handed bytes before the segment has been READ --
     * criterion 3's interleaving, on the assembled path.
     *
     * <p>⚠️ A HAND-OFF BOUND IS NOT A MEMORY BOUND, and this is the
     * distinction the criterion is written around. {@code readAllBytes()}
     * followed by chunk-sized slices satisfies every "no hand-off exceeds
     * 7,000 bytes" assertion in this file while materialising the whole
     * segment first -- review MEASURED that mutation surviving all of them.
     * What separates streaming from buffer-then-forward is WHEN the first
     * consumer sees a byte, so that is what this asserts: at the moment of the
     * first hand-off, at most one chunk has come out of the store.
     *
     * <p>⚠️ IT IS ASSERTED AT K=64, not K=1. At K=1 the two
     * implementations differ only in peak memory; at K=64 a buffering one also
     * holds the segment while serving every one of them, which is the shape
     * NFR-6 rules out.
     */
    @Test
    void theFirstHandOffReachesAConsumerBeforeTheSegmentIsDRAINED() throws Exception {
        byte[] segment = segmentOf(700_000);
        long[] readSoFar = {0};
        long[] readWhenFirstDelivered = {-1};
        SegmentProxyFixtures.StubStore store =
                new SegmentProxyFixtures.StubStore(segment, readSoFar, 0);

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        List<AutoCloseable> handles = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            handles.add(hub.subscribe(key, push -> (buffer, offset, length) -> {
                if (readWhenFirstDelivered[0] < 0) {
                    readWhenFirstDelivered[0] = readSoFar[0];
                }
            }));
        }

        hub.publish(oneRun("seg-streamed", key, 3, 10), null, null,
                new SegmentServing(proxyAlways(), store.capabilities(),
                        new SegmentProxy(store, 7000)));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(readWhenFirstDelivered[0])
                .as("a serving path that drains the stream first has read all %d bytes by its "
                        + "first hand-off; a streaming one has read at most one 7,000-byte chunk",
                        segment.length)
                .isLessThanOrEqualTo(7000);
        assertThat(readWhenFirstDelivered[0])
                .as("and a zero would mean nothing was ever delivered")
                .isPositive();
    }

    /**
     * `proxy` for bytes the pod HOLDS costs ZERO store reads.
     *
     * <p>⚠️ THE MODE NAMES HOW THE BYTES REACH THE CONSUMER, NOT WHERE THE
     * SERVING PATH GETS THEM. A hub that read the store whenever the mode was
     * `proxy` would buy one GET per flush for an array already in memory --
     * invisible to every other assertion here, because the bytes that arrive
     * would be identical.
     */
    @Test
    void proxyForBytesTheNodeHOLDSCostsZEROReads() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        byte[] segment = segmentOf(300_000);
        store.put("seg-held", Body.ofBytes(segment));
        long getsBefore = store.counts().gets();

        SubscriptionHub hub = new SubscriptionHub();
        RunKey key = new RunKey(INDEX, 0);
        Collecting sink = new Collecting();
        List<FetchMode> modes = new ArrayList<>();

        try (var ignored = hub.subscribe(key, pushes -> {
            pushes.forEach(push -> modes.add(push.via()));
            return sink;
        })) {
            hub.publish(oneRun("seg-held", key, 3, 10), "seg-held", segment,
                    servingWith(store, proxyAlways(), 7000));
        }

        assertThat(modes).containsExactly(FetchMode.PROXY);
        assertThat(store.counts().gets() - getsBefore)
                .as("the bytes were in hand; re-reading them is a GET bought for nothing")
                .isZero();
        assertThat(sink.bytes()).isEqualTo(segment);
        assertThat(sink.largestHandOff.get())
                .as("and they are still handed over a chunk at a time")
                .isLessThanOrEqualTo(7000);
    }

    /**
     * THE PRODUCTION PATH CHOOSES, and the choice is visible at the subscriber.
     *
     * <p>⚠️ THIS IS THE ASSERTION M5.45a EXISTS FOR. Every other test here
     * drives {@code SubscriptionHub.publish} directly with a policy the test
     * built. This one drives {@code DefaultIngest} -- a real append, a real
     * flush, a real commit -- and asserts the mode that arrives was decided by
     * the policy {@code DefaultIngest} built from the BACKEND'S OWN PRICES.
     * Before it, {@link FetchPolicy} had no production caller at all and every
     * property M5.11 asserts was a property of a class rather than of a serving
     * path.
     *
     * <p>⚠️ TWO SIZES IN ONE TEST, because either alone is passed by a constant.
     * A path hard-wired to `inline` passes the small case; one hard-wired to
     * `proxy` passes the large one. The default cap is 256 KiB
     * ({@code FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES}) and the two flushes
     * straddle it.
     */
    @Test
    void theREALIngesterChoosesTheModeFromTheSegmentSIZE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();

        try (var ignored = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 3),
                        SubscriptionHub.assembling(seen::add));
                DefaultIngest ingest =
                        IngestTestSupport.ingest(store, hub, IngestTestSupport.NEVER)) {
            IngestTestSupport.appendOnce(ingest, "logs", 3, 10);
            IngestTestSupport.awaitPush(seen);
            assertThat(seen).singleElement().satisfies(push -> assertThat(push.via())
                    .as("ten small documents are well under the 256 KiB inline cap")
                    .isEqualTo(FetchMode.INLINE));
            long inlineBytes = seen.get(0).segment().length;
            assertThat(inlineBytes)
                    .as("and the assertion above is only meaningful if it really is small")
                    .isLessThan(FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES);
            seen.clear();

            IngestTestSupport.appendOnce(ingest, "logs", 3, 30_000);
            IngestTestSupport.awaitPush(seen);
            assertThat(seen).singleElement().satisfies(push -> {
                assertThat((long) push.segment().length)
                        .as("30,000 documents must actually exceed the cap, or this proves nothing")
                        .isGreaterThan(FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES);
                assertThat(push.via())
                        .as("past the cap the INGESTER picks proxy; nothing the consumer said")
                        .isEqualTo(FetchMode.PROXY);
            });
        }
    }

    /**
     * A policy that can never inline.
     *
     * <p>⚠️ A CAP OF 1 BYTE, NOT 0: {@code FetchPolicyConfig} refuses a cap of
     * zero because "an inline cap of 0 inlines nothing ever", so the smallest
     * legal cap is what forces every real segment past it.
     */
    private static FetchPolicy proxyAlways() {
        return new FetchPolicy(new FetchPolicyConfig(1, 0, 1));
    }

    private static SegmentServing serving(CountingBinStore store) {
        return servingWith(store,
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())));
    }

    private static SegmentServing servingWith(CountingBinStore store, FetchPolicy policy) {
        return new SegmentServing(policy, store.capabilities(), new SegmentProxy(store));
    }

    private static SegmentServing servingWith(CountingBinStore store, FetchPolicy policy,
            int chunkBytes) {
        return new SegmentServing(policy, store.capabilities(),
                new SegmentProxy(store, chunkBytes));
    }
}
