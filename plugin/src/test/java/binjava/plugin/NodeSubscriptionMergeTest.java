// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.client.ConsumerClient;
import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.CommitDelta;
import binjava.format.OpType;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import binjava.ingest.FetchPolicy;
import binjava.ingest.FetchPolicyConfig;
import binjava.ingest.SegmentProxy;
import binjava.ingest.SegmentServing;
import binjava.ingest.SubscriptionHub;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A node holding K runs of one segment takes the bytes ONCE (M5.62).
 *
 * <p>⚠️ M5.40a BOUGHT THE CONTRACT AND NOT THE BYTES, and this is where the
 * difference was paid. {@code SubscriptionHub} groups by {@code Subscriber}
 * IDENTITY, so one subscriber registered for K runs is opened once and handed
 * the segment once -- but {@code NodeSubscriptions} built one
 * {@code ConsumerClient} per run and each subscribed for itself, so a node
 * holding 178 runs of a segment was 178 consumers to the hub and took 178
 * copies. The property held for anything that opted in, and nothing did.
 *
 * <p>⚠️ THE ASSERTION IS THE OPEN COUNT, NOT THE DELIVERY COUNT. K deliveries
 * are correct and required -- each run carries its own offsets -- so counting
 * them measures nothing. What K must NOT scale with is how many times the
 * segment is handed over, which is one {@code open} per consumer per segment.
 */
class NodeSubscriptionMergeTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final String SEGMENT_KEY = "seg-with-three-runs";
    private static final List<RunKey> RUNS = List.of(
            new RunKey(INDEX, 0), new RunKey(INDEX, 1), new RunKey(INDEX, 2));

    /**
     * A transport over a real {@link SubscriptionHub} that MERGES: one
     * {@code Subscriber} for every key of a multi-key subscribe.
     *
     * <p>⚠️ THE SINGLE-KEY OVERLOAD IS DELIBERATELY UNMERGED, because that is
     * what the interface's own default does and what production did before this
     * row. A fake that merged both ways could not tell the two apart.
     */
    private record MergingHubTransport(SubscriptionHub hub, AtomicInteger opens,
            java.util.Map<RunKey, Integer> registrations) implements SubscriptionTransport {

        MergingHubTransport(SubscriptionHub hub, AtomicInteger opens) {
            this(hub, opens, new java.util.HashMap<>());
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return subscribe(List.of(key), listener);
        }

        @Override
        public MultiSubscription subscribe(List<RunKey> keys, Listener listener) {
            SubscriptionHub.Subscriber one = new SubscriptionHub.Subscriber() {
                @Override
                public Assembling open(List<SubscriptionHub.Push> pushes) {
                    opens.incrementAndGet();
                    return new Assembling();
                }

                @Override
                public void complete(List<SubscriptionHub.Push> pushes,
                        binjava.ingest.SegmentSink sink) {
                    // ⚠️ THE BYTES COME FROM THE SINK, NOT FROM THE PUSH.
                    // `Push.segment()` is EMPTY in every push the serving path
                    // builds -- its own javadoc says so -- because the segment
                    // travels through the sink the subscriber opened. A fake
                    // reading `push.segment()` hands the consumer an empty array
                    // and `SegmentReader.open` fails with "shorter than its own
                    // preamble and footer", which is how this was found.
                    //
                    // ⚠️ AND ONE ARRAY SERVES ALL K DELIVERIES, which is the
                    // merge made visible: the sink was opened once, so there is
                    // one assembled array for every run this node holds.
                    byte[] assembled = ((Assembling) sink).bytes();
                    pushes.forEach(push -> listener.onDelivery(new Delivery(push.key(),
                            push.segmentKey(), push.recordCount(), push.firstOffset(),
                            push.via(), assembled)));
                }
            };
            java.util.Map<RunKey, AutoCloseable> handles = new java.util.LinkedHashMap<>();
            keys.forEach(key -> {
                registrations.merge(key, 1, Integer::sum);
                handles.put(key, hub.subscribe(key, one));
            });
            return new MultiSubscription() {
                @Override
                public void add(RunKey key) {
                    // ⚠️ THE SAME `one` SUBSCRIBER, which is what the merge is.
                    // A fake building a fresh subscriber here would register a
                    // second consumer and the hub would hand the segment over
                    // twice -- the shape this row removes, wearing the new API.
                    handles.computeIfAbsent(key, k -> {
                        registrations.merge(k, 1, Integer::sum);
                        return hub.subscribe(k, one);
                    });
                }

                @Override
                public void remove(RunKey key) {
                    // ⚠️ THE MAPPING GOES ONLY AFTER THE CLOSE SUCCEEDS, which
                    // is what `MultiSubscription.remove`'s contract requires of
                    // every implementation and not only of the default: a
                    // stream unsubscribed by a call that then threw leaves a
                    // live consumer with no registration, because the caller's
                    // recovery re-enters `remove` and never `add`. This fake's
                    // close cannot throw, which is exactly why writing it the
                    // other way round would have been invisible.
                    AutoCloseable handle = handles.get(key);
                    if (handle == null) {
                        return;
                    }
                    try {
                        handle.close();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    handles.remove(key);
                }

                @Override
                public void close() {
                    handles.values().forEach(handle -> {
                        try {
                            handle.close();
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    handles.clear();
                }
            };
        }
    }

    /** A sink that keeps what it was handed, so {@code complete} can deliver it. */
    private static final class Assembling implements binjava.ingest.SegmentSink {
        private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            out.write(buffer, offset, length);
        }

        byte[] bytes() {
            return out.toByteArray();
        }
    }

    @Test
    void aNodeHoldingTHREERunsOfOneSegmentIsOpenedONCE() throws Exception {
        BinStore store = new MemoryBinStore();
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger opens = new AtomicInteger();
        byte[] segment = segmentWithARunPerKey();

        List<ConsumerClient> clients = new ArrayList<>();
        try (NodeSubscriptions node =
                new NodeSubscriptions(new MergingHubTransport(hub, opens), 16)) {
            RUNS.forEach(key -> clients.add(node.clientFor(key)));

            assertThat(hub.subscribedStreams())
                    .as("PREMISE: the node really did register for all three streams")
                    .containsExactlyInAnyOrderElementsOf(RUNS);

            hub.publish(new CommitDelta(0, List.of(new binjava.format.SegmentCommit(SEGMENT_KEY,
                            List.of(new RunCommit(RUNS.get(0), 1, 100L),
                                    new RunCommit(RUNS.get(1), 1, 200L),
                                    new RunCommit(RUNS.get(2), 1, 300L))))),
                    SEGMENT_KEY, segment, serving(store));

            assertThat(opens.get())
                    .as("ONE open for the whole node, however many runs of the segment it "
                            + "holds -- one per run is the 178-copy shape this row removes")
                    .isEqualTo(1);

            List<Long> offsets = new ArrayList<>();
            for (ConsumerClient client : clients) {
                offsets.add(client.readNext(Duration.ofSeconds(5)).orElseThrow().offset());
            }
            assertThat(offsets)
                    .as("and every run still gets its OWN delivery with its own offsets, "
                            + "routed to the client that holds it -- merging the bytes must "
                            + "not merge the streams")
                    .containsExactly(100L, 200L, 300L);
        }
    }

    /**
     * A stream opened later does not re-register the ones already held
     * (M5.62).
     *
     * <p>⚠️ THE FIRST DESIGN REBUILT THE WHOLE SUBSCRIPTION whenever the key
     * set moved, and review found a major in the window between the two calls:
     * opening the new registration first means every key carried by both is
     * registered TWICE for as long as the window lasts, and nothing on the
     * consumer path dedups a repeated {@code Delivery} -- {@code decodeInto}
     * re-emits the same records at the same offsets. Closing the old one first
     * drops whatever arrives in the gap instead. {@code add} touches one key,
     * so neither window exists.
     *
     * <p>⚠️ COUNTED PER KEY, NOT IN TOTAL. A total would be satisfied by a
     * rebuild that happened to register the same number of times; what must
     * hold is that a key already held is never registered again.
     */
    @Test
    void aStreamOpenedLaterDoesNotREREGISTERTheOnesAlreadyHeld() {
        SubscriptionHub hub = new SubscriptionHub();
        java.util.Map<RunKey, Integer> registrations = new java.util.HashMap<>();
        MergingHubTransport transport =
                new MergingHubTransport(hub, new AtomicInteger(), registrations);
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            RUNS.forEach(node::clientFor);

            assertThat(registrations)
                    .as("each stream is registered EXACTLY once, however many opened after it "
                            + "-- a rebuild registers the first key three times")
                    .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                            RUNS.get(0), 1, RUNS.get(1), 1, RUNS.get(2), 1));
            assertThat(hub.subscriberCount(RUNS.get(0)))
                    .as("and the hub holds ONE subscription for the first stream, not one per "
                            + "later arrival -- a duplicate registration is a duplicate push")
                    .isEqualTo(1);
        }
    }

    private static byte[] segmentWithARunPerKey() throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : RUNS) {
            writer.add(key, new SegmentRecord("doc-" + key.partitionId(), OpType.INDEX,
                    OptionalLong.of(1),
                    ("{\"p\":" + key.partitionId() + "}").getBytes(StandardCharsets.UTF_8)), 7L);
        }
        return writer.toByteArray(11L);
    }

    private static SegmentServing serving(BinStore store) {
        return new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L << 30, 1L << 30, 1000, false)),
                store.capabilities(), new SegmentProxy(store));
    }
}
