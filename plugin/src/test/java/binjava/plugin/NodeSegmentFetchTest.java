// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.ConsumerClient;
import binjava.client.ConsumerRecord;
import binjava.client.Delivery;
import binjava.client.SegmentSource;
import binjava.client.SubscriptionTransport;
import binjava.format.FetchMode;
import binjava.format.Grant;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * One fetch per (node, segment), and every run decodes its own records from it
 * (M5.45h, FR-6, cost.md R5).
 *
 * <p>⚠️ K IS 64 RUNS ON ONE NODE, and the shape it refuses is the one the
 * seam permits: {@code direct} gives a node one {@code Delivery} per RUN, each
 * carrying the same key-scoped grant, so a {@link SegmentSource} owned per
 * client fetches once per run -- ~400 whole-object GETs for one 8 MiB segment
 * on a catch-up node. Shards-per-node, which non-negotiable 6 forbids by name,
 * and invisible to the ingester's own meter because a consumer-side GET
 * happens in another process.
 *
 * <p>⚠️ BOTH HALVES, BECAUSE THE FETCH COUNT ALONE IS NOT A CRITERION.
 * Coalescing by letting the first delivery fetch and handing the other 63 an
 * empty array gives a count of 1 and ingests nothing for 63 shards. Every case
 * that counts fetches also reads the records back, per run.
 *
 * <p>⚠️ THE COUNT IS OF DELEGATE INVOCATIONS, NOT OF DISTINCT URLS. All K
 * deliveries carry the same {@link Grant}, which is a record with value
 * equality, so a set of requested urls has size 1 under the per-run
 * implementation too -- that collapse was measured on M5.45d.
 */
class NodeSegmentFetchTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final String SEGMENT_KEY = "seg-with-sixty-four-runs";
    private static final int K = 64;

    /** ⚠️ Serves the bytes it was built with and COUNTS what it was asked. */
    private static final class CountingSource implements SegmentSource {
        private final Map<String, byte[]> objects;
        private final IOException failWith;
        private int fetches;

        CountingSource(Map<String, byte[]> objects) {
            this(objects, null);
        }

        CountingSource(Map<String, byte[]> objects, IOException failWith) {
            this.objects = objects;
            this.failWith = failWith;
        }

        @Override
        public byte[] fetch(Grant grant) throws IOException {
            fetches++;
            if (failWith != null) {
                throw failWith;
            }
            byte[] bytes = objects.get(grant.url());
            if (bytes == null) {
                throw new IOException("no object behind " + grant.url());
            }
            return bytes;
        }

        int fetches() {
            return fetches;
        }
    }

    /**
     * A transport that delivers what a test hands it, to the node's ONE
     * listener.
     *
     * <p>⚠️ IT IS THE NODE'S REGISTRATION THAT ROUTES, not this fake: every
     * delivery goes to the single listener {@link NodeSubscriptions} holds, and
     * that listener looks the key up in the node's own client map. A fake that
     * routed per key would be asserting its own dispatch.
     */
    private static final class OneListenerTransport implements SubscriptionTransport {
        private Listener listener;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener l) {
            return subscribe(List.of(key), l);
        }

        @Override
        public MultiSubscription subscribe(List<RunKey> keys, Listener l) {
            this.listener = l;
            return new MultiSubscription() {
                @Override
                public void add(RunKey key) {
                }

                @Override
                public void remove(RunKey key) {
                }

                @Override
                public void close() {
                }
            };
        }

        void deliver(Delivery delivery) {
            listener.onDelivery(delivery);
        }
    }

    private static Grant grantFor(String segmentKey) {
        return new Grant("https://store.example/" + segmentKey,
                Instant.EPOCH.plus(Duration.ofSeconds(60)));
    }

    private static List<RunKey> runs(int k) {
        List<RunKey> keys = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            keys.add(new RunKey(INDEX, i));
        }
        return keys;
    }

    /** One record per run, its payload naming the run it belongs to. */
    private static byte[] segmentWithARunPerKey(List<RunKey> keys) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord("doc-" + key.partitionId(), OpType.INDEX,
                    OptionalLong.of(1),
                    ("run-" + key.partitionId()).getBytes(StandardCharsets.UTF_8)), 1_000L);
        }
        return writer.toByteArray(1_000L);
    }

    private static Delivery directDelivery(RunKey key, String segmentKey) {
        return new Delivery(key, segmentKey, 1, 10L * key.partitionId(), FetchMode.DIRECT,
                new byte[0], grantFor(segmentKey));
    }

    @Test
    void SIXTYFOURRunsOfONESegmentCostONEFetchAndEACHDecodesItsOWNRecords() throws Exception {
        List<RunKey> keys = runs(K);
        byte[] segment = segmentWithARunPerKey(keys);
        CountingSource delegate = new CountingSource(
                Map.of(grantFor(SEGMENT_KEY).url(), segment));
        NodeSegmentSource nodeSource = new NodeSegmentSource(delegate, 1L << 20);
        OneListenerTransport transport = new OneListenerTransport();

        List<String> read = new ArrayList<>();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16, nodeSource)) {
            List<ConsumerClient> clients = new ArrayList<>();
            for (RunKey key : keys) {
                clients.add(node.clientFor(key));
            }
            for (RunKey key : keys) {
                transport.deliver(directDelivery(key, SEGMENT_KEY));
            }
            for (ConsumerClient client : clients) {
                Optional<ConsumerRecord> record = client.readNext(Duration.ofSeconds(5));
                read.add(new String(record.orElseThrow().record().payload(),
                        StandardCharsets.UTF_8));
            }
        }

        assertThat(delegate.fetches())
                .as("one fetch operation per (node, segment), NOT one per client -- %d runs of "
                        + "one object on a catch-up node is shards-per-node scaling, and no "
                        + "meter in the ingester can see a consumer-side GET", K)
                .isEqualTo(1);
        assertThat(nodeSource.fetches())
                .as("and the node's own counter agrees with the delegate's")
                .isEqualTo(1);
        List<String> expected = new ArrayList<>();
        keys.forEach(key -> expected.add("run-" + key.partitionId()));
        assertThat(read)
                .as("EVERY run decodes its OWN records: coalescing by letting the first "
                        + "delivery fetch and handing the other %d an empty array gives a "
                        + "fetch count of 1 and ingests nothing for %d shards", K - 1, K - 1)
                .containsExactlyElementsOf(expected);
    }

    /**
     * The hold is bounded IN BYTES and evicts the LEAST RECENTLY USED
     * (M5.45h criterion 2).
     *
     * <p>⚠️ A HOLD IS A NEW MEMORY BOUND, NOT A FREE ONE. Sixty-four
     * deliveries arrive at 64 independent {@code readNext} calls, so unioning
     * them needs a per-node hold of the segment -- and M5's criterion 6 asks
     * only that memory be flat in K, which a hold of C is at every C. So the
     * ceiling is asserted here, with its eviction.
     *
     * <p>⚠️ FOUR SEGMENTS OF FOUR DIFFERENT SIZES, and round 2 measured why
     * equal ones are not enough: with every segment the same length, a
     * {@code held.size() > 2} ceiling -- byte-unbounded, two 8 MiB segments
     * under a 10 MiB bound -- survived the whole suite, because two equal
     * entries are always exactly twice one. The {@code + 7} defeats a
     * capacity-divided-by-segment-size implementation and does nothing to
     * entry counting.
     *
     * <p>⚠️ THE SIZE RELATIONS ARE ASSERTED AS PREMISES rather than assumed
     * from the run counts: a change to the segment format moves all four
     * lengths, and a fixture that quietly stopped forcing an eviction would
     * pass while measuring nothing.
     */
    @Test
    void theNODESHoldIsBOUNDEDInBYTESAndEvictsTheLEASTRecentlyUsed() throws Exception {
        byte[] a = segmentWithARunPerKey(runs(1));
        byte[] b = segmentWithARunPerKey(runs(8));
        byte[] c = segmentWithARunPerKey(runs(4));
        byte[] d = segmentWithARunPerKey(runs(9));
        Map<String, byte[]> objects = new LinkedHashMap<>();
        objects.put(grantFor("seg-a").url(), a);
        objects.put(grantFor("seg-b").url(), b);
        objects.put(grantFor("seg-c").url(), c);
        objects.put(grantFor("seg-d").url(), d);
        CountingSource delegate = new CountingSource(objects);
        // ⚠️ NOT A MULTIPLE OF ANY SEGMENT: a ceiling that happened to be
        // exactly 2x one of them would pass under an implementation dividing
        // the capacity by a segment size.
        long capacity = a.length + b.length + 7L;
        NodeSegmentSource nodeSource = new NodeSegmentSource(delegate, capacity);

        assertThat(a.length + (long) b.length)
                .as("PREMISE: a and b fit together, or nothing is held to evict")
                .isLessThanOrEqualTo(capacity);
        assertThat(b.length + (long) c.length)
                .as("PREMISE: b and c do NOT fit, so admitting c forces an eviction")
                .isGreaterThan(capacity);
        assertThat(a.length + (long) c.length)
                .as("PREMISE: a and c DO fit, so exactly ONE entry goes and WHICH one is "
                        + "what the next assertions read")
                .isLessThanOrEqualTo(capacity);
        assertThat(a.length + (long) d.length)
                .as("PREMISE: a and d do NOT fit together while d alone does -- this is what "
                        + "forces the hold down to ONE entry, which an entry-counting ceiling "
                        + "never does")
                .isGreaterThan(capacity);
        assertThat((long) d.length).isLessThanOrEqualTo(capacity);

        nodeSource.fetch(grantFor("seg-a"));
        nodeSource.fetch(grantFor("seg-b"));
        // ⚠️ TOUCHES a, so the least recently used is now b and NOT the entry
        // that arrived first -- insertion order and access order disagree from
        // here on, which is what makes this case tell them apart.
        nodeSource.fetch(grantFor("seg-a"));
        nodeSource.fetch(grantFor("seg-c"));
        long afterC = delegate.fetches();

        nodeSource.fetch(grantFor("seg-a"));
        assertThat(delegate.fetches() - afterC)
                .as("a was touched most recently of the two and is still held -- evicting by "
                        + "INSERTION order would drop it, the segment this node is actively "
                        + "reading")
                .isZero();
        nodeSource.fetch(grantFor("seg-b"));
        assertThat(delegate.fetches() - afterC)
                .as("b is the one that fell out")
                .isEqualTo(1);

        // ⚠️ THE SECOND PHASE IS THE BYTE CEILING ITSELF: d forces the hold
        // down to ONE entry, which a ceiling counting ENTRIES never does.
        nodeSource.fetch(grantFor("seg-d"));

        assertThat(nodeSource.bytesHeld())
                .as("the hold never exceeds its ceiling of %d bytes -- a ceiling in ENTRIES "
                        + "holds two segments of any size, which for 8 MiB segments under a "
                        + "10 MiB bound is a heap an operator sized for one", capacity)
                .isLessThanOrEqualTo(capacity);
    }

    /**
     * A segment too large for the hold is still served, and it does not
     * EVICT WHAT THE NODE IS READING (M5.45h, round 1's test major).
     *
     * <p>⚠️ THE OBVIOUS VERSION OF THIS CASE PASSES WITHOUT THE GUARD. Admit
     * an oversize segment and the eviction loop removes it again, so
     * {@code bytesHeld()} reads 0 either way -- but on the way it evicts EVERY
     * segment the node was holding first, and all of their runs re-fetch. So
     * the hold is warmed with a small segment before the oversize one arrives,
     * and what is asserted is that the small one survives.
     */
    @Test
    void aSegmentTOOLARGEForTheHoldIsSERVEDWithoutEVICTINGWhatTheNodeIsReading()
            throws Exception {
        byte[] small = segmentWithARunPerKey(runs(1));
        byte[] large = segmentWithARunPerKey(runs(K));
        Map<String, byte[]> objects = new LinkedHashMap<>();
        objects.put(grantFor("seg-small").url(), small);
        objects.put(grantFor("seg-large").url(), large);
        CountingSource delegate = new CountingSource(objects);
        // ⚠️ ROOM FOR THE SMALL ONE AND NOT THE LARGE ONE, which is the whole
        // fixture: `maxSegmentBytes` is a flush TRIGGER rather than a ceiling,
        // so a segment larger than a node's hold is a normal state.
        NodeSegmentSource nodeSource = new NodeSegmentSource(delegate, large.length - 1L);
        nodeSource.fetch(grantFor("seg-small"));
        long afterWarming = delegate.fetches();

        byte[] served = nodeSource.fetch(grantFor("seg-large"));

        assertThat(served)
                .as("refusing to return a segment the node fetched successfully would fail a "
                        + "read for a hold that is merely too small")
                .isEqualTo(large);
        assertThat(nodeSource.bytesHeld())
                .as("the oversize segment is NOT held: admitting it would break the very "
                        + "ceiling the hold exists to keep")
                .isLessThanOrEqualTo(nodeSource.capacityBytes());
        nodeSource.fetch(grantFor("seg-small"));
        assertThat(delegate.fetches() - afterWarming)
                .as("ONE fetch since warming -- the oversize one. Admitting it first would "
                        + "evict every segment this node was holding, so the hot segment's "
                        + "runs all re-fetch and the oversize arrival costs far more than the "
                        + "one request it looks like")
                .isEqualTo(1);
    }

    /**
     * One failed fetch fails EVERY run the node holds (M5.45h criterion 3).
     *
     * <p>⚠️ IT MUST TELL "fails every run" FROM "quietly empties every run",
     * which a stated-only criterion cannot. The second advances K offsets past
     * records nobody read -- silent data loss, and the same shape M5.45g's
     * criterion 5 names one layer down.
     */
    @Test
    void aFAILEDFetchFAILSEveryRunRatherThanEMPTYINGThem() throws Exception {
        List<RunKey> keys = runs(4);
        CountingSource delegate = new CountingSource(Map.of(),
                new IOException("the grant expired"));
        NodeSegmentSource nodeSource = new NodeSegmentSource(delegate, 1L << 20);
        OneListenerTransport transport = new OneListenerTransport();

        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16, nodeSource)) {
            List<ConsumerClient> clients = new ArrayList<>();
            for (RunKey key : keys) {
                clients.add(node.clientFor(key));
            }
            for (RunKey key : keys) {
                transport.deliver(directDelivery(key, SEGMENT_KEY));
            }
            for (ConsumerClient client : clients) {
                assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(5)))
                        .as("an empty Optional here is an ordinary empty poll: the caller is "
                                + "told the stream is healthy and a whole window is gone")
                        .isInstanceOf(UncheckedIOException.class)
                        .hasMessageContaining("the grant expired");
            }
        }

        assertThat(delegate.fetches())
                .as("a failure is not held, so each run tries again rather than inheriting an "
                        + "empty array -- %d runs, %d attempts", keys.size(), keys.size())
                .isEqualTo(keys.size());
    }
}
