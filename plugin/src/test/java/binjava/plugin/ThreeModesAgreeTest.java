// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.binstore.SignedUrl;
import binjava.binstore.Version;
import binjava.binstore.backend.MemoryBinStore;
import binjava.client.ConsumerClient;
import binjava.client.ConsumerRecord;
import binjava.client.Delivery;
import binjava.client.SegmentSource;
import binjava.client.SubscriptionTransport;
import binjava.format.CommitDelta;
import binjava.format.FetchMode;
import binjava.format.Grant;
import binjava.format.OpType;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import binjava.ingest.FetchPolicy;
import binjava.ingest.FetchPolicyConfig;
import binjava.ingest.GrantIssuer;
import binjava.ingest.SegmentProxy;
import binjava.ingest.SegmentServing;
import binjava.ingest.SubscriptionHub;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * SPEC criterion 5: all THREE modes deliver byte-identical records (M5.45e).
 *
 * <p>⚠️ THE ASSERTION THE MILESTONE IS FOR, and it could not be made until both
 * halves existed: M5.45d taught the hub to mint a grant and serve {@code
 * direct}, M5.45g gave the consumer a {@link SegmentSource} to fetch with.
 *
 * <p>⚠️ DRIVEN THROUGH {@link ConsumerClient}, NOT THROUGH THE HUB, which is
 * criterion 1's actual demand: the client's own decode has to be in the path,
 * because that is where a mode difference would show up as different RECORDS
 * rather than as different bytes on a sink. Asserting at the hub would compare
 * what the hub wrote, which is the same array by construction on two of the
 * three modes and proves nothing about the third.
 *
 * <p>⚠️ IT LIVES IN {@code plugin} BECAUSE THIS MODULE ALREADY SEES BOTH HALVES
 * — {@code api(client)} plus {@code testImplementation(ingest)} — and
 * {@code EndToEndTest} already carries the same hub-to-transport bridge. An
 * earlier version of this class sat in {@code http} under a test-only edge to
 * {@code client}, on the stated premise that NO module saw both. That premise
 * was false, and the edge would have been the first FORWARD one in the tree:
 * every other dependency, test or production, points backwards along
 * {@code settings.gradle.kts} order, and that one ran from the producer-side
 * {@code _bulk} adapter to the consumer library. M5.45's note that the bridge
 * has "no production home" means no {@code src/main} home; two test ones
 * already existed.
 *
 * <p>⚠️ THE URL BINDING IS NOT ASSERTED HERE. The source below strips a known
 * prefix, so it serves any url ending in the right key — review measured that
 * an issuer which never signs, self-constructing the same string, passes this
 * class. That is deliberate: the binding between the minted url and the bytes
 * is {@code DirectFetchTest}'s (M5.45g criterion 4), where the source serves ONE
 * url and throws for any other, and ingester-side it is held by
 * {@code GrantIssuerTest}, {@code DirectServingTest} and
 * {@code GrantNeverLoggedTest}. What THIS class owns is that the three modes
 * agree.
 */
class ThreeModesAgreeTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(INDEX, 0);
    private static final String SEGMENT_KEY = "seg-the-one-under-test";

    /** A backend that can presign, which neither shipping one can. */
    private static final class CanPresign implements BinStore {
        private final BinStore delegate = new MemoryBinStore();

        @Override public SignedUrl presign(String key, Duration ttl) {
            return new SignedUrl("https://store.example/" + key, java.time.Instant.EPOCH.plus(ttl));
        }

        @Override public Capabilities capabilities() {
            Capabilities real = delegate.capabilities();
            return new Capabilities(real.conditionalWrites(), real.batchDelete(), true,
                    real.maxKeyBytes(), real.minPartSize(), real.costs());
        }

        @Override public InputStream get(String k) throws IOException { return delegate.get(k); }

        @Override public InputStream getRange(String k, long s, long e) throws IOException {
            return delegate.getRange(k, s, e);
        }

        @Override public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v)
                throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public ListPage list(String p, String after, int max) throws IOException {
            return delegate.list(p, after, max);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * The bridge M5.45 recorded as having no production home.
     *
     * <p>It converts {@code SubscriptionHub.Push} into {@code client.Delivery}
     * and carries the grant across, which is the whole of what {@code direct}
     * needs from a transport.
     */
    private static final class HubBridge implements SubscriptionTransport {
        private final SubscriptionHub hub;
        final List<FetchMode> modesSeen = new CopyOnWriteArrayList<>();

        HubBridge(SubscriptionHub hub) {
            this.hub = hub;
        }

        @Override public AutoCloseable subscribe(RunKey key, Listener listener) {
            return hub.subscribe(key, SubscriptionHub.assembling(push -> {
                modesSeen.add(push.via());
                listener.onDelivery(new Delivery(push.key(), push.segmentKey(),
                        push.recordCount(), push.firstOffset(), push.via(),
                        push.segment(), push.grant(), push.sequencerEpoch()));
            }));
        }
    }

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 7L);
        }
        return w.toByteArray(11L);
    }

    private static SegmentServing serving(BinStore store, long inlineCap, int fanOutThreshold,
            boolean directEnabled) {
        FetchPolicyConfig config = new FetchPolicyConfig(inlineCap, inlineCap,
                fanOutThreshold, directEnabled);
        return directEnabled
                ? new SegmentServing(new FetchPolicy(config), store.capabilities(),
                        new SegmentProxy(store), new GrantIssuer(store))
                : new SegmentServing(new FetchPolicy(config), store.capabilities(),
                        new SegmentProxy(store));
    }

    /**
     * Drives one publish all the way to decoded records, through the client.
     *
     * <p>⚠️ {@code expected} IS ASSERTED, NOT DOCUMENTED. Without it all three
     * calls could serve the SAME mode and the byte-identity assertion would hold
     * trivially — it would be comparing inline against inline against inline.
     * The mode is read off the push the bridge saw, so it is the mode the
     * CONSUMER was actually told.
     */
    private static List<ConsumerRecord> recordsThrough(SegmentServing serving, BinStore store,
            byte[] segment, boolean podHoldsBytes, FetchMode expected) throws Exception {
        store.put(SEGMENT_KEY, Body.ofBytes(segment));
        SubscriptionHub hub = new SubscriptionHub();
        HubBridge bridge = new HubBridge(hub);
        SegmentSource source = grant -> {
            try (InputStream in = store.get(
                    grant.url().substring("https://store.example/".length()))) {
                return in.readAllBytes();
            }
        };
        List<ConsumerRecord> got = new ArrayList<>();
        try (ConsumerClient c = new ConsumerClient(bridge, KEY, 16, source)) {
            hub.publish(new CommitDelta(0, SEGMENT_KEY,
                            List.of(new RunCommit(KEY, 3, 500L))),
                    podHoldsBytes ? SEGMENT_KEY : "seg-some-other-pod-wrote",
                    podHoldsBytes ? segment : new byte[] {1},
                    serving);
            for (int i = 0; i < 3; i++) {
                got.add(c.readNext(Duration.ofMillis(200)).orElseThrow());
            }
        }
        assertThat(bridge.modesSeen)
                .as("the mode the consumer was actually served")
                .isNotEmpty()
                .containsOnly(expected);
        return got;
    }

    /**
     * Every field of the decoded record, because the criterion says BYTE FOR BYTE.
     *
     * <p>⚠️ AN EARLIER VERSION PROJECTED offset, id and payload ONLY, and review
     * measured what that let through: {@code decodeInto}'s
     * {@code reader.createdAtMillis()} replaced by {@code 0L} survives, and so
     * would a dropped {@code opType} or {@code version}. ⚠️ THE CROSS-MODE
     * COMPARISON CANNOT BACKSTOP THAT BY CONSTRUCTION — all three modes run the
     * same {@code decodeInto}, so a field dropped in one is dropped identically
     * in all three and the {@code isEqualTo} chain still holds. Only naming the
     * fields catches it, and the absolute {@code containsExactly} below is what
     * gives the names teeth.
     */
    private static List<String> bodies(List<ConsumerRecord> records) {
        List<String> out = new ArrayList<>();
        for (ConsumerRecord r : records) {
            out.add(r.offset() + ":" + r.record().id() + ":" + r.record().opType()
                    + ":" + r.record().version() + ":" + r.timestampMillis() + ":"
                    + new String(r.record().payload(), StandardCharsets.UTF_8));
        }
        return out;
    }

    /**
     * Criterion 5: the same commit through all three modes yields the same records.
     *
     * <p>⚠️ COMPARED AS DECODED RECORDS, not as segment bytes. Two of the three
     * modes hand the consumer the same array by construction, so comparing
     * arrays would pass without the client ever running — and {@code direct} is
     * the one where the array arrives EMPTY and the bytes come from a fetch, so
     * it is the only mode where the client's decode is load-bearing.
     */
    @Test
    void allTHREEModesDeliverBYTEIdenticalRecords() throws Exception {
        byte[] segment = segmentOf("a", "b", "c");

        try (CanPresign inlineStore = new CanPresign();
                CanPresign proxyStore = new CanPresign();
                CanPresign directStore = new CanPresign()) {

            // inline: this pod holds the bytes and they fit under the cap.
            List<ConsumerRecord> inline = recordsThrough(
                    serving(inlineStore, 1L << 20, 1, false), inlineStore, segment, true, FetchMode.INLINE);

            // proxy: this pod does NOT hold them, so the ingester streams them.
            List<ConsumerRecord> proxy = recordsThrough(
                    serving(proxyStore, 1L, 1, false), proxyStore, segment, false, FetchMode.PROXY);

            // direct: cold, direct enabled, fan-out below the threshold.
            List<ConsumerRecord> direct = recordsThrough(
                    serving(directStore, 1L, 4, true), directStore, segment, false, FetchMode.DIRECT);

            assertThat(bodies(inline))
                    .as("inline decodes the commit's three records at the log's offsets")
                    .containsExactly(
                            "500:a:INDEX:OptionalLong[1]:11:{\"id\":\"a\"}",
                            "501:b:INDEX:OptionalLong[1]:11:{\"id\":\"b\"}",
                            "502:c:INDEX:OptionalLong[1]:11:{\"id\":\"c\"}");
            assertThat(bodies(proxy))
                    .as("`proxy` delivers what `inline` delivered, byte for byte")
                    .isEqualTo(bodies(inline));
            assertThat(bodies(direct))
                    .as("and `direct`, where the bytes came from the consumer's own fetch")
                    .isEqualTo(bodies(inline));
        }
    }

    /**
     * Criterion 2: at a fan-out above the threshold, {@code direct} is not served.
     *
     * <p>⚠️ OBSERVED FROM OUTSIDE, which is what makes this more than a restatement
     * of {@code FetchPolicy}'s unit tests: the mode the CONSUMER is told is read
     * off the delivery that reached it, not off the policy that chose it. A
     * consumer cannot ask for a mode (FR-6), and this is the property that makes
     * ADR-0004's rejected $3,732/month shape unreachable from the client side.
     */
    @Test
    void aFanOutABOVETheThresholdIsNOTServedDIRECT() throws Exception {
        byte[] segment = segmentOf("a", "b", "c");
        try (CanPresign store = new CanPresign()) {
            store.put(SEGMENT_KEY, Body.ofBytes(segment));
            SubscriptionHub hub = new SubscriptionHub();
            HubBridge bridge = new HubBridge(hub);
            SegmentSource neverCalled = grant -> {
                throw new AssertionError("direct must not be served at this fan-out");
            };

            try (ConsumerClient one = new ConsumerClient(bridge, KEY, 16, neverCalled);
                    ConsumerClient two = new ConsumerClient(bridge, KEY, 16, neverCalled)) {
                hub.publish(new CommitDelta(0, SEGMENT_KEY,
                                List.of(new RunCommit(KEY, 3, 500L))),
                        "seg-some-other-pod-wrote", new byte[] {1},
                        serving(store, 1L, 1, true));

                assertThat(one.readNext(Duration.ofMillis(200))).isPresent();
                assertThat(two.readNext(Duration.ofMillis(200))).isPresent();
            }

            assertThat(bridge.modesSeen)
                    .as("two consumers is above a threshold of 1, so the policy served PROXY")
                    .isNotEmpty()
                    .doesNotContain(FetchMode.DIRECT);
        }
    }
}
