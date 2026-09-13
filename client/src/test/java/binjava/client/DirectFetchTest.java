// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.FetchMode;
import binjava.format.Grant;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * The client-side byte source for {@code direct} (M5.45g, FR-6).
 *
 * <p>⚠️ THE SEAM IS NOT A {@code BinStore}: ADR-0023 keeps
 * {@code binjava.binstore} out of {@code client}, so {@code direct} needs a
 * source that takes a {@link Grant} — which lives in {@code format}, where
 * {@code client} can reach it.
 *
 * <p>⚠️ THE FETCHER SERVES ONLY THE EXACT URL IT IS HANDED, and that is what
 * makes these cases worth having. Review measured the alternative: a
 * deterministic single-segment fetcher answers identically whatever url it is
 * given, so a {@code ConsumerClient} passing a LOCALLY CONSTRUCTED grant — or
 * ignoring the parameter entirely — decodes correctly while production would
 * fetch an unsigned or wrong-key URL. Counting requests misses it and counting
 * decoded bytes misses it; only binding the bytes to the url does not.
 */
class DirectFetchTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(A, 0);
    private static final Grant SECOND_GRANT =
            new Grant("https://store.example/seg-the-second", Instant.ofEpochMilli(60_000L));
    private static final Grant GRANT =
            new Grant("https://store.example/seg-the-one-granted", Instant.ofEpochMilli(60_000L));

    private static final class FakeTransport implements SubscriptionTransport {
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();

        @Override public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        void push(Delivery d) {
            listeners.forEach(l -> l.onDelivery(d));
        }
    }

    /** Serves bytes for ONE url and throws for every other, so the url is pinned. */
    private static final class ServesOneUrl implements SegmentSource {
        private final String url;
        private final byte[] bytes;
        final List<String> asked = new CopyOnWriteArrayList<>();

        ServesOneUrl(String url, byte[] bytes) {
            this.url = url;
            this.bytes = bytes;
        }

        @Override public byte[] fetch(Grant grant) throws IOException {
            asked.add(grant.url());
            if (!url.equals(grant.url())) {
                throw new IOException("no object at " + grant.url());
            }
            return bytes;
        }
    }

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery direct(long firstOffset, int recordCount) {
        return new Delivery(KEY, "seg-the-one-granted", recordCount, firstOffset,
                FetchMode.DIRECT, new byte[0], GRANT);
    }

    /**
     * A {@code direct} delivery is refused where it is BUILT when it has no grant.
     *
     * <p>⚠️ THE SAME SHAPE AS {@code SubscriptionHub.Push}'s guards ONE LAYER UP,
     * and the asymmetry M5.44 closed three times over: a consumer told to fetch
     * and not told how learns a segment exists and can do nothing with it.
     */
    @Test
    void aDIRECTDeliveryWithoutAGrantIsRefusedWhereItIsBUILT() {
        assertThatThrownBy(() -> new Delivery(KEY, "seg", 1, 0L, FetchMode.DIRECT, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without a grant");
    }

    /**
     * And a grant on a non-{@code direct} delivery is refused.
     *
     * <p>⚠️ security.md rule 4 makes an unnecessary secret a COST rather than
     * waste: a signed URL minted for a consumer that will never fetch with it
     * is one more place it can leak from.
     */
    @Test
    void aGrantOnANonDIRECTDeliveryIsRefused() {
        assertThatThrownBy(() ->
                new Delivery(KEY, "seg", 1, 0L, FetchMode.INLINE, new byte[0], GRANT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a grant is for `direct`");

        // ⚠️ AND PROXY, WHICH IS THE MODE WHERE THE CONSUMER DEFINITIVELY NEVER
        // FETCHES. Review measured `via != DIRECT` -> `via == INLINE` surviving
        // the whole suite, because nothing anywhere built a client-side
        // delivery with PROXY -- so a proxy delivery carrying a signed URL was
        // accepted, the exact state this guard's rule-4 reasoning forbids.
        assertThatThrownBy(() ->
                new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0], GRANT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a grant is for `direct`");
    }

    /**
     * Records decode through the seam, and the bytes come from the FETCHER.
     *
     * <p>⚠️ BYTES DECODED, NOT CALLS MADE. A counting fake counts REQUESTS, so a
     * source returning a prefix that every real delivery would throw on still
     * satisfies a request-rate assertion. The segment arrives EMPTY on a
     * {@code direct} delivery, so anything decoded came from the fetch.
     */
    @Test
    void recordsDECODEFromTheBytesTheSEAMReturNS() throws Exception {
        ServesOneUrl source = new ServesOneUrl(GRANT.url(), segmentOf("a", "b", "c"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, source)) {
            transport.push(direct(100, 3));

            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(100);
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(101);
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(102);
        }
    }

    /**
     * The seam is NOT used when {@code via} is not {@code DIRECT}.
     *
     * <p>⚠️ THE BRANCH WAS EXECUTED CONSTANTLY AND CONSTRAINED NEVER. Review
     * measured it against the whole suite: hoist the null-source check above
     * the {@code via} check, so a configured source wins for EVERY delivery,
     * and every module stays green — because every client built with a source
     * pushed only {@code DIRECT}, and every case pushing {@code INLINE} used
     * the three-argument constructor where the source is null. Once M5.45h
     * wires a real source that is a consumer-side GET on every inline
     * delivery, which is non-negotiable 6, plus an NPE on the null grant.
     */
    @Test
    void theSEAMIsNOTUsedWhenViaIsNotDIRECT() throws Exception {
        ServesOneUrl source = new ServesOneUrl(GRANT.url(), segmentOf("x"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, source)) {
            transport.push(new Delivery(KEY, "seg", 2, 50L, FetchMode.INLINE,
                    segmentOf("a", "b")));

            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(50);
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(51);
        }
        assertThat(source.asked)
                .as("an inline delivery carries its bytes; the seam must not be reached")
                .isEmpty();
    }

    /**
     * The seam is handed the grant FROM THE DELIVERY, not one built locally.
     *
     * <p>⚠️ THIS IS THE CASE A REQUEST COUNT AND A BYTE COUNT BOTH MISS. The
     * fetcher above serves one url and throws for any other, so a
     * {@code ConsumerClient} that constructed its own grant, or ignored the
     * parameter, would fail to decode rather than quietly fetch the wrong
     * object — and the observed url is asserted equal to the minted one so the
     * binding is visible rather than inferred.
     */
    @Test
    void theSEAMIsHandedTheGrantFromTheDELIVERY() throws Exception {
        ServesOneUrl source = new ServesOneUrl(GRANT.url(), segmentOf("a"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, source)) {
            transport.push(direct(7, 1));
            assertThat(c.readNext(Duration.ofMillis(50))).isPresent();

            // ⚠️ A SECOND DELIVERY UNDER A SECOND URL, because with one
            // delivery "the arriving grant" and "the FIRST grant" are
            // indistinguishable -- review measured a client memoizing the first
            // grant and fetching with it forever, green against every case that
            // pushed exactly one.
            transport.push(new Delivery(KEY, "seg-the-second", 1, 8L, FetchMode.DIRECT,
                    new byte[0], SECOND_GRANT));
            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(50)))
                    .as("the second fetch uses the SECOND grant, which this source refuses")
                    .isInstanceOf(java.io.UncheckedIOException.class);
        }
        assertThat(source.asked)
                .as("each fetch uses its own delivery's url, in order")
                .containsExactly("https://store.example/seg-the-one-granted",
                        "https://store.example/seg-the-second");
    }

    /**
     * A fetch that fails PROPAGATES and emits NO records.
     *
     * <p>⚠️ ASSERTED, NOT STATED. Swallowing the failure and returning an empty
     * batch passes every other criterion on this row while the caller is told
     * the stream is healthy and the window is gone — silent data loss. An
     * expired grant and a 403 are the NORMAL failures on this path, not the
     * corrupt-segment case.
     */
    @Test
    void aFAILINGFetchPROPAGATESAndEmitsNORecords() throws Exception {
        ServesOneUrl servesSomethingElse =
                new ServesOneUrl("https://store.example/a-different-object", segmentOf("a"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, servesSomethingElse)) {
            transport.push(direct(100, 1));

            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(50)))
                    .as("the failure reaches the caller rather than reading as an empty poll")
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasMessageContaining("seg-the-one-granted");
        }
    }

    /**
     * A {@code direct} delivery with no seam configured is refused, not dropped.
     *
     * <p>⚠️ THE MIRROR OF {@code SubscriptionHub}'s null-issuer guard: a pod that
     * serves {@code direct} to a consumer built without a source would otherwise
     * have that consumer report an empty, healthy stream forever.
     */
    @Test
    void aDIRECTDeliveryWithNOSeamIsRefused() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            transport.push(direct(100, 1));

            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(50)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no segment source");
        }
    }
}
