// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * The consumer resolves a {@code proxy} delivery through the ingester's
 * segment route (M10.2, ADR-0073, M10 criterion 3).
 *
 * <p>⚠️ BEFORE M10 A {@code PROXY} EVENT ARRIVING OVER HTTP CARRIED ONLY
 * COORDINATES, and {@code ConsumerClient} decoded its empty inline array --
 * a segment that fails its footer check, so a node indexing above the inline
 * cap was indexing nothing. The fake below serves bytes for ONE key and throws
 * for every other, so the key the client asks for is pinned rather than
 * assumed.
 */
class ProxyFetchTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000ab");
    private static final RunKey KEY = new RunKey(A, 0);
    private static final String SEGMENT = "bins/c/data/the-one-proxied.bseg";

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

    /** Serves ONE segment key by proxy, and refuses grants outright. */
    private static final class ServesOneKey implements SegmentSource {
        private final String key;
        private final byte[] bytes;
        final List<String> asked = new CopyOnWriteArrayList<>();

        ServesOneKey(String key, byte[] bytes) {
            this.key = key;
            this.bytes = bytes;
        }

        @Override public byte[] fetch(Grant grant) throws IOException {
            throw new IOException("a proxy delivery never fetches under a grant");
        }

        @Override public byte[] fetchSegment(String segmentKey) throws IOException {
            asked.add(segmentKey);
            if (!key.equals(segmentKey)) {
                throw new IOException("no segment " + segmentKey);
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

    private static Delivery proxied(long firstOffset, int recordCount) {
        return new Delivery(KEY, SEGMENT, recordCount, firstOffset, FetchMode.PROXY,
                new byte[0]);
    }

    @Test
    void aPROXYDeliveryDecodesFromTheBytesTheRouteReturns() throws Exception {
        ServesOneKey source = new ServesOneKey(SEGMENT, segmentOf("a", "b"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, source, TestRetries.noFailedFetch())) {
            transport.push(proxied(40, 2));

            ConsumerRecord first = c.readNext(Duration.ofMillis(200)).orElseThrow();
            ConsumerRecord second = c.readNext(Duration.ofMillis(200)).orElseThrow();
            assertThat(first.offset()).isEqualTo(40);
            assertThat(first.record().id()).isEqualTo("a");
            assertThat(second.record().id()).isEqualTo("b");

            // ⚠️ AN IN-PROCESS SUBSCRIBER is handed the assembled bytes by the
            // serving path; fetching them again would be a second read of a
            // segment already in hand.
            transport.push(new Delivery(KEY, SEGMENT, 1, 42, FetchMode.PROXY,
                    segmentOf("carried")));
            assertThat(c.readNext(Duration.ofMillis(200)).orElseThrow().record().id())
                    .isEqualTo("carried");
        }
        assertThat(source.asked).as("one fetch, for the delivery that carried nothing")
                .containsExactly(SEGMENT);
    }

    @Test
    void aPROXYDeliveryToAConsumerWithNoSourceFailsLoudly() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, null, TestRetries.noFailedFetch())) {
            transport.push(proxied(0, 1));

            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(200)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("proxy");
        }
    }

    @Test
    void aSourceThatCannotProxyRefusesRatherThanYieldingNothing() {
        SegmentSource grantsOnly = grant -> new byte[] {1};

        assertThatThrownBy(() -> grantsOnly.fetchSegment(SEGMENT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(SEGMENT);
    }
}
