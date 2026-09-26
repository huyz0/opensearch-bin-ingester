// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
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
 * A {@code proxy} delivery carries coordinates and no bytes; the consumer
 * fetches the segment through the ingester's route (M10.2, FR-6, ADR-0073).
 */
class ProxyFetchTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000bb"), 0);

    private static final class FakeTransport implements SubscriptionTransport {
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        void push(Delivery d) {
            listeners.forEach(l -> l.onDelivery(d));
        }
    }

    private static final class ServesOneKey implements ProxySource {
        private final String key;
        private final byte[] bytes;
        final List<String> asked = new CopyOnWriteArrayList<>();

        ServesOneKey(String key, byte[] bytes) {
            this.key = key;
            this.bytes = bytes;
        }

        @Override
        public byte[] fetch(String segmentKey) throws IOException {
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

    private static Delivery proxy(String segmentKey, long firstOffset, int recordCount) {
        return new Delivery(KEY, segmentKey, recordCount, firstOffset, FetchMode.PROXY,
                new byte[0]);
    }

    @Test
    void aProxyDeliveryDecodesFromTheBytesTheRouteReturns() throws Exception {
        ServesOneKey source = new ServesOneKey("seg-proxied", segmentOf("a", "b"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, null, source)) {
            transport.push(proxy("seg-proxied", 40, 2));

            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(40);
            assertThat(c.readNext(Duration.ofMillis(50)).orElseThrow().offset()).isEqualTo(41);
        }
        assertThat(source.asked).as("fetched by the delivery's own segment key")
                .containsExactly("seg-proxied");
    }

    @Test
    void aFailedProxyFetchPropagatesAndEmitsNoRecords() throws Exception {
        ServesOneKey source = new ServesOneKey("some-other-segment", segmentOf("a"));
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16, null, source)) {
            transport.push(proxy("seg-proxied", 40, 1));

            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(50)))
                    .as("a failed fetch is never an empty poll")
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasRootCauseMessage("no segment seg-proxied");
        }
        assertThat(source.asked).containsExactly("seg-proxied");
    }

    @Test
    void aProxyDeliveryToAConsumerWithNoProxySourceIsAMisconfigurationNotAnEmptyPoll()
            throws Exception {
        FakeTransport transport = new FakeTransport();
        try (ConsumerClient c = new ConsumerClient(transport, KEY, 16)) {
            transport.push(proxy("seg-proxied", 40, 1));

            assertThatThrownBy(() -> c.readNext(Duration.ofMillis(50)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("proxy");
        }
    }
}
