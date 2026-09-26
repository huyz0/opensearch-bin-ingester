// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.ProxySource;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * K runs of one {@code proxy} segment on one node cost one fetch from the
 * ingester's route (M10.2, NFR-4, ADR-0073).
 */
class NodeProxyFetchTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000cc");
    private static final String SEGMENT_KEY = "seg-proxied-with-sixty-four-runs";
    private static final int K = 64;

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

    private static List<RunKey> runs(int k) {
        List<RunKey> keys = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            keys.add(new RunKey(INDEX, i));
        }
        return keys;
    }

    private static byte[] segmentWithARunPerKey(List<RunKey> keys) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord("doc-" + key.partitionId(), OpType.INDEX,
                    OptionalLong.of(1),
                    ("run-" + key.partitionId()).getBytes(StandardCharsets.UTF_8)), 1_000L);
        }
        return writer.toByteArray(1_000L);
    }

    @Test
    void sixtyFourRunsOfOneProxiedSegmentCostOneFetchAndEachDecodesItsOwnRecords()
            throws Exception {
        List<RunKey> keys = runs(K);
        byte[] segment = segmentWithARunPerKey(keys);
        AtomicInteger routeFetches = new AtomicInteger();
        ProxySource route = segmentKey -> {
            routeFetches.incrementAndGet();
            if (!SEGMENT_KEY.equals(segmentKey)) {
                throw new IOException("no segment " + segmentKey);
            }
            return segment;
        };
        SegmentSource noDirect = grant -> {
            throw new AssertionError("a proxy delivery must not reach the direct source");
        };
        NodeSegmentSource nodeSource = new NodeSegmentSource(noDirect, route, 1L << 20);
        OneListenerTransport transport = new OneListenerTransport();

        List<String> read = new ArrayList<>();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16, nodeSource)) {
            List<ConsumerClient> clients = new ArrayList<>();
            for (RunKey key : keys) {
                clients.add(node.clientFor(key));
            }
            for (RunKey key : keys) {
                transport.deliver(new Delivery(key, SEGMENT_KEY, 1, 10L * key.partitionId(),
                        FetchMode.PROXY, new byte[0]));
            }
            for (ConsumerClient client : clients) {
                ConsumerRecord record = client.readNext(Duration.ofSeconds(5)).orElseThrow();
                read.add(new String(record.record().payload(), StandardCharsets.UTF_8));
            }
        }

        assertThat(routeFetches.get())
                .as("one request to the ingester's route per (node, segment), not one per run")
                .isEqualTo(1);
        List<String> expected = new ArrayList<>();
        keys.forEach(key -> expected.add("run-" + key.partitionId()));
        assertThat(read).containsExactlyElementsOf(expected);
    }
}
