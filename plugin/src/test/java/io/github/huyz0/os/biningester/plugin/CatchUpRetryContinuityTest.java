// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class CatchUpRetryContinuityTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000b1");

    @Test
    void retriesWithTheOriginalSnapshotAndRequestIdentity() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.failOnce = true;
        RunKey key = new RunKey(INDEX, 0);
        transport.events = List.of(event(key, 7));
        AtomicReference<Optional<List<CatchUpRequestFrame.Stream>>> positions =
                new AtomicReference<>(Optional.of(List.of(
                        new CatchUpRequestFrame.Stream(key, 7))));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, clients, positions::get);

            coordinator.attempt();
            CatchUpRequestFrame original = transport.requests.getFirst();
            positions.set(Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 99))));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(2);
            assertThat(transport.requests.get(1).requestId()).isEqualTo(original.requestId());
            assertThat(transport.requests.get(1).streams()).isEqualTo(original.streams());
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(7);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isTrue();
        }
    }

    @Test
    void refusesAForwardGapAndRetriesWithoutSkippingTheMissingOffset() throws Exception {
        FakeTransport transport = new FakeTransport();
        RunKey key = new RunKey(INDEX, 2);
        transport.events = List.of(event(key, 0), event(key, 2));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 0))));

            coordinator.attempt();
            CatchUpRequestFrame original = transport.requests.getFirst();

            assertThat(transport.requests).hasSize(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isFalse();

            transport.events = List.of(event(key, 0), event(key, 1), event(key, 2));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(2);
            assertThat(transport.requests.get(1).requestId()).isEqualTo(original.requestId());
            assertThat(transport.requests.get(1).streams()).isEqualTo(original.streams());
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isTrue();
        }
    }

    /**
     * ⚠️ A PARTIAL OVERLAP (M10.11, harvested from b65b9f9's review): a retry
     * whose run STARTS inside what was already delivered and ends past it. A
     * wholly repeated run is skipped and a forward gap is refused above; this
     * straddling run is refused too -- delivering it would repeat offset 0,
     * and trimming it silently would hide a server that answered wrongly --
     * and the request stays open until a contiguous answer arrives.
     */
    @Test
    void refusesARetryThatPartlyOverlapsWhatWasDeliveredAndResumesContiguously()
            throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.failOnce = true;
        RunKey key = new RunKey(INDEX, 3);
        transport.events = List.of(event(key, 0));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 0))));

            coordinator.attempt();
            CatchUpRequestFrame original = transport.requests.getFirst();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();

            transport.events = List.of(event(key, 0, 2));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(2);
            assertThat(client.readNext(Duration.ZERO))
                    .as("neither offset 0 again nor offset 1 from the straddling run").isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isFalse();

            transport.events = List.of(event(key, 1));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(3);
            assertThat(transport.requests.get(2).requestId()).isEqualTo(original.requestId());
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isTrue();
        }
    }

    @Test
    void unsupportedCatchUpStillDeliversLiveSubscriptionEvents() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.result = SubscriptionTransport.CatchUpResult.UNSUPPORTED;
        RunKey key = new RunKey(INDEX, 1);
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 0))));

            coordinator.attempt();
            transport.deliverLive(key, new Delivery(key, "live-segment", 1, 9,
                    FetchMode.INLINE, segment(key, 9), null));

            assertThat(clients.openClients()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(9);
        }
    }

    private static SubscriptionEvent event(RunKey key, long offset) throws IOException {
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment-" + offset,
                offset, 1, FetchMode.INLINE, segment(key, offset));
    }

    /** One run of {@code count} records from {@code first}, in one segment. */
    private static SubscriptionEvent event(RunKey key, long first, int count)
            throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (long offset = first; offset < first + count; offset++) {
            writer.add(key, new SegmentRecord("doc-" + offset, OpType.INDEX,
                    OptionalLong.of(offset), "{}".getBytes(StandardCharsets.UTF_8)), offset);
        }
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment-" + first + "-" + count,
                first, count, FetchMode.INLINE, writer.toByteArray(11L));
    }

    private static byte[] segment(RunKey key, long offset) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("doc-" + offset, OpType.INDEX,
                OptionalLong.of(offset), "{}".getBytes(StandardCharsets.UTF_8)), offset);
        return writer.toByteArray(11L);
    }

    private static final class FakeTransport implements SubscriptionTransport {
        private final Map<RunKey, Listener> listeners = new java.util.concurrent.ConcurrentHashMap<>();
        private final List<CatchUpRequestFrame> requests = new ArrayList<>();
        private List<SubscriptionEvent> events = List.of();
        private CatchUpResult result = CatchUpResult.COMPLETE;
        private boolean failOnce;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.put(key, listener);
            return () -> listeners.remove(key, listener);
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<SubscriptionEvent> lane) throws IOException {
            requests.add(request);
            events.forEach(lane);
            if (failOnce) {
                failOnce = false;
                throw new IOException("retry");
            }
            return result;
        }

        void deliverLive(RunKey key, Delivery delivery) {
            listeners.get(key).onDelivery(delivery);
        }
    }
}
