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

    // M9.37 review note: a retry run that straddles the delivered boundary (starts before it,
    // ends after it) is a PARTIAL overlap. It is refused whole -- neither re-delivering the
    // already delivered offset nor slicing off the new one -- and a contiguous retry recovers.
    @Test
    void refusesAPartiallyOverlappingRetryRunWithoutDeliveringAnyOfIt() throws Exception {
        FakeTransport transport = new FakeTransport();
        RunKey key = new RunKey(INDEX, 3);
        transport.events = List.of(event(key, 0), event(key, 2));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 0))));

            coordinator.attempt();
            CatchUpRequestFrame original = transport.requests.getFirst();
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            assertThat(client.readNext(Duration.ZERO)).isEmpty();

            // ⚠️ THE OVERLAPPING RUN IS THE LAST EVENT, so no later forward-gap check can
            // refuse the response for it: a silently skipped overlap would complete the
            // exchange and lose offset 1.
            transport.events = List.of(event(key, 0, 2));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(2);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.catchUpComplete(original.requestId())).isFalse();

            transport.events = List.of(event(key, 1), event(key, 2));
            coordinator.attempt();

            assertThat(transport.requests).hasSize(3);
            assertThat(transport.requests.get(2).requestId()).isEqualTo(original.requestId());
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
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
        return event(key, offset, 1);
    }

    private static SubscriptionEvent event(RunKey key, long offset, int count) throws IOException {
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment-" + offset,
                offset, count, FetchMode.INLINE, segment(key, offset, count));
    }

    private static byte[] segment(RunKey key, long offset) throws IOException {
        return segment(key, offset, 1);
    }

    private static byte[] segment(RunKey key, long offset, int count) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (long at = offset; at < offset + count; at++) {
            writer.add(key, new SegmentRecord("doc-" + at, OpType.INDEX,
                    OptionalLong.of(at), "{}".getBytes(StandardCharsets.UTF_8)), at);
        }
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
