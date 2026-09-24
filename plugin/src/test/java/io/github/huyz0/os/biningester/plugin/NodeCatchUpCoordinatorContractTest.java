// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class NodeCatchUpCoordinatorContractTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000af");

    @Test
    void waitsUntilEveryRequestedStreamHasAHeldClient() {
        CountingTransport transport = new CountingTransport();
        RunKey first = new RunKey(INDEX, 0);
        RunKey late = new RunKey(INDEX, 1);
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            clients.clientFor(first);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(
                            new CatchUpRequestFrame.Stream(first, 0),
                            new CatchUpRequestFrame.Stream(late, 0))));

            coordinator.attempt();
            assertThat(transport.requests.get()).isZero();

            clients.clientFor(late);
            coordinator.attempt();
            assertThat(transport.requests.get()).isEqualTo(1);
            assertThat(transport.lastRequest.streams()).containsExactly(
                    new CatchUpRequestFrame.Stream(first, 0),
                    new CatchUpRequestFrame.Stream(late, 0));
        }
    }

    @Test
    void completedClientCanStartANewCatchUpRequest() throws Exception {
        CountingTransport transport = new CountingTransport();
        RunKey key = new RunKey(INDEX, 0);
        UUID firstRequest = UUID.randomUUID();
        UUID nextRequest = UUID.randomUUID();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            var client = clients.clientFor(key);
            assertThat(clients.deliverCatchUp(firstRequest, event(key, 0))).isTrue();
            clients.completeCatchUp(firstRequest, Set.of(key));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            assertThat(client.catchUpComplete(firstRequest)).isTrue();

            assertThat(clients.deliverCatchUp(nextRequest, event(key, 1))).isTrue();
            clients.completeCatchUp(nextRequest, Set.of(key));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.catchUpComplete(nextRequest)).isTrue();
        }
    }

    @Test
    void catchesUpEveryLocalStreamInOneRequestAboveTheOriginalV1Limit() {
        CountingTransport transport = new CountingTransport();
        int streamCount = 1025;
        List<CatchUpRequestFrame.Stream> streams = java.util.stream.IntStream
                .range(0, streamCount)
                .mapToObj(partition -> new CatchUpRequestFrame.Stream(
                        new RunKey(INDEX, partition), partition))
                .toList();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            streams.forEach(stream -> clients.clientFor(stream.key()));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(streams));

            coordinator.attempt();

            assertThat(transport.submittedRequests).hasSize(1);
            assertThat(transport.submittedRequests.getFirst().streams()).containsExactlyElementsOf(
                    streams);
            assertThat(transport.submittedRequests.getFirst().encode()[7]).isEqualTo((byte) 2);
            assertThat(transport.submittedRequests).allSatisfy(request ->
                    assertThat(request.streams()).hasSizeBetween(1,
                            CatchUpRequestFrame.MAX_STREAMS));
        }
    }

    @Test
    void keepsTheMaximumNodeSnapshotEligibleWhileClientsAreBeingHeld() {
        CountingTransport transport = new CountingTransport();
        AtomicInteger snapshots = new AtomicInteger();
        List<CatchUpRequestFrame.Stream> streams = java.util.stream.IntStream
                .range(0, CatchUpRequestFrame.MAX_STREAMS)
                .mapToObj(partition -> new CatchUpRequestFrame.Stream(
                        new RunKey(INDEX, partition), partition))
                .toList();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> {
                        snapshots.incrementAndGet();
                        return Optional.of(streams);
                    });

            coordinator.attempt();
            coordinator.attempt();

            assertThat(snapshots).hasValue(2);
            assertThat(transport.requests).hasValue(0);
        }
    }

    @Test
    void refusesAnOversizedNodeSnapshotWithoutRetryingItForever() {
        CountingTransport transport = new CountingTransport();
        AtomicInteger snapshots = new AtomicInteger();
        List<CatchUpRequestFrame.Stream> streams = java.util.Collections.nCopies(
                CatchUpRequestFrame.MAX_STREAMS + 1,
                new CatchUpRequestFrame.Stream(new RunKey(INDEX, 0), 0));
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 4)) {
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> {
                        snapshots.incrementAndGet();
                        return Optional.of(streams);
                    });

            coordinator.attempt();
            coordinator.attempt();

            assertThat(snapshots).hasValue(1);
            assertThat(transport.requests).hasValue(0);
        }
    }

    private static SubscriptionEvent event(RunKey key, long offset) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("doc-" + offset, OpType.INDEX,
                OptionalLong.of(offset), "{}".getBytes(StandardCharsets.UTF_8)), offset);
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment-" + offset,
                offset, 1, FetchMode.INLINE, writer.toByteArray(11L));
    }

    private static final class CountingTransport implements SubscriptionTransport {
        private final AtomicInteger requests = new AtomicInteger();
        private final List<CatchUpRequestFrame> submittedRequests = new ArrayList<>();
        private CatchUpRequestFrame lastRequest;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) { }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<SubscriptionEvent> events) {
            lastRequest = request;
            requests.incrementAndGet();
            this.submittedRequests.add(request);
            return CatchUpResult.COMPLETE;
        }
    }
}
