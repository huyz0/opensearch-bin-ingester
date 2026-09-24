// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/** A delivery gap is repaired through node catch-up before the live tail advances. */
class GapRereadTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000e1");
    private static final RunKey KEY = new RunKey(INDEX, 0);

    private static final class ReplayTransport implements SubscriptionTransport {
        private final boolean partialFirstResponse;
        private final long endExclusive;
        private final AtomicInteger replayRequests = new AtomicInteger();

        private ReplayTransport() {
            this(false, 4);
        }

        private ReplayTransport(boolean partialFirstResponse) {
            this(partialFirstResponse, 4);
        }

        private ReplayTransport(boolean partialFirstResponse, long endExclusive) {
            this.partialFirstResponse = partialFirstResponse;
            this.endExclusive = endExclusive;
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<SubscriptionEvent> lane) throws IOException {
            replayRequests.incrementAndGet();
            assertThat(request.streams()).containsExactly(
                    new CatchUpRequestFrame.Stream(KEY, 1));
            lane.accept(event(1, "missing"));
            if (partialFirstResponse && replayRequests.get() == 1) {
                return CatchUpResult.COMPLETE;
            }
            if (endExclusive > 2) {
                lane.accept(event(2, "tail"));
            }
            if (endExclusive > 3) {
                lane.accept(event(3, "after-tail"));
            }
            return CatchUpResult.COMPLETE;
        }

        private static SubscriptionEvent event(long offset, String id) throws IOException {
            return new SubscriptionEvent("replay", 0, 1, KEY, "replay-" + offset, offset, 1,
                    FetchMode.INLINE, bytes(id));
        }
    }

    @Test
    void rereadsTheGapBeforeReleasingTheLiveTailAndSuppressesItsOverlap() throws Exception {
        ReplayTransport transport = new ReplayTransport();
        try (NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 2)) {
            ConsumerClient client = subscriptions.clientFor(KEY);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, subscriptions, Optional::<List<CatchUpRequestFrame.Stream>>empty);

            client.deliver(delivery("first", 0, "first"));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();

            // Offset 1 is lost on the live route. Offset 2 proves the gap and
            // later live records must remain held until replay repairs 1..2.
            client.deliver(delivery("tail", 2, "tail"));
            client.deliver(delivery("after-tail", 3, "after-tail"));
            assertThat(client.readNext(Duration.ZERO))
                    .as("a later live offset must not pass an unresolved gap")
                    .isEmpty();
            coordinator.attempt();
            assertThat(transport.replayRequests).hasValue(1);
            assertThat(client.lastGap()).isPresent();

            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            coordinator.attempt();
            assertThat(transport.replayRequests).hasValue(2);

            // The replay lane is bounded: retry the stable request after freeing room.
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(3);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
            assertThat(client.gapsDetected()).isEqualTo(1);
            assertThat(transport.replayRequests).hasValue(2);
        }
    }

    @Test
    void keepsLiveTailHeldUntilReplayCoversTheWholeGap() throws Exception {
        ReplayTransport transport = new ReplayTransport(true, 3);
        try (NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 2)) {
            ConsumerClient client = subscriptions.clientFor(KEY);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, subscriptions, Optional::<List<CatchUpRequestFrame.Stream>>empty);

            client.deliver(delivery("first", 0, "first"));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            client.deliver(delivery("tail", 3, "tail"));
            client.deliver(delivery("after-tail", 4, "after-tail"));
            assertThat(client.readNext(Duration.ZERO)).isEmpty();

            coordinator.attempt();
            assertThat(transport.replayRequests).hasValue(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();

            coordinator.attempt();
            assertThat(transport.replayRequests).hasValue(2);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(3);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(4);
            assertThat(client.readNext(Duration.ZERO)).isEmpty();
        }
    }

    private static Delivery delivery(String segment, long offset, String... ids)
            throws IOException {
        return new Delivery(KEY, segment, ids.length, offset, FetchMode.INLINE,
                bytes(ids), null, 1);
    }

    private static byte[] bytes(String... ids) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (String id : ids) {
            writer.add(KEY, new SegmentRecord(id, OpType.INDEX,
                    OptionalLong.of(1), id.getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return writer.toByteArray(1L);
    }
}
