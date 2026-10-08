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
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * A held stream's repair completes over a void (M13.25e, ADR-0082 §5).
 *
 * <p>⚠️ **THE STALL THIS CLOSES WAS PERMANENT**: a catch-up answer carries
 * nothing for a void, so the repair never reached the live tail it waited for,
 * and the stream stayed held and was asked again at every progress interval.
 */
class GapRepairOverVoidTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000e2"), 0);

    private static byte[] bytes(String id) throws IOException {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        return w.toByteArray(1L);
    }

    private static Delivery delivery(String id, long offset) throws IOException {
        return new Delivery(KEY, "seg-" + id, 1, offset, FetchMode.INLINE, bytes(id));
    }

    /**
     * Answers the repair as a catch-up would: one record, a void, then the
     * held live tail itself -- whose live copy is then suppressed as an
     * overlap.
     */
    private static final class VoidingTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<SubscriptionEvent> lane) throws IOException {
            lane.accept(new SubscriptionEvent("replay", 0, 1, KEY, "replay-1", 1, 1,
                    FetchMode.INLINE, bytes("one"), null, SubscriptionEvent.RANGE_ABSENT,
                    SubscriptionEvent.RANGE_ABSENT, 9L));
            lane.accept(SubscriptionEvent.voidRange("replay", 0, 1, KEY, 2, 3, 10L));
            lane.accept(new SubscriptionEvent("replay", 0, 1, KEY, "replay-5", 5, 1,
                    FetchMode.INLINE, bytes("tail"), null, SubscriptionEvent.RANGE_ABSENT,
                    SubscriptionEvent.RANGE_ABSENT, 11L));
            return CatchUpResult.COMPLETE;
        }
    }

    @Test
    void aREPAIRReachesTheLiveTailOverAVoid() throws Exception {
        VoidingTransport transport = new VoidingTransport();
        try (NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 8)) {
            ConsumerClient client = subscriptions.clientFor(KEY);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                    transport, subscriptions, Optional::<List<CatchUpRequestFrame.Stream>>empty);

            client.deliver(delivery("zero", 0));
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            client.deliver(delivery("tail", 5));
            assertThat(client.readNext(Duration.ZERO)).as("held behind the gap").isEmpty();

            coordinator.attempt();

            java.util.List<Long> read = new java.util.ArrayList<>();
            for (int i = 0; i < 6; i++) {
                client.readNext(Duration.ZERO).ifPresent(r -> read.add(r.offset()));
            }
            assertThat(read).as("⚠️ REPAIRED OVER THE VOID, the tail read once").containsExactly(1L, 5L);
            assertThat(client.voidedOffsetsSkipped()).isEqualTo(3);
            assertThat(client.gapsDetected()).as("the one gap the live tail exposed").isEqualTo(1);

            // ⚠️ M13.25e review P1, T1: THE STREAM IS RELEASED, not merely read to
            // the tail -- the catch-up lane waited for records a void never hands
            // over, so the exchange never completed and every later read was empty.
            client.deliver(delivery("after", 6));
            assertThat(client.readNext(Duration.ofMillis(200)).orElseThrow().offset())
                    .as("a live record after the repair").isEqualTo(6);
        }
    }
}
