// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.PositionCollectedException;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The node-level router hands each shard its own stream's floor (M8.6,
 * ADR-0056).
 *
 * <p>⚠️ **ONE SUBSCRIPTION FEEDS EVERY SHARD ON THE NODE**, so the floor
 * arrives at the router, not at a client, and the router is where it is either
 * routed or dropped. It was a lambda before the floor could travel -- and a
 * lambda implements only {@code onDelivery}, so the floor would have fallen to
 * the interface's no-op default and no shard on the node would ever have
 * refused a collected position, with every client-level test green.
 */
class NodeFloorRoutingTest {

    private static final RunKey LOGS = new RunKey(new UUID(7, 7), 0);
    private static final RunKey METRICS = new RunKey(new UUID(8, 8), 2);

    /** A transport that keeps the one listener the node registers. */
    private static final class CapturingTransport implements SubscriptionTransport {
        final List<Listener> listeners = new ArrayList<>();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> { };
        }
    }

    @Test
    void aFLOORReachesTheClientForITSStreamAndNoOther() throws Exception {
        CapturingTransport transport = new CapturingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            ConsumerClient logs = node.clientFor(LOGS);
            ConsumerClient metrics = node.clientFor(METRICS);
            assertThat(transport.listeners).as("the premise: the node registered").isNotEmpty();

            transport.listeners.get(0).onRetainedFloor(LOGS, 500);

            assertThatThrownBy(() -> logs.refuseIfCollected(100))
                    .as("⚠️ ROUTED: the shard for this stream now refuses a collected position")
                    .isInstanceOf(PositionCollectedException.class);
            metrics.refuseIfCollected(100);
        }
    }

    @Test
    void aFLOORForAStreamThisNodeDoesNotHoldCreatesNOClient() throws Exception {
        // ⚠️ A floor must not CREATE a client -- that would make one nobody
        // holds, which is why a delivery for such a key is dropped. Nor is it
        // kept for later: a client asks for a FRESH floor after its own
        // resume (ADR-0056), and one held from before the resume could never
        // clear it anyway.
        CapturingTransport transport = new CapturingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            node.clientFor(LOGS);
            int subscribed = transport.listeners.size();

            transport.listeners.get(0).onRetainedFloor(METRICS, 500);
            assertThat(transport.listeners)
                    .as("⚠️ NO CLIENT, AND SO NO REGISTRATION, WAS CREATED BY THE FLOOR")
                    .hasSize(subscribed);

            ConsumerClient metrics = node.clientFor(METRICS);
            assertThat(metrics.retainedFloor())
                    .as("⚠️ AND NOTHING WAS STASHED FOR IT").isEmpty();
        }
    }

    @Test
    void theROUTERAsksForAStreamOnlyWhileITSClientWantsAFloor() throws Exception {
        // ⚠️ THE ASK GOES THROUGH THE ROUTER TOO. A router that answered the
        // interface's default `false` would never ask, and no shard on the
        // node would ever be sent a floor after a resume.
        CapturingTransport transport = new CapturingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            ConsumerClient logs = node.clientFor(LOGS);
            node.clientFor(METRICS);
            SubscriptionTransport.Listener router = transport.listeners.get(0);

            assertThat(router.wantsRetainedFloor(LOGS)).as("TAILING: NO ASK").isFalse();

            logs.requestFreshFloor();
            assertThat(router.wantsRetainedFloor(METRICS))
                    .as("⚠️ NOT FOR A STREAM WHOSE CLIENT DID NOT RESUME").isFalse();
            assertThat(router.wantsRetainedFloor(LOGS))
                    .as("⚠️ FOR THE ONE THAT DID").isTrue();
            assertThat(router.wantsRetainedFloor(new RunKey(new UUID(9, 9), 0)))
                    .as("NOR FOR A STREAM THE NODE DOES NOT HOLD").isFalse();
        }
    }
}
