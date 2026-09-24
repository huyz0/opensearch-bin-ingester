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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class CatchUpReleaseBackpressureTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000ad");

    @Test
    void releasingShardDoesNotWaitForAFullCatchUpLane() throws Exception {
        RunKey key = new RunKey(INDEX, 0);
        SubscriptionTransport transport = new SubscriptionTransport() {
            @Override public AutoCloseable subscribe(RunKey ignored, Listener listener) {
                return () -> { };
            }
            @Override public void register(io.github.huyz0.os.biningester.format.IndexRegistration r) { }
            @Override public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                    Consumer<SubscriptionEvent> lane) {
                lane.accept(event(key, 7));
                lane.accept(event(key, 8));
                return CatchUpResult.COMPLETE;
            }
        };
        NodeSubscriptions clients = new NodeSubscriptions(transport, 1);
        clients.clientFor(key);
        NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 7))));
        Thread attempt = Thread.ofVirtual().start(coordinator::attempt);
        boolean parkedOnFullLane = awaits(attempt, Thread.State.WAITING, Duration.ofSeconds(5));

        Thread release = Thread.ofVirtual().start(() -> clients.release(key));
        release.join(Duration.ofSeconds(5));
        attempt.join(Duration.ofSeconds(5));

        assertThat(parkedOnFullLane)
                .as("a full catch-up lane refuses immediately instead of parking its producer")
                .isFalse();
        assertThat(release.isAlive())
                .as("shard close can release its held client while replay is backpressured")
                .isFalse();
        assertThat(attempt.isAlive())
                .as("the replay attempt cannot remain parked after its client is released")
                .isFalse();
        assertThat(clients.openClients()).isZero();
        clients.close();
    }

    @Test
    void fullLaneRefusalIsContainedAndTheSameRequestRetriesAfterCapacityReturns()
            throws Exception {
        RunKey key = new RunKey(INDEX, 0);
        AtomicInteger requests = new AtomicInteger();
        SubscriptionTransport transport = new SubscriptionTransport() {
            @Override public AutoCloseable subscribe(RunKey ignored, Listener listener) {
                return () -> { };
            }
            @Override public void register(io.github.huyz0.os.biningester.format.IndexRegistration r) { }
            @Override public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                    Consumer<SubscriptionEvent> lane) {
                requests.incrementAndGet();
                lane.accept(event(key, 7));
                lane.accept(event(key, 8));
                return CatchUpResult.COMPLETE;
            }
        };
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 1)) {
            var client = clients.clientFor(key);
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    () -> Optional.of(List.of(new CatchUpRequestFrame.Stream(key, 7))));
            AtomicReference<Throwable> escaped = new AtomicReference<>();
            Thread firstAttempt = Thread.ofVirtual().start(() -> {
                try {
                    coordinator.attempt();
                } catch (Throwable failure) {
                    escaped.set(failure);
                }
            });
            firstAttempt.join(Duration.ofSeconds(5));

            assertThat(firstAttempt.isAlive()).isFalse();
            assertThat(escaped.get()).isNull();
            assertThat(requests.get()).isEqualTo(1);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(7);

            coordinator.attempt();

            assertThat(requests.get()).isEqualTo(2);
            assertThat(client.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(8);
        }
    }

    private static boolean awaits(Thread thread, Thread.State state, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (thread.getState() != state && System.nanoTime() < deadline) {
            Thread.yield();
        }
        return thread.getState() == state;
    }

    private static SubscriptionEvent event(RunKey key, long offset) {
        byte[] bytes;
        try {
            SegmentWriter writer = new SegmentWriter();
            writer.add(key, new SegmentRecord("doc-" + offset, OpType.INDEX,
                    OptionalLong.of(offset), "{}".getBytes(StandardCharsets.UTF_8)), offset);
            bytes = writer.toByteArray(11L);
        } catch (java.io.IOException impossible) {
            throw new AssertionError(impossible);
        }
        return new SubscriptionEvent("catch-up", 0, 1, key, "segment-" + offset,
                offset, 1, FetchMode.INLINE, bytes);
    }
}
