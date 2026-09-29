// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * M12.19c (M10.24 T1): a burst queued behind one held delivery costs EXACTLY
 * one poll beyond the long poll already open -- that one carried the first
 * push, and ONE more carried all five queued behind it. {@code SubscriptionChannelTest}'s assertion is
 * "fewer than six", which a drain capped at two pushes an answer (four polls)
 * also meets. Here the reader is held in the burst's LAST callback too, so the
 * count is read before it can open the next poll, and can be exact.
 */
class SubscriptionBurstPollCountTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 3);

    private WebServer server;
    private HttpSubscriptionTransport transport;

    @AfterEach
    void stop() {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    private static void publish(SubscriptionHub hub, String segmentKey, long firstOffset)
            throws Exception {
        io.github.huyz0.os.biningester.format.CommitDelta delta =
                new io.github.huyz0.os.biningester.format.CommitDelta(1, List.of(
                        new io.github.huyz0.os.biningester.format.SegmentCommit(segmentKey,
                                List.of(new io.github.huyz0.os.biningester.format.RunCommit(
                                        STREAM, 1, firstOffset)),
                                new io.github.huyz0.os.biningester.format.SegmentCommit
                                        .Attribution("pod1", "inc-1", 0))));
        try (var store = new io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore()) {
            store.put(segmentKey,
                    io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
            hub.publish(delta, segmentKey, new byte[] {1, 2, 3},
                    new io.github.huyz0.os.biningester.ingest.SegmentServing(
                            new io.github.huyz0.os.biningester.ingest.FetchPolicy(
                                    new io.github.huyz0.os.biningester.ingest.FetchPolicyConfig(
                                            Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                            store.capabilities(),
                            new io.github.huyz0.os.biningester.ingest.SegmentProxy(store,
                                    io.github.huyz0.os.biningester.ingest.SegmentProxy
                                            .DEFAULT_CHUNK_BYTES,
                                    new io.github.huyz0.os.biningester.ingest.SegmentCache(0),
                                    new io.github.huyz0.os.biningester.binstore
                                            .IndexCostLedger())));
        }
    }

    @Test
    void aBurstQueuedBehindOneDeliveryCostsExactlyOneMorePoll() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(),
                new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1),
                        Duration.ofHours(2), Duration.ofMinutes(30)),
                Clock.systemUTC());
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        List<Delivery> got = new CopyOnWriteArrayList<>();
        CountDownLatch firstHeld = new CountDownLatch(1);
        CountDownLatch burstQueued = new CountDownLatch(1);
        CountDownLatch lastHeld = new CountDownLatch(1);
        CountDownLatch counted = new CountDownLatch(1);
        SubscriptionTransport.Listener listener = delivery -> {
            got.add(delivery);
            try {
                if (got.size() == 1) {
                    firstHeld.countDown();
                    burstQueued.await();
                } else if (got.size() == 6) {
                    lastHeld.countDown();
                    counted.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5));

        try (var ignored = transport.subscribe(STREAM, listener)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            // ⚠️ THE BASELINE WAITS FOR THE FIRST LONG POLL TO ARRIVE (review T1):
            // `reconnects` rises when the short handshake poll is ANSWERED, and
            // the long poll that follows is counted when it reaches the server,
            // so read at once the baseline may or may not include it.
            await(() -> service.pollCount() >= 2, "the handshake and the first long poll");
            long pollsBefore = service.pollCount();
            publish(hub, "seg-burst-0", 200);
            assertThat(firstHeld.await(10, TimeUnit.SECONDS))
                    .as("the premise: the reader is held on the first push").isTrue();
            for (int i = 1; i < 6; i++) {
                publish(hub, "seg-burst-" + i, 200 + i);
            }
            burstQueued.countDown();
            assertThat(lastHeld.await(10, TimeUnit.SECONDS))
                    .as("the premise: the whole burst arrived, the reader held on its last")
                    .isTrue();
            long polls = service.pollCount() - pollsBefore;
            counted.countDown();

            assertThat(polls)
                    .as("⚠️ EXACTLY ONE MORE: the open long poll carried the first push, ONE "
                            + "more carried the five queued behind it -- a drain capped per "
                            + "answer takes more")
                    .isEqualTo(1);
        }
    }
}
