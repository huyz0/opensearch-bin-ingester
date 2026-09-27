// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.ingest.FetchPolicy;
import io.github.huyz0.os.biningester.ingest.FetchPolicyConfig;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.RetainedFloors;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentServing;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * M10.33, ADR-0076: the zone a poll declares reaches the hub subscriber its
 * session holds.
 *
 * <p>⚠️ THE SERVING PATH'S SPLIT IS PINNED BY {@code CrossAzSubscriberModeTest};
 * what only this layer can lose is the CARRY. A session that subscribed without
 * the poll's {@code az} would be served {@code inline} across the zone while
 * every serving-path test stayed green.
 */
class SubscriptionZoneCarryTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 5);
    private static final int SEGMENT = 1_000;

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

    @Test
    void aSessionPolledFromAnotherZoneIsQueuedProxyWithAnEmptySegment() throws Exception {
        SubscriptionHub hub = new SubscriptionHub("az-a");
        Clock clock = Clock.systemUTC();
        SubscriptionService service = new SubscriptionService(hub, new IndexCatalog(),
                new WatermarkTable(clock, Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofMinutes(30)),
                Duration.ofSeconds(90), clock, SubscriptionService.MAX_SESSIONS,
                RetainedFloors.unknown(), new DrainGate(), 64L << 20);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5),
                Duration.ofMillis(200), 1 << 20, "az-b");
        List<Delivery> got = new CopyOnWriteArrayList<>();
        try (var ignored = transport.subscribe(STREAM, got::add)) {
            await(() -> service.sessionCount() == 1, "the az-b session");
            byte[] bytes = new byte[SEGMENT];
            try (MemoryBinStore store = new MemoryBinStore()) {
                store.put("seg-zoned", Body.ofBytes(bytes));
                // ⚠️ BOTH CAPS UNBOUNDED, so the size policy answers INLINE and
                // only the zone can turn it into PROXY.
                hub.publish(new CommitDelta(1, List.of(new SegmentCommit("seg-zoned",
                                List.of(new RunCommit(STREAM, 1, 0)),
                                new SegmentCommit.Attribution("pod1", "inc-1", 0)))),
                        "seg-zoned", bytes, new SegmentServing(
                                new FetchPolicy(new FetchPolicyConfig(
                                        Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                                store.capabilities(), new SegmentProxy(store)));
            }
            await(() -> !got.isEmpty(), "the delivery");
        }

        assertThat(got.get(0).via())
                .as("the az-b poll's zone reached the hub: served proxy, not inline")
                .isEqualTo(FetchMode.PROXY);
        assertThat(got.get(0).segment()).as("and carrying no payload").isEmpty();
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
}
