// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
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
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A {@code proxy} push carries no bytes on the wire, so the HTTP subscription
 * must not assemble one per session and charge it to the queue budget
 * (M10.3, NFR-6, ADR-0073).
 */
class SubscriptionProxyBudgetTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);
    private static final int SEGMENT = 1024 * 1024;
    private static final long BUDGET = 64L * 1024 * 1024;

    private static SubscriptionService service(SubscriptionHub hub) {
        Clock clock = Clock.systemUTC();
        return new SubscriptionService(hub, new IndexCatalog(),
                new WatermarkTable(clock, Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofMinutes(30)),
                Duration.ofSeconds(90), clock, SubscriptionService.MAX_SESSIONS,
                RetainedFloors.unknown(), new DrainGate(), BUDGET);
    }

    private static void publish(SubscriptionHub hub, String segmentKey, FetchPolicyConfig policy)
            throws Exception {
        byte[] bytes = new byte[SEGMENT];
        CommitDelta delta = new CommitDelta(1, List.of(new SegmentCommit(segmentKey,
                List.of(new RunCommit(STREAM, 1, 0)),
                new SegmentCommit.Attribution("pod1", "inc-1", 0))));
        try (var store = new MemoryBinStore()) {
            store.put(segmentKey, Body.ofBytes(bytes));
            hub.publish(delta, segmentKey, bytes, new SegmentServing(new FetchPolicy(policy),
                    store.capabilities(), new SegmentProxy(store)));
        }
    }

    @Test
    void aProxyPushChargesNothingToTheQueueBudget() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        SubscriptionService service = service(hub);
        for (int i = 0; i < 3; i++) {
            service.sessionFor("s" + i + "@" + STREAM, STREAM);
        }

        publish(hub, "seg-proxy", new FetchPolicyConfig(1, 0, 1, false));

        assertThat(service.queuedBytes())
                .as("three sessions were each pushed a 1 MiB `proxy` segment whose bytes the "
                        + "answer never carries; holding a copy per session is 3 MiB for nothing")
                .isZero();

        // The control: the same sessions, the same segment served inline, DO
        // hold its bytes -- so the zero above is a measurement, not a budget
        // that counts nothing.
        publish(hub, "seg-inline", new FetchPolicyConfig(Long.MAX_VALUE, Long.MAX_VALUE, 1, false));
        assertThat(service.queuedBytes()).isEqualTo(3L * SEGMENT);
    }
}
