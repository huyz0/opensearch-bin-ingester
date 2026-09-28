// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.PendingPool;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A ROUTED {@code _bulk} carries its lane too, and a lane outside {@code i8}
 * never wraps into an active one (M10.6, ADR-0074, M10 criterion 8).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkServiceRoutedLaneTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-00000000ab11");
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "logs-000001"));
    private static final String BODY = "{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n";

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static List<Byte> lanesInStore(CountingBinStore store) throws Exception {
        List<Byte> lanes = new ArrayList<>();
        for (ObjectStat object : store.list("bins/cluster-a/data/", null, 1_000).objects()) {
            try (InputStream in = store.get(object.key())) {
                for (RunEntry e : SegmentReader.open(in.readAllBytes()).directory()) {
                    lanes.add(e.lane());
                }
            }
        }
        return lanes;
    }

    private WebClient start(CountingBinStore store, DefaultIngest ingest) {
        Duration wait = Duration.ofSeconds(10);
        RoutedIngest routed = new RoutedIngest(ingest, new IndexCatalog(),
                new PendingPool(Clock.systemUTC(), wait, 1 << 20), wait, Clock.systemUTC());
        routed.register(new IndexRegistration("idx-uuid", "logs-000001", List.of("logs"), 4, 4,
                1, 1));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new BulkService(routed, PRINCIPAL)))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    @Test
    void aRoutedWriteCarriesItsLaneToTheRunEntry() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1", TestSequencers.leased(store, "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            WebClient client = start(store, ingest);

            try (HttpClientResponse answer = client.post("/logs/_bulk")
                    .queryParam("routing", "tenant-a").queryParam("lane", "2").submit(BODY)) {
                assertThat(answer.status().code()).isEqualTo(202);
            }

            assertThat(lanesInStore(store)).containsExactly((byte) 2);
        }
    }

    @Test
    void aLaneThatWouldWrapIntoAnActiveOneIsABadRequest() throws Exception {
        // ⚠️ 256 IS 0 AND 258 IS 2 AFTER A CAST: without the i8 range check
        // both are 202s at lanes the producer did not ask for.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1", TestSequencers.leased(store, "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            WebClient client = start(store, ingest);

            for (String lane : new String[] {"256", "258", "-254"}) {
                try (HttpClientResponse answer = client.post("/logs/_bulk")
                        .queryParam("routing", "tenant-a").queryParam("lane", lane)
                        .submit(BODY)) {
                    assertThat(answer.status().code()).as("lane=%s", lane).isEqualTo(400);
                }
            }
            ingest.flushNow();
            assertThat(lanesInStore(store)).as("nothing written").isEmpty();
        }
    }
}
