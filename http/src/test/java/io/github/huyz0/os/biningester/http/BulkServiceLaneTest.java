// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The producer names a lane on {@code _bulk} and it reaches the run entry
 * (M10.6, ADR-0074, M10 criterion 8), through the real stack: a lane that
 * stopped at the adapter would pass any test of the adapter alone.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BulkServiceLaneTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-00000000ab10");
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final String BODY =
            "{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n";

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static Map<RunKey, Byte> lanesInStore(CountingBinStore store) throws Exception {
        Map<RunKey, Byte> lanes = new HashMap<>();
        for (ObjectStat object : store.list("bins/cluster-a/data/", null, 1_000).objects()) {
            try (InputStream in = store.get(object.key())) {
                for (RunEntry e : SegmentReader.open(in.readAllBytes()).directory()) {
                    lanes.put(e.key(), e.lane());
                }
            }
        }
        return lanes;
    }

    @Test
    void aNamedLaneReachesTheRunEntryAndAnAbsentOneIsZero() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1", TestSequencers.leased(store, "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            try (HttpClientResponse elevated = client.post("/logs/_bulk")
                    .queryParam("partition", "3").queryParam("lane", "2").submit(BODY);
                    HttpClientResponse plain = client.post("/logs/_bulk")
                            .queryParam("partition", "4").submit(BODY)) {
                assertThat(elevated.status().code()).isEqualTo(202);
                assertThat(plain.status().code()).isEqualTo(202);
            }

            assertThat(lanesInStore(store))
                    .containsEntry(new RunKey(LOGS, 3), (byte) 2)
                    .containsEntry(new RunKey(LOGS, 4), (byte) 0);
        }
    }

    @Test
    void aLaneOutsideTheActiveSetOrNotAnI8IsABadRequestAndWritesNothing() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(Duration.ofMillis(30), 8L << 20, "cluster-a"), store,
                "bins/cluster-a", "pod1", TestSequencers.leased(store, "bins/cluster-a", "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(ingest, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();
            long dataPutsBefore = store.putPurposeCounts().dataPuts();

            for (String lane : new String[] {"3", "-3", "one", "200", "-129", ""}) {
                try (HttpClientResponse answer = client.post("/logs/_bulk")
                        .queryParam("partition", "3").queryParam("lane", lane).submit(BODY)) {
                    assertThat(answer.status().code()).as("lane=%s", lane).isEqualTo(400);
                }
            }
            ingest.flushNow();

            assertThat(store.putPurposeCounts().dataPuts() - dataPutsBefore)
                    .as("a refused lane buffered nothing").isZero();
        }
    }
}
